/// Block writer is created once per ingestion request cycle,
/// it expects sorted chunks of data broken by streams (stream id + tenant)
const std = @import("std");
const builtin = @import("builtin");
const Allocator = std.mem.Allocator;
const Io = std.Io;

const Line = @import("../lines.zig").Line;
const SID = @import("../lines.zig").SID;
const Block = @import("Block.zig");
const BlockData = @import("../data/BlockData.zig").BlockData;
const BlockHeader = @import("BlockHeader.zig");
const IndexBlockHeader = @import("IndexBlockHeader.zig");
const TableWriter = @import("TableWriter.zig");
const TableHeader = @import("TableHeader.zig");
const encoding = @import("encoding");

const BlockWriter = @This();

pub const indexBlockSize = 16 * 1024;
pub const indexBlockFlushThreshold = 128 * 1024;
pub const metaIndexSize = 4 * 1024;

const Content = union(enum) {
    block: *Block,
    data: *BlockData,
};

// state for the current index block (reset after flush)
sid: ?SID,
blockMinTimestamp: u64,
blockMaxTimestamp: u64,
// state for the table
len: u32,
size: u32,
tableMinTimestamp: u64,
tableMaxTimestamp: u64,
blocksCount: u32,

// state to validate ordering across all written blocks
sidLast: ?SID,
minTimestampLast: u64,

// TODO: refactor this garbage to work with Wrter interface
indexBlockBuf: std.ArrayList(u8),
indexBlockHeader: IndexBlockHeader = .{},
metaIndexBuf: std.ArrayList(u8),

pub fn init(allocator: Allocator) !*BlockWriter {
    var indexBlockBuf = try std.ArrayList(u8).initCapacity(allocator, indexBlockSize);
    errdefer indexBlockBuf.deinit(allocator);
    var metaIndexBuf = try std.ArrayList(u8).initCapacity(allocator, metaIndexSize);
    errdefer metaIndexBuf.deinit(allocator);

    const bw = try allocator.create(BlockWriter);
    bw.* = BlockWriter{
        .sid = null,
        .blockMinTimestamp = 0,
        .blockMaxTimestamp = 0,

        .len = 0,
        .size = 0,
        .tableMinTimestamp = 0,
        .tableMaxTimestamp = 0,
        .blocksCount = 0,

        .sidLast = null,
        .minTimestampLast = 0,

        .indexBlockBuf = indexBlockBuf,
        .metaIndexBuf = metaIndexBuf,
    };
    return bw;
}

pub fn deinit(self: *BlockWriter, allocator: Allocator) void {
    self.indexBlockBuf.deinit(allocator);
    self.metaIndexBuf.deinit(allocator);
    allocator.destroy(self);
}

pub fn writeLines(
    self: *BlockWriter,
    io: Io,
    allocator: Allocator,
    sid: SID,
    lines: []Line,
    tableWriter: *TableWriter,
) !void {
    const block = try Block.initFromLines(allocator, lines);
    defer block.deinit(allocator);

    if (block.len() == 0) {
        return;
    }

    const c = Content{ .block = block };
    try self.writeBlock(io, allocator, c, sid, tableWriter);
}

pub fn writeData(
    self: *BlockWriter,
    io: Io,
    allocator: Allocator,
    data: *BlockData,
    tableWriter: *TableWriter,
) !void {
    const c = Content{ .data = data };
    try self.writeBlock(io, allocator, c, data.sid, tableWriter);
}

fn writeBlock(
    self: *BlockWriter,
    io: Io,
    alloc: Allocator,
    // block: *Block,
    content: Content,
    sid: SID,
    tableWriter: *TableWriter,
) !void {
    var isSeenSid = false;
    if (self.sidLast) |sidLast| {
        std.debug.assert(!sid.lessThan(&sidLast));
        isSeenSid = sid.eql(&sidLast);
    }

    const hasState = self.sid != null;
    if (!hasState) {
        self.sid = sid;
    }

    const blockHeader = try writeContent(io, alloc, content, sid, tableWriter);

    if (self.len == 0 or blockHeader.timestampsHeader.min < self.tableMinTimestamp) {
        self.tableMinTimestamp = blockHeader.timestampsHeader.min;
    }
    if (self.len == 0 or blockHeader.timestampsHeader.max > self.tableMaxTimestamp) {
        self.tableMaxTimestamp = blockHeader.timestampsHeader.max;
    }
    if (!hasState or blockHeader.timestampsHeader.min < self.blockMinTimestamp) {
        self.blockMinTimestamp = blockHeader.timestampsHeader.min;
    }
    if (!hasState or blockHeader.timestampsHeader.max > self.blockMaxTimestamp) {
        self.blockMaxTimestamp = blockHeader.timestampsHeader.max;
    }

    if (isSeenSid) {
        std.debug.assert(blockHeader.timestampsHeader.min >= self.minTimestampLast);
    }
    self.sidLast = sid;
    self.minTimestampLast = blockHeader.timestampsHeader.min;

    self.size += blockHeader.size;
    self.len += blockHeader.len;
    self.blocksCount += 1;

    try self.indexBlockBuf.ensureUnusedCapacity(alloc, BlockHeader.encodeExpectedSize);
    const slice = self.indexBlockBuf.unusedCapacitySlice()[0..BlockHeader.encodeExpectedSize];
    const offset = blockHeader.encode(slice);
    self.indexBlockBuf.items.len += offset;
    if (self.indexBlockBuf.items.len > indexBlockFlushThreshold) {
        try self.flushIndexBlock(io, alloc, tableWriter);
    }
}

fn writeContent(io: Io, alloc: Allocator, content: Content, sid: SID, tableWriter: *TableWriter) !BlockHeader {
    switch (content) {
        .block => |block| {
            var blockHeader = BlockHeader.initFromBlock(block, sid);
            try tableWriter.writeBlock(io, alloc, block, &blockHeader);
            return blockHeader;
        },
        .data => |data| {
            var blockHeader = BlockHeader.initFromData(data, sid);
            try tableWriter.writeData(io, alloc, &blockHeader, data);
            return blockHeader;
        },
    }
}

pub fn finish(self: *BlockWriter, io: Io, allocator: Allocator, tableWriter: *TableWriter, th: *TableHeader) !void {
    th.uncompressedSize = self.size;
    th.len = self.len;
    th.blocksCount = self.blocksCount;
    th.minTimestamp = self.tableMinTimestamp;
    th.maxTimestamp = self.tableMaxTimestamp;
    th.bloomValuesBuffersAmount = @intCast(tableWriter.bloomValuesList.items.len);

    try self.flushIndexBlock(io, allocator, tableWriter);

    try tableWriter.writeColumnKeys(io, allocator);
    try tableWriter.writeColumnIndexes(io, allocator);

    try self.writeIndexBlockHeaders(io, allocator, tableWriter);

    th.compressedSize = tableWriter.size();
}

fn flushIndexBlock(self: *BlockWriter, io: Io, allocator: Allocator, tableWriter: *TableWriter) !void {
    defer self.indexBlockBuf.clearRetainingCapacity();
    if (self.indexBlockBuf.items.len > 0) {
        try self.indexBlockHeader.writeIndexBlock(
            io,
            allocator,
            &self.indexBlockBuf,
            self.sid.?,
            self.blockMinTimestamp,
            self.blockMaxTimestamp,
            tableWriter,
        );

        try self.metaIndexBuf.ensureUnusedCapacity(allocator, IndexBlockHeader.encodeExpectedSize);
        const slice = self.metaIndexBuf.unusedCapacitySlice()[0..IndexBlockHeader.encodeExpectedSize];
        const offset = self.indexBlockHeader.encode(slice);
        self.metaIndexBuf.items.len += offset;
    }

    self.sid = null;
    self.blockMinTimestamp = 0;
    self.blockMaxTimestamp = 0;
}

fn writeIndexBlockHeaders(self: *BlockWriter, io: Io, allocator: Allocator, tableWriter: *TableWriter) !void {
    const bound = try encoding.compressBound(self.metaIndexBuf.items.len);
    const slice = try tableWriter.metaindexDst.allocSlice(allocator, bound);
    const offset = try encoding.compressAuto(slice, self.metaIndexBuf.items);

    try tableWriter.metaindexDst.appendAllocated(io, slice, offset);
}
