/// Block writer is created once per ingestion request cycle,
/// it expects sorted chunks of data broken by streams (stream id + tenant)
const std = @import("std");
const Allocator = std.mem.Allocator;

const Line = @import("../lines.zig").Line;
const SID = @import("../lines.zig").SID;
const Block = @import("Block.zig");
const BlockData = @import("../inmem/BlockData.zig").BlockData;
const BlockHeader = @import("block_header.zig").BlockHeader;
const IndexBlockHeader = @import("IndexBlockHeader.zig");
const StreamWriter = @import("StreamWriter.zig");
const TableHeader = @import("TableHeader.zig");
const encoding = @import("encoding");

const Self = @This();

pub const indexBlockSize = 16 * 1024;
pub const indexBlockFlushThreshold = 128 * 1024;
pub const metaIndexSize = 4 * 1024;

const Content = union(enum) {
    block: *Block,
    data: *BlockData,
};

// state to the latestBlocks til not flushed
sid: ?SID,
minTimestamp: u64,
maxTimestamp: u64,
// state to the all written blocks
len: u32,
size: u32,
globalMinTimestamp: u64,
globalMaxTimestamp: u64,
blocksCount: u32,

// TODO: refactor this garbage to work with Wrter interface
indexBlockBuf: std.ArrayList(u8),
// TODO: make IndexBlockHeader as a value type
indexBlockHeader: *IndexBlockHeader,
metaIndexBuf: std.ArrayList(u8),

pub fn init(allocator: Allocator) !*Self {
    var indexBlockBuf = try std.ArrayList(u8).initCapacity(allocator, indexBlockSize);
    errdefer indexBlockBuf.deinit(allocator);
    var indexBlockHeader = try IndexBlockHeader.init(allocator);
    errdefer indexBlockHeader.deinit(allocator);
    var metaIndexBuf = try std.ArrayList(u8).initCapacity(allocator, metaIndexSize);
    errdefer metaIndexBuf.deinit(allocator);

    const bw = try allocator.create(Self);
    bw.* = Self{
        .sid = null,
        .minTimestamp = 0,
        .maxTimestamp = 0,

        .len = 0,
        .size = 0,
        .globalMinTimestamp = 0,
        .globalMaxTimestamp = 0,
        .blocksCount = 0,

        .indexBlockBuf = indexBlockBuf,
        .indexBlockHeader = indexBlockHeader,
        .metaIndexBuf = metaIndexBuf,
    };
    return bw;
}

pub fn deinit(self: *Self, allocator: Allocator) void {
    self.indexBlockBuf.deinit(allocator);
    self.indexBlockHeader.deinit(allocator);
    self.metaIndexBuf.deinit(allocator);
    allocator.destroy(self);
}

pub fn writeLines(
    self: *Self,
    allocator: Allocator,
    sid: SID,
    lines: []Line,
    streamWriter: *StreamWriter,
) !void {
    const block = try Block.initFromLines(allocator, lines);
    defer block.deinit(allocator);

    if (block.len() == 0) {
        return;
    }

    const c = Content{ .block = block };
    try self.writeBlock(allocator, c, sid, streamWriter);
}

pub fn writeData(
    self: *Self,
    allocator: Allocator,
    data: *BlockData,
    streamWriter: *StreamWriter,
) !void {
    const c = Content{ .data = data };
    try self.writeBlock(allocator, c, data.sid, streamWriter);
}

fn writeBlock(
    self: *Self,
    alloc: Allocator,
    // block: *Block,
    content: Content,
    sid: SID,
    streamWriter: *StreamWriter,
) !void {
    // TODO: assert incoming sid is growing,
    // because it expects the caller passes blocks ordered by sid,
    // assert in builting.is_test

    const hasState = self.sid != null;
    if (!hasState) {
        self.sid = sid;
    }

    const blockHeader = try writeContent(alloc, content, sid, streamWriter);

    if (self.len == 0 or blockHeader.timestampsHeader.min < self.globalMinTimestamp) {
        self.globalMinTimestamp = blockHeader.timestampsHeader.min;
    }
    if (self.len == 0 or blockHeader.timestampsHeader.max > self.globalMaxTimestamp) {
        self.globalMaxTimestamp = blockHeader.timestampsHeader.max;
    }
    if (!hasState or blockHeader.timestampsHeader.min < self.minTimestamp) {
        self.minTimestamp = blockHeader.timestampsHeader.min;
    }
    if (!hasState or blockHeader.timestampsHeader.max > self.maxTimestamp) {
        self.maxTimestamp = blockHeader.timestampsHeader.max;
    }

    self.size += blockHeader.size;
    self.len += blockHeader.len;
    self.blocksCount += 1;

    try self.indexBlockBuf.ensureUnusedCapacity(alloc, BlockHeader.encodeExpectedSize);
    const slice = self.indexBlockBuf.unusedCapacitySlice()[0..BlockHeader.encodeExpectedSize];
    const offset = blockHeader.encode(slice);
    self.indexBlockBuf.items.len += offset;
    if (self.indexBlockBuf.items.len > indexBlockFlushThreshold) {
        try self.flushIndexBlock(alloc, streamWriter);
    }
}

fn writeContent(alloc: Allocator, content: Content, sid: SID, streamWriter: *StreamWriter) !BlockHeader {
    switch (content) {
        .block => |block| {
            var blockHeader = BlockHeader.initFromBlock(block, sid);
            try streamWriter.writeBlock(alloc, block, &blockHeader);
            return blockHeader;
        },
        .data => |data| {
            var blockHeader = BlockHeader.initFromData(data, sid);
            try streamWriter.writeData(alloc, &blockHeader, data);
            return blockHeader;
        },
    }
}

pub fn finish(self: *Self, allocator: Allocator, streamWriter: *StreamWriter, th: *TableHeader) !void {
    th.uncompressedSize = self.size;
    th.len = self.len;
    th.blocksCount = self.blocksCount;
    th.minTimestamp = self.minTimestamp;
    th.maxTimestamp = self.maxTimestamp;
    th.bloomValuesBuffersAmount = @intCast(streamWriter.bloomValuesList.items.len);

    try self.flushIndexBlock(allocator, streamWriter);

    try streamWriter.writeColumnKeys(allocator);
    try streamWriter.writeColumnIndexes(allocator);

    try self.writeIndexBlockHeaders(allocator, streamWriter);

    th.compressedSize = streamWriter.size();
}

fn flushIndexBlock(self: *Self, allocator: Allocator, streamWriter: *StreamWriter) !void {
    defer self.indexBlockBuf.clearRetainingCapacity();
    if (self.indexBlockBuf.items.len > 0) {
        try self.indexBlockHeader.writeIndexBlock(
            allocator,
            &self.indexBlockBuf,
            self.sid.?,
            self.minTimestamp,
            self.maxTimestamp,
            streamWriter,
        );

        try self.metaIndexBuf.ensureUnusedCapacity(allocator, IndexBlockHeader.encodeExpectedSize);
        const slice = self.metaIndexBuf.unusedCapacitySlice()[0..IndexBlockHeader.encodeExpectedSize];
        const offset = self.indexBlockHeader.encode(slice);
        self.metaIndexBuf.items.len += offset;
    }

    self.sid = null;
    self.minTimestamp = 0;
    self.maxTimestamp = 0;
}

fn writeIndexBlockHeaders(self: *Self, allocator: Allocator, streamWriter: *StreamWriter) !void {
    const bound = try encoding.compressBound(self.metaIndexBuf.items.len);
    const slice = try streamWriter.metaIndexDst.allocSlice(allocator, bound);
    const offset = try encoding.compressAuto(slice, self.metaIndexBuf.items);

    try streamWriter.metaIndexDst.appendAllocated(slice, offset);
}
