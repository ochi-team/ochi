const std = @import("std");

const Line = @import("../lines.zig").Line;
const SID = @import("../lines.zig").SID;
const Block = @import("block.zig").Block;
const BlockHeader = @import("block_header.zig").BlockHeader;
const IndexBlockHeader = @import("IndexBlockHeader.zig");
const StreamWriter = @import("stream_writer.zig").StreamWriter;
const TableHeader = @import("TableHeader.zig");
const encoding = @import("encoding");
const ColumnIDGen = @import("ColumnIDGen.zig");

const Self = @This();

const currentVersion = 1;

pub const indexBlockSize = 16 * 1024;
pub const indexBlockFlushThreshold = 128 * 1024;
pub const metaIndexSize = 4 * 1024;

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
//
indexBlockBuf: std.ArrayList(u8),
indexBlockHeader: *IndexBlockHeader,
metaIndexBuf: std.ArrayList(u8),

pub fn init(allocator: std.mem.Allocator) !*Self {
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

pub fn deinit(self: *Self, allocator: std.mem.Allocator) void {
    self.indexBlockBuf.deinit(allocator);
    self.indexBlockHeader.deinit(allocator);
    self.metaIndexBuf.deinit(allocator);
    allocator.destroy(self);
}

pub fn writeLines(
    self: *Self,
    allocator: std.mem.Allocator,
    sid: SID,
    lines: []*const Line,
    streamWriter: *StreamWriter,
) !void {
    const block = try Block.init(allocator, lines);
    defer block.deinit(allocator);
    try self.writeBlock(allocator, block, sid, streamWriter);
}

fn writeBlock(
    self: *Self,
    allocator: std.mem.Allocator,
    block: *Block,
    sid: SID,
    streamWriter: *StreamWriter,
) !void {
    if (block.len() == 0) {
        return;
    }

    const hasState = self.sid != null;
    if (!hasState) {
        self.sid = sid;
    }

    var blockHeader = BlockHeader.init(block, sid);
    try streamWriter.writeBlock(allocator, block, &blockHeader);

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

    try self.indexBlockBuf.ensureUnusedCapacity(allocator, BlockHeader.encodeExpectedSize);
    const slice = self.indexBlockBuf.unusedCapacitySlice()[0..BlockHeader.encodeExpectedSize];
    const offset = blockHeader.encode(slice);
    self.indexBlockBuf.items.len += offset;
    if (self.indexBlockBuf.items.len > indexBlockFlushThreshold) {
        try self.flushIndexBlock(allocator, streamWriter);
    }
}

pub fn finish(self: *Self, allocator: std.mem.Allocator, streamWriter: *StreamWriter, th: *TableHeader) !void {
    th.version = currentVersion;
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

fn flushIndexBlock(self: *Self, allocator: std.mem.Allocator, streamWriter: *StreamWriter) !void {
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
        const offset = try self.indexBlockHeader.encode(slice);
        self.metaIndexBuf.items.len += offset;
    }
    self.sid = null;
    self.minTimestamp = 0;
    self.maxTimestamp = 0;
}

fn writeIndexBlockHeaders(self: *Self, allocator: std.mem.Allocator, streamWriter: *StreamWriter) !void {
    const bound = try encoding.compressBound(self.metaIndexBuf.items.len);
    try streamWriter.metaIndexBuf.ensureUnusedCapacity(allocator, bound);
    const slice = streamWriter.metaIndexBuf.unusedCapacitySlice()[0..bound];
    const offset = try encoding.compressAuto(slice, self.metaIndexBuf.items);
    streamWriter.metaIndexBuf.items.len += offset;
}
