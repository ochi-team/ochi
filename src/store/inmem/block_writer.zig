const std = @import("std");

const Line = @import("../lines.zig").Line;
const SID = @import("../lines.zig").SID;

const Block = @import("block.zig").Block;
const BlockHeader = @import("block_header.zig").BlockHeader;
const IndexBlockHeader = @import("index_block_header.zig").IndexBlockHeader;

const StreamWriter = @import("stream_writer.zig").StreamWriter;

pub const BlockWriter = struct {
    pub const indexBlockSize = 164 * 1024;
    pub const indexBlockFlushThreshold = indexBlockSize - 32 * 1024;
    pub const metaIndexSize = 128 * 1024;

    // state to the latestBlocks til not flushed
    sid: ?SID,
    minTimestamp: u64,
    maxTimestamp: u64,
    // state to the all written blocks
    len: u32,
    size: u64,
    globalMinTimestamp: u64,
    globalMaxTimestamp: u64,
    blocksCount: u32,
    //
    indexBlockBuf: std.ArrayList(u8),
    indexBlockHeader: *IndexBlockHeader,
    metaIndexBuf: std.ArrayList(u8),

    pub fn init(allocator: std.mem.Allocator) !*BlockWriter {
        var indexBlockBuf = try std.ArrayList(u8).initCapacity(allocator, indexBlockSize);
        errdefer indexBlockBuf.deinit(allocator);
        var indexBlockHeader = try IndexBlockHeader.init(allocator);
        errdefer indexBlockHeader.deinit(allocator);
        var metaIndexBuf = try std.ArrayList(u8).initCapacity(allocator, metaIndexSize);
        errdefer metaIndexBuf.deinit(allocator);

        const bw = try allocator.create(BlockWriter);
        bw.* = BlockWriter{
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

    pub fn deinit(self: *BlockWriter, allocator: std.mem.Allocator) void {
        self.indexBlockBuf.deinit(allocator);
        self.indexBlockHeader.deinit(allocator);
        self.metaIndexBuf.deinit(allocator);
        allocator.destroy(self);
    }

    pub fn writeLines(self: *BlockWriter, allocator: std.mem.Allocator, sid: SID, lines: []*const Line, streamWriter: *StreamWriter) !void {
        const block = try Block.init(allocator, lines);
        defer block.deinit(allocator);
        try self.writeBlock(allocator, block, sid, streamWriter);
    }

    fn writeBlock(self: *BlockWriter, allocator: std.mem.Allocator, block: *Block, sid: SID, streamWriter: *StreamWriter) !void {
        if (block.len() == 0) {
            return;
        }

        const hasState = self.sid != null;
        if (!hasState) {
            self.sid = sid;
        }

        // TODO: write block headers (block header, timestampt header, column header) and update its stats to block writer
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

        try blockHeader.encode(&self.indexBlockBuf);
        if (self.indexBlockBuf.capacity - self.indexBlockBuf.items.len < indexBlockFlushThreshold) {
            try self.flushIndexBlock(allocator, streamWriter);
        }
    }

    pub fn finish(self: *BlockWriter, allocator: std.mem.Allocator, streamWriter: *StreamWriter) !void {
        try self.flushIndexBlock(allocator, streamWriter);

        // TODO: compress metaindexbuf before writing
        try streamWriter.metaIndexBuf.appendSlice(allocator, self.metaIndexBuf.items);
    }

    fn flushIndexBlock(self: *BlockWriter, allocator: std.mem.Allocator, streamWriter: *StreamWriter) !void {
        defer self.indexBlockBuf.clearRetainingCapacity();
        if (self.indexBlockBuf.items.len > 0) {
            try self.indexBlockHeader.writeIndexBlock(allocator, &self.indexBlockBuf, self.sid.?, self.minTimestamp, self.maxTimestamp, streamWriter);
            self.indexBlockHeader.encode(&self.metaIndexBuf);
        }
        self.sid = null;
        self.minTimestamp = 0;
        self.maxTimestamp = 0;
    }
};
