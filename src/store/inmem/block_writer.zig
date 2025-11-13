const std = @import("std");

const Field = @import("../lines.zig").Field;
const Lines = @import("../lines.zig").Lines;
const Line = @import("../lines.zig").Line;
const SID = @import("../lines.zig").SID;

const Block = @import("block.zig").Block;
const BlockHeader = @import("block_header.zig").BlockHeader;

const StreamWriter = @import("stream_writer.zig").StreamWriter;

pub const BlockWriter = struct {
    // indexBlockHeader: *IndexBlockHeader,
    //
    // // state
    sid: ?SID,
    //
    // indexBlockBuf: []u8,

    pub fn init(allocator: std.mem.Allocator) !*BlockWriter {
        const w = try allocator.create(BlockWriter);
        // const indexBlockHeader = try IndexBlockHeader.init(allocator);
        // const indexBlockBuf = try allocator.alloc(u8, 20 * 1024);
        w.* = BlockWriter{
            // .indexBlockHeader = indexBlockHeader,
            .sid = null,
            // .indexBlockBuf = indexBlockBuf,
        };
        return w;
    }

    pub fn deinint(self: *BlockWriter, allocator: std.mem.Allocator) void {
        // allocator.free(self.indexBlockBuf);
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
        try streamWriter.writeBlockallocator, block, &blockHeader);

        // TODO: update block header and timestamp header stats

        //     blockHeader.encode(self.indexBlockBuf);
        //
        //     // TODO: implement growing buffer of blockIndex and flush only if it reached ~128kb
        //     if (true) {
        //         self.flushIndexBlock();
        //         self.indexBlockBuf.len = 0;
        //     }
    }

    fn flushIndexBlock(self: *BlockWriter) void {
        _ = self;
        // if (self.indexBlockBuf.len > 0) {
        //     self.indexBlockHeader.writeIndexBlock(self.indexBlockBuf, self.streamWriter.indexBuffer);
        //     // TODO: write meta index block
        // }
        // self.sidFirst = null;
    }

    pub fn finish(self: *BlockWriter) void {
        self.flushIndexBlock();
        // TODO write column names, column indexes, meta indexes
    }
};
