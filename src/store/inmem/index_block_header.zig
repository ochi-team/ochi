const std = @import("std");

const SID = @import("../lines.zig").SID;
const StreamWriter = @import("stream_writer.zig").StreamWriter;
const Encoder = @import("encode.zig").Encoder;

pub const IndexBlockHeader = struct {
    sid: ?SID,
    minTs: u64,
    maxTs: u64,

    offset: u64,
    size: u64,

    pub fn init(allocator: std.mem.Allocator) !*IndexBlockHeader {
        const bh = try allocator.create(IndexBlockHeader);
        bh.* = .{
            .sid = null,
            .minTs = 0,
            .maxTs = 0,
            .offset = 0,
            .size = 0,
        };
        return bh;
    }

    pub fn deinit(self: *IndexBlockHeader, allocator: std.mem.Allocator) void {
        allocator.destroy(self);
    }

    pub fn writeIndexBlock(self: *IndexBlockHeader, indexBlockBuf: *std.ArrayList(u8), sid: SID, minTs: u64, maxTs: u64, streamWriter: *StreamWriter) !void {
        if (indexBlockBuf.items.len == 0) {
            return;
        }

        self.sid = sid;
        self.minTs = minTs;
        self.maxTs = maxTs;

        // TODO: compress zstd or openzl
        self.offset = streamWriter.indexBuffer.items.len;
        self.size = indexBlockBuf.items.len;

        try streamWriter.indexBuffer.appendSliceBounded(indexBlockBuf.items);
    }

    pub fn encode(self: *IndexBlockHeader, buf: *std.ArrayList(u8)) !void {
        var enc = Encoder.init(buf);
        try self.sid.?.encode(buf);
        try enc.writeInt(u64, self.minTs);
        try enc.writeInt(u64, self.maxTs);
        try enc.writeInt(u64, self.offset);
        try enc.writeInt(u64, self.size);
    }
};
