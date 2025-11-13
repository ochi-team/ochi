const std = @import("std");

const encoding = @import("encoding.zig");

const Block = @import("block.zig").Block;
const Buffer = @import("buffer.zig").Buffer;
const BlockHeader = @import("block_header.zig").BlockHeader;
const TimestampsHeader = @import("block_header.zig").TimestampsHeader;

pub const StreamWriter = struct {
    timestampsBuffer: *Buffer,

    pub fn init(allocator: std.mem.Allocator) !*StreamWriter {
        const tsBuffer = try Buffer.init(allocator);

        const w = try allocator.create(StreamWriter);
        w.* = StreamWriter{
            .timestampsBuffer = tsBuffer,
        };
        return w;
    }

    pub fn deinit(self: *StreamWriter, allocator: std.mem.Allocator) void {
        self.timestampsBuffer.deinit(allocator);
        allocator.destroy(self);
    }

    pub fn writeBlock(self: *StreamWriter, allocator: std.mem.Allocator, block: *Block, blockHeader: *BlockHeader) !void {
        try self.writeTimestamps(allocator, &blockHeader.timestampsHeader, block.timestamps);
    }

    fn writeTimestamps(self: *StreamWriter, allocator: std.mem.Allocator, tsHeader: *TimestampsHeader, timestamps: []u64) !void {
        if (timestamps.len == 0) {
            @panic("writer: given empty timestamps slice");
        }
        const encodedTimestamps = try encoding.encodeTimestamps(allocator, timestamps);
        defer allocator.free(encodedTimestamps);
        // TODO: write tsHeader data from encodedTimestamps

        tsHeader.min = timestamps[0];
        tsHeader.max = timestamps[timestamps.len - 1];
        tsHeader.offset = self.timestampsBuffer.len;
        tsHeader.size = encodedTimestamps.len;

        self.timestampsBuffer.write(encodedTimestamps);
    }
};
