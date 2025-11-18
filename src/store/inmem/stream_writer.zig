const std = @import("std");

const encoding = @import("encoding.zig");

const Block = @import("block.zig").Block;
const BlockHeader = @import("block_header.zig").BlockHeader;
const TimestampsHeader = @import("block_header.zig").TimestampsHeader;

pub const StreamWriter = struct {
    const tsBufferSize = 120 * 1024;
    const indexBufferSize = 120 * 1024;

    // TODO: expose metrics on len/cap relations
    timestampsBuffer: std.ArrayList(u8),
    indexBuffer: std.ArrayList(u8),

    pub fn init(allocator: std.mem.Allocator) !*StreamWriter {
        var timestampsBuffer = try std.ArrayList(u8).initCapacity(allocator, tsBufferSize);
        errdefer timestampsBuffer.deinit(allocator);
        var indexBuffer = try std.ArrayList(u8).initCapacity(allocator, indexBufferSize);
        errdefer indexBuffer.deinit(allocator);

        const w = try allocator.create(StreamWriter);
        w.* = StreamWriter{
            .timestampsBuffer = timestampsBuffer,
            .indexBuffer = indexBuffer,
        };
        return w;
    }

    pub fn deinit(self: *StreamWriter, allocator: std.mem.Allocator) void {
        self.timestampsBuffer.deinit(allocator);
        self.indexBuffer.deinit(allocator);
        allocator.destroy(self);
    }

    pub fn writeBlock(self: *StreamWriter, allocator: std.mem.Allocator, block: *Block, blockHeader: *BlockHeader) !void {
        try self.writeTimestamps(allocator, &blockHeader.timestampsHeader, block.timestamps);
    }

    fn writeTimestamps(self: *StreamWriter, allocator: std.mem.Allocator, tsHeader: *TimestampsHeader, timestamps: []u64) !void {
        if (timestamps.len == 0) {
            @panic("writer: given empty timestamps slice");
        }
        // TODO: pass static buffer instead of allocator
        const encodedTimestamps = try encoding.encodeTimestamps(allocator, timestamps);
        defer allocator.free(encodedTimestamps);
        // TODO: write tsHeader data from encodedTimestamps

        tsHeader.min = timestamps[0];
        tsHeader.max = timestamps[timestamps.len - 1];
        tsHeader.offset = self.timestampsBuffer.items.len;
        tsHeader.size = encodedTimestamps.len;

        try self.timestampsBuffer.appendSliceBounded(encodedTimestamps);
    }
};
