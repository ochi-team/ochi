const std = @import("std");

const encode = @import("encode.zig");

const Block = @import("block.zig").Block;
const Column = @import("block.zig").Column;
const BlockHeader = @import("block_header.zig").BlockHeader;
const ColumnsHeader = @import("block_header.zig").ColumnsHeader;
const ColumnHeader = @import("block_header.zig").ColumnHeader;
const TimestampsHeader = @import("block_header.zig").TimestampsHeader;

pub const StreamWriter = struct {
    const tsBufferSize = 120 * 1024;
    const indexBufferSize = 120 * 1024;
    const metaIndexBufferSize = 120 * 1024;

    // TODO: expose metrics on len/cap relations
    timestampsBuffer: std.ArrayList(u8),
    indexBuffer: std.ArrayList(u8),
    metaIndexBuf: std.ArrayList(u8),

    pub fn init(allocator: std.mem.Allocator) !*StreamWriter {
        var timestampsBuffer = try std.ArrayList(u8).initCapacity(allocator, tsBufferSize);
        errdefer timestampsBuffer.deinit(allocator);
        var indexBuffer = try std.ArrayList(u8).initCapacity(allocator, indexBufferSize);
        errdefer indexBuffer.deinit(allocator);
        var metaIndexBuf = try std.ArrayList(u8).initCapacity(allocator, metaIndexBufferSize);
        errdefer metaIndexBuf.deinit(allocator);

        const w = try allocator.create(StreamWriter);
        w.* = StreamWriter{
            .timestampsBuffer = timestampsBuffer,
            .indexBuffer = indexBuffer,
            .metaIndexBuf = metaIndexBuf,
        };
        return w;
    }

    pub fn deinit(self: *StreamWriter, allocator: std.mem.Allocator) void {
        self.timestampsBuffer.deinit(allocator);
        self.indexBuffer.deinit(allocator);
        self.metaIndexBuf.deinit(allocator);
        allocator.destroy(self);
    }

    pub fn writeBlock(self: *StreamWriter, allocator: std.mem.Allocator, block: *Block, blockHeader: *BlockHeader) !void {
        try self.writeTimestamps(allocator, &blockHeader.timestampsHeader, block.timestamps);

        const columnsHeader = try ColumnsHeader.init(allocator, block);
        defer columnsHeader.deinit(allocator);
        for (block.getColumns(), 0..) |col, i| {
            var header = columnsHeader.headers[i];
            try self.writeColumnHeader(allocator, col, &header);
        }
    }

    fn writeTimestamps(self: *StreamWriter, allocator: std.mem.Allocator, tsHeader: *TimestampsHeader, timestamps: []u64) !void {
        if (timestamps.len == 0) {
            @panic("writer: given empty timestamps slice");
        }
        // TODO: pass static buffer instead of allocator
        const encodedTimestamps = try encode.encodeTimestamps(allocator, timestamps);
        defer allocator.free(encodedTimestamps);
        // TODO: write tsHeader data from encodedTimestamps

        tsHeader.min = timestamps[0];
        tsHeader.max = timestamps[timestamps.len - 1];
        tsHeader.offset = self.timestampsBuffer.items.len;
        tsHeader.size = encodedTimestamps.len;

        try self.timestampsBuffer.appendSlice(allocator, encodedTimestamps);
    }

    fn writeColumnHeader(_: *StreamWriter, allocator: std.mem.Allocator, col: Column, ch: *ColumnHeader) !void {
        ch.key = col.key;

        // TODO: get bloom column values

        const valuesEncoder = try encode.ValuesEncoder.init(allocator);
        defer valuesEncoder.deinit();
        const valueType = try valuesEncoder.encode(col.values, &ch.dict);
        ch.type = valueType.type;
        ch.min = valueType.min;
        ch.max = valueType.max;
    }
};
