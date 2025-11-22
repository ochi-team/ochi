const std = @import("std");

const SID = @import("../lines.zig").SID;
const StreamWriter = @import("stream_writer.zig").StreamWriter;
const Block = @import("block.zig").Block;
const Column = @import("block.zig").Column;
const Encoder = @import("encode.zig").Encoder;
const Decoder = @import("encode.zig").Decoder;

pub const BlockHeader = struct {
    sid: SID,
    size: u64,
    len: u32,
    timestampsHeader: TimestampsHeader,

    pub fn init(block: *Block, sid: SID) BlockHeader {
        return .{
            .sid = sid,
            .size = block.size(),
            .len = @intCast(block.len()),
            .timestampsHeader = .{
                .offset = 0,
                .size = 0,
                .min = 0,
                .max = 0,
            },
        };
    }

    const blockHeaderSize = @sizeOf(BlockHeader);

    pub fn encode(self: *BlockHeader, buf: *std.ArrayList(u8)) !void {
        self.sid.encode(buf);

        var enc = Encoder.init(buf);
        enc.writeInt(u64, self.size);
        enc.writeInt(u32, self.len);

        try self.timestampsHeader.encode(buf);
    }

    pub fn decode(buf: []const u8) !BlockHeader {
        var decoder = Decoder.init(buf);

        const sid = try SID.decode(try decoder.peek(32));

        const size = try decoder.readInt(u64);
        const len = try decoder.readInt(u32);

        const timestampsHeader = try TimestampsHeader.decode(&decoder);

        return .{
            .sid = sid,
            .size = size,
            .len = len,
            .timestampsHeader = timestampsHeader,
        };
    }
};

pub const TimestampsHeader = struct {
    offset: u64,
    size: u64,
    min: u64,
    max: u64,

    pub fn encode(self: *TimestampsHeader, buf: *std.ArrayList(u8)) !void {
        var enc = Encoder.init(buf);
        enc.writeInt(u64, self.offset);
        enc.writeInt(u64, self.size);
        enc.writeInt(u64, self.min);
        enc.writeInt(u64, self.max);
    }

    pub fn decode(decoder: *Decoder) !TimestampsHeader {
        const offset = try decoder.readInt(u64);
        const size = try decoder.readInt(u64);
        const min = try decoder.readInt(u64);
        const max = try decoder.readInt(u64);

        return .{
            .offset = offset,
            .size = size,
            .min = min,
            .max = max,
        };
    }
};

pub const ColumnsHeader = struct {
    headers: []ColumnHeader,
    celledColumns: []Column,

    pub fn init(allocator: std.mem.Allocator, block: *Block) !*ColumnsHeader {
        const cols = block.getColumns();
        const headers = try allocator.alloc(ColumnHeader, cols.len);
        errdefer allocator.free(headers);
        var inited: u16 = 0;
        errdefer {
            for (0..inited) |i| {
                headers[i].values.deinit(allocator);
            }
        }
        {
            for (0..headers.len) |i| {
                headers[i].values = try ColumnValues.init(allocator);
                inited += 1;
            }
        }

        const celledCols = block.getCelledColumns();

        const ch = try allocator.create(ColumnsHeader);
        ch.* = .{
            .headers = headers,
            .celledColumns = celledCols,
        };

        return ch;
    }

    pub fn deinit(self: *ColumnsHeader, allocator: std.mem.Allocator) void {
        for (0..self.headers.len) |i| {
            self.headers[i].values.deinit(allocator);
        }
        allocator.free(self.headers);
        allocator.destroy(self);
    }
};

pub const maxColumnValueSize = 256;
pub const maxColumnValuesLen = 8;
pub const ColumnValues = struct {
    values: std.ArrayList([]const u8),

    pub fn init(allocator: std.mem.Allocator) !ColumnValues {
        const values = try std.ArrayList([]const u8).initCapacity(allocator, maxColumnValuesLen);
        return .{
            .values = values,
        };
    }
    pub fn deinit(self: *ColumnValues, allocator: std.mem.Allocator) void {
        self.values.deinit(allocator);
    }

    pub fn set(self: *ColumnValues, v: []const u8) ?u8 {
        if (v.len > maxColumnValueSize) return null;

        var valSize: u16 = 0;
        for (0..self.values.items.len) |i| {
            if (std.mem.eql(u8, v, self.values.items[i])) {
                return @intCast(i);
            }

            valSize += @intCast(self.values.items[i].len);
        }
        if (self.values.items.len >= maxColumnValuesLen) return null;
        if (valSize + v.len > maxColumnValueSize) return null;

        // we don't allocate more than 8 elements
        self.values.appendAssumeCapacity(v);
        return @intCast(self.values.items.len - 1);
    }
};

pub const ColumnHeader = struct {
    key: []const u8,
    values: ColumnValues,
};

test {
    _ = @import("block_header_test.zig");
}
