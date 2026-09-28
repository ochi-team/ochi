const std = @import("std");

const encoding = @import("encoding");
const Encoder = encoding.Encoder;
const Decoder = encoding.Decoder;

// makes no sense to keep large values in invariant columns,
// it won't help to improve performance
pub const maxInvariantColumnValueSize = 256;

const Column = @This();

key: []const u8,
values: [][]const u8,

pub fn isInvariant(self: *Column) bool {
    if (self.values.len == 0) {
        return true;
    }

    for (1..self.values.len) |i| {
        if (!std.mem.eql(u8, self.values[i], self.values[0])) {
            return false;
        }
    }

    return true;
}

pub fn encodeAsInvariant(self: *Column, enc: *Encoder, comptime encodeKey: bool) void {
    if (encodeKey) {
        enc.writeString(self.key);
    }
    enc.writeString(self.values[0]);
}

pub fn invariantBound(self: *const Column, comptime encodeKey: bool) usize {
    var size: usize = 0;
    if (encodeKey) {
        size += Encoder.varIntBound(self.key.len) + self.key.len;
    }
    size += Encoder.varIntBound(self.values[0].len) + self.values[0].len;
    return size;
}

pub fn decodeAsInvariant(dec: *Decoder, allocator: std.mem.Allocator, comptime decodeKey: bool) !Column {
    var key: []const u8 = undefined;
    if (decodeKey) {
        key = dec.readString();
    }
    const value = dec.readString();
    const values = try allocator.alloc([]const u8, 1);
    values[0] = value;
    return .{
        .key = key,
        .values = values,
    };
}

test {
    _ = @import("Column_test.zig");
}
