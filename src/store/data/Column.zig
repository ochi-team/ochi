const std = @import("std");

const encoding = @import("encoding");
const Encoder = encoding.Encoder;
const Decoder = encoding.Decoder;

// makes no sense to keep large values in invariant columns,
// it won't help to improve performance
pub const maxInvariantColumnValueSize = 256;

const Self = @This();

key: []const u8,
values: [][]const u8,

pub fn isInvariant(self: *Self) bool {
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

pub fn encodeAsInvariant(self: *Self, enc: *Encoder, comptime encodeKey: bool) void {
    if (encodeKey) {
        enc.writeString(self.key);
    }
    enc.writeString(self.values[0]);
}

pub fn invariantBound(self: *const Self, comptime encodeKey: bool) usize {
    var size: usize = 0;
    if (encodeKey) {
        size += Encoder.varIntBound(self.key.len) + self.key.len;
    }
    size += Encoder.varIntBound(self.values[0].len) + self.values[0].len;
    return size;
}

pub fn decodeAsInvariant(dec: *Decoder, allocator: std.mem.Allocator, comptime decodeKey: bool) !Self {
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

const testing = std.testing;

test "Self.encodeAsInvariant" {
    const alloc = testing.allocator;

    const Case = struct {
        key: []const u8,
        value: []const u8,
    };

    const cases = &[_]Case{
        .{ .key = "column1", .value = "constant_value" },
        .{ .key = "col", .value = "" },
        .{ .key = "", .value = "value" },
        .{ .key = "long_column_name", .value = "some data here" },
    };

    for (cases) |case| {
        inline for (&[_]bool{ true, false }) |toEncodeKey| {
            // Create column with single value
            const values = try alloc.alloc([]const u8, 1);
            values[0] = case.value;
            defer alloc.free(values);

            var column = Self{
                .key = case.key,
                .values = values,
            };

            // Encode without key
            const bufSize = column.invariantBound(toEncodeKey);
            const buf = try alloc.alloc(u8, bufSize);
            defer alloc.free(buf);

            var enc = Encoder.init(buf);
            column.encodeAsInvariant(&enc, toEncodeKey);

            // Decode
            var dec = Decoder.init(buf[0..enc.offset]);
            const decoded = try Self.decodeAsInvariant(&dec, alloc, toEncodeKey);
            defer alloc.free(decoded.values);

            // Verify
            if (toEncodeKey) {
                try testing.expectEqualStrings(case.key, decoded.key);
            }
            try testing.expectEqual(1, decoded.values.len);
            try testing.expectEqualStrings(case.value, decoded.values[0]);
        }
    }
}
