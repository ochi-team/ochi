const std = @import("std");

const encoding = @import("encoding");
const Encoder = encoding.Encoder;
const Decoder = encoding.Decoder;

// makes no sense to keep large values in celled columns,
// it won't help to improve performance
pub const maxCelledColumnValueSize = 256;

const Self = @This();

key: []const u8,
values: [][]const u8,

// TODO: rename this crap, to "uniform" I guess
// 1. uniform ⭐ - Clear, descriptive, commonly used in data contexts
// 2. constant ⭐ - Mathematically precise, well-understood
// 3. invariant - Emphasizes that the value doesn't vary
// 4. homogeneous - Academic but precise
// 5. singular - Has a single unique value
// 6. static - Doesn't change across rows
// 7. repeated - Same value repeated
// 8. monolithic - Single-valued
// 9. fixed - Fixed across all rows
// 10. scalar - Single scalar value for the column
pub fn isCelled(self: *Self) bool {
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

pub fn encodeAsCelled(self: *Self, enc: *Encoder, comptime encodeKey: bool) void {
    if (encodeKey) {
        enc.writeString(self.key);
    }
    enc.writeString(self.values[0]);
}

pub fn celledBound(self: *const Self, comptime encodeKey: bool) usize {
    var size: usize = 0;
    if (encodeKey) {
        size += Encoder.maxVarUint64Len + self.key.len;
    }
    size += Encoder.maxVarUint64Len + self.values[0].len;
    return size;
}

pub fn decodeAsCelled(dec: *Decoder, allocator: std.mem.Allocator, comptime decodeKey: bool) !Self {
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

test "Self.encodeAsCelled" {
    const alloc = std.testing.allocator;

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
            const bufSize = column.celledBound(toEncodeKey);
            const buf = try alloc.alloc(u8, bufSize);
            defer alloc.free(buf);

            var enc = Encoder.init(buf);
            column.encodeAsCelled(&enc, toEncodeKey);

            // Decode
            var dec = Decoder.init(buf[0..enc.offset]);
            const decoded = try Self.decodeAsCelled(&dec, alloc, toEncodeKey);
            defer alloc.free(decoded.values);

            // Verify
            if (toEncodeKey) {
                try std.testing.expectEqualStrings(case.key, decoded.key);
            }
            try std.testing.expectEqual(1, decoded.values.len);
            try std.testing.expectEqualStrings(case.value, decoded.values[0]);
        }
    }
}
