const std = @import("std");

const encoding = @import("encoding");
const Encoder = encoding.Encoder;
const Decoder = encoding.Decoder;

// makes no sense to keep large values in invariant columns,
// it won't help to improve performance
pub const maxInvariantColumnValueSize = 256;

const Column = @import("Column.zig");

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

            var column = Column{
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
            const decoded = try Column.decodeAsInvariant(&dec, alloc, toEncodeKey);
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
