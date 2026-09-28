const std = @import("std");

const Encoder = @import("encoding").Encoder;
const Decoder = @import("encoding").Decoder;

pub const maxDictColumnValueSize = 256;
pub const maxDictColumnValuesLen = 8;

const Self = @import("ColumnDict.zig");

const testing = std.testing;

test "setReturnsNullOnExceedingMaxColumnValueSize" {
    var cv = try Self.init(testing.allocator);
    defer cv.deinit(testing.allocator);

    const oversized_value = try testing.allocator.alloc(u8, maxDictColumnValueSize + 1);
    defer testing.allocator.free(oversized_value);

    const result = cv.set(oversized_value);
    try testing.expect(result == null);
}

test "setReturnsNullOnExceedingTotalValueSize" {
    var cv = try Self.init(testing.allocator);
    defer cv.deinit(testing.allocator);

    const v1 = try testing.allocator.alloc(u8, maxDictColumnValueSize / 2);
    const v2 = try testing.allocator.alloc(u8, maxDictColumnValueSize / 2);
    const v3 = try testing.allocator.alloc(u8, maxDictColumnValueSize / 2);
    defer testing.allocator.free(v1);
    defer testing.allocator.free(v2);
    defer testing.allocator.free(v3);

    // fill with some data
    @memset(v1, 'a');
    const r1 = cv.set(v1);
    try testing.expect(r1 != null);

    @memset(v2, 'b');
    const r2 = cv.set(v2);
    try testing.expect(r2 != null);

    // this should fail
    @memset(v3, 'c');
    const r3 = cv.set(v3);
    try testing.expect(r3 == null);
}

test "setReturnsNullOnExceedingTotalValuesLen" {
    var cv = try Self.init(testing.allocator);
    defer cv.deinit(testing.allocator);

    var testValues: [8][]const u8 = undefined;
    for (0..8) |i| {
        testValues[i] = try testing.allocator.dupe(u8, &[_]u8{@intCast(i)});
    }
    defer {
        for (0..8) |i| {
            testing.allocator.free(testValues[i]);
        }
    }

    // fill with some data
    for (0..8) |i| {
        const r = cv.set(testValues[i]);
        try testing.expect(r != null);
    }

    const r = cv.set("1a");
    try testing.expect(r == null);
}

test "ColumnDictEncode" {
    const alloc = testing.allocator;

    const Case = struct {
        values: []const []const u8,
    };

    const cases = &[_]Case{
        .{
            .values = &[_][]const u8{},
        },
        .{
            .values = &[_][]const u8{"value1"},
        },
        .{
            .values = &[_][]const u8{ "value1", "value2", "value3" },
        },
        .{
            .values = &[_][]const u8{ "a", "b", "c", "d", "e", "f", "g", "h" },
        },
        .{
            .values = &[_][]const u8{ "", "non-empty", "another" },
        },
    };

    for (cases) |case| {
        var dict = try Self.init(alloc);
        defer dict.deinit(alloc);

        // Populate dict
        for (case.values) |value| {
            dict.values.appendAssumeCapacity(value);
        }

        // Encode
        const bufSize = dict.bound();
        const buf = try alloc.alloc(u8, bufSize);
        defer alloc.free(buf);

        var enc = Encoder.init(buf);
        dict.encode(&enc);

        // Decode
        var dec = Decoder.init(buf[0..enc.offset]);
        var decoded = try Self.decode(&dec, alloc);
        defer decoded.deinit(alloc);

        // Verify - now we can use expectEqualDeep since capacity is consistent
        try testing.expectEqualDeep(dict, decoded);
    }
}
