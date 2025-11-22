const std = @import("std");
const testing = std.testing;
const block_header = @import("block_header.zig");
const ColumnValues = block_header.ColumnDict;

test "setReturnsNullOnExceedingMaxColumnValueSize" {
    var cv = try ColumnValues.init(testing.allocator);
    defer cv.deinit(testing.allocator);

    const oversized_value = try testing.allocator.alloc(u8, block_header.maxColumnValueSize + 1);
    defer testing.allocator.free(oversized_value);

    const result = cv.set(oversized_value);
    try testing.expect(result == null);
}

test "setReturnsNullOnExceedingTotalValueSize" {
    var cv = try ColumnValues.init(testing.allocator);
    defer cv.deinit(testing.allocator);

    const v1 = try testing.allocator.alloc(u8, block_header.maxColumnValueSize / 2);
    const v2 = try testing.allocator.alloc(u8, block_header.maxColumnValueSize / 2);
    const v3 = try testing.allocator.alloc(u8, block_header.maxColumnValueSize / 2);
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
    var cv = try ColumnValues.init(testing.allocator);
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
