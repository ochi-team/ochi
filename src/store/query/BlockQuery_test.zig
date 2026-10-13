const std = @import("std");
const Allocator = std.mem.Allocator;

const BlockQuery = @import("BlockQuery.zig");

const testing = std.testing;

test "bitsetIsEmpty" {
    const f = struct {
        fn f(alloc: Allocator, len: usize, set: []const usize, expectedEmpty: bool) !void {
            var bitset: std.bit_set.DynamicBitSetUnmanaged = try .initEmpty(alloc, len);
            defer bitset.deinit(alloc);

            for (set) |i| {
                bitset.set(i);
            }

            try testing.expectEqual(expectedEmpty, BlockQuery.bitsetIsEmpty(&bitset));
        }
    }.f;

    const alloc = testing.allocator;
    try f(alloc, 0, &.{}, true);
    try f(alloc, 1, &.{}, true);
    try f(alloc, 1, &.{0}, false);
    try f(alloc, 64, &.{42}, false);
    try f(alloc, 64, &.{}, true);
    try f(alloc, 129, &.{128}, false);
    try f(alloc, 128, &.{127}, false);
    try f(alloc, 127, &.{126}, false);
}

test "bitsetAndNot" {
    const alloc = std.testing.allocator;
    const Set = std.bit_set.DynamicBitSetUnmanaged;
    // 130 bits: spans three words with a partial tail
    var bm: Set = try .initFull(alloc, 130);
    defer bm.deinit(alloc);
    var x: Set = try .initEmpty(alloc, 130);
    defer x.deinit(alloc);

    BlockQuery.bitsetAndNot(&bm, &x);
    try std.testing.expectEqual(@as(usize, 130), bm.count());

    x.set(0);
    x.set(64);
    x.set(129);
    BlockQuery.bitsetAndNot(&bm, &x);
    try std.testing.expectEqual(@as(usize, 127), bm.count());
    try std.testing.expect(!bm.isSet(0) and !bm.isSet(64) and !bm.isSet(129));
    try std.testing.expect(bm.isSet(1) and bm.isSet(128));
}
