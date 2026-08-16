const std = @import("std");
const Allocator = std.mem.Allocator;

const Table = @import("Table.zig");
const BlockHeader = @import("BlockHeader.zig");
const Query = @import("../../query/Query.zig");
const FilterExpression = Query.FilterExpression;

const BlockQuery = @This();

bitset: std.bit_set.DynamicBitSetUnmanaged,

pub fn init(
    self: *BlockQuery,
    alloc: Allocator,
    len: usize,
) !BlockQuery {
    const bitset: std.bit_set.DynamicBitSetUnmanaged = try .initFull(alloc, len);
    self.* = .{
        .bitset = bitset,
    };
    self.bitset.setAll();
}

pub fn deinit(self: *BlockQuery, alloc: Allocator) void {
    self.bitset.deinit(alloc);
}

pub fn query(
    self: *BlockQuery,
    table: *const Table,
    blockHeader: *const BlockHeader,
    q: *const Query,
) !void {
    _ = table;
    _ = blockHeader;
    if (q.fieldsExpr) |fieldsExpr| {
        try self.filterByExpr(fieldsExpr);
    }

    if (bitsetIsEmpty(&self.bitset)) {
        return;
    }

    unreachable;
}

fn filterByExpr(self: *BlockQuery, fieldsExpr: *const FilterExpression) !void {
    switch (fieldsExpr) {
        .orOp => |orExpr| try self.filterOr(orExpr),
        .andOp => |andExpr| try self.filterAnd(andExpr),
        .predicate => |predicate| try self.filterPredicate(predicate),
    }
}

fn filterOr(self: *BlockQuery, expr: [2]*const FilterExpression) !void {
    if (!self.matchBloomFilterOr(expr)) {
        self.bitset.unsetAll();
        return;
    }

    unreachable;
}

fn bitsetIsEmpty(bitset: *const std.bit_set.DynamicBitSetUnmanaged) bool {
    const tail: usize = if (bitset.bit_length % @bitSizeOf(std.bit_set.DynamicBitSetUnmanaged.MaskInt) > 0) 1 else 0;
    const wordsCount: usize = bitset.bit_length / @bitSizeOf(std.bit_set.DynamicBitSetUnmanaged.MaskInt) + tail;
    for (0..wordsCount) |i| {
        if (bitset.masks[i] > 0) return false;
    }

    return true;
}

const testing = std.testing;

test "bitsetIsEmpty" {
    const f = struct {
        fn f(alloc: Allocator, len: usize, set: []const usize, expectedEmpty: bool) !void {
            var bitset: std.bit_set.DynamicBitSetUnmanaged = try .initEmpty(alloc, len);
            defer bitset.deinit(alloc);

            for (set) |i| {
                bitset.set(i);
            }

            try testing.expectEqual(expectedEmpty, bitsetIsEmpty(&bitset));
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
