const std = @import("std");
const Allocator = std.mem.Allocator;

const Table = @import("../data/Table.zig");
const BlockHeader = @import("../data/BlockHeader.zig");
const Query = @import("../../query/Query.zig");
const FilterExpression = Query.FilterExpression;

const Tokenizer = @import("../bloom/Tokenizer.zig");

const BlockResponseSlice = @import("BlockResponseSlice.zig");

pub const Ctx = struct {
    alloc: Allocator,
    tokenizer: *Tokenizer,
};

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
    self: *const BlockQuery,
    alloc: Allocator,
    table: *const Table,
    blockHeader: *const BlockHeader,
    q: *const Query,
) !void {
    // FIXME
    unreachable;

    var ctx: Ctx = undefined;
    _ = table;
    _ = blockHeader;
    if (q.fieldsExpr) |fieldsExpr| {
        try filterByExpr(&ctx, fieldsExpr);
    }

    if (bitsetIsEmpty(&self.bitset)) {
        return;
    }

    return BlockResponseSlice.init();
}

fn filterByExpr(fieldsExpr: *const FilterExpression) !void {
    switch (fieldsExpr) {
        .orOp => |orExpr| try filterOr(orExpr),
        .andOp => |andExpr| try filterAnd(andExpr),
        .predicate => |predicate| try filterPredicate(predicate),
    }
}

fn filterOr(expr: [2]*const FilterExpression) !void {
    if (!self.matchBloomFilterOr(expr)) {
        self.bitset.unsetAll();
        return;
    }

    unreachable;
}

fn matchBloomFilterOr(self: *const BlockQuery, expr: [2]*const FilterExpression) bool {
    const fieldsTokens = self.fieldsOrTokens(expr);
    _ = fieldsTokens;
    unreachable;
}

const KeyTokens = struct {
    key: []const u8,
    tokens: []const []const u8,
    hashes: []u64,
};

fn fieldsOrTokens(ctx: *Ctx, expr: [2]*const FilterExpression) !KeyTokens {
    // TODO: tokens must be calculated ones and cached per expression,
    // probably better to calculate it once a level above

    var m = std.StringHashMap([]const []const u8).init(ctx.alloc);
    defer m.deinit();
    var fieldKeys = std.ArrayList([]const u8).empty;
    defer fieldKeys.deinit(ctx.alloc);

    var tokensBuf: [16][]const u8 = undefined;
    const tokensArray = std.ArrayList([]const u8).initBuffer(&tokensBuf);

    for (expr) |f| {
        switch (f) {
            .andOp => {},
            .orOp => {},
            .predicate => |e| {
                if (e.predicate.op != .equal) {
                    continue;
                }
                try getTokens(&tokensArray, e.predicate.value);
                mergeTokens(ctx.alloc, &m, &fieldKeys, e.predicate.key, tokensArray.items);
            },
        }
    }

    unreachable;
}

fn getTokens(ctx: *Ctx, dst: *std.ArrayList([]const u8), predicate: []const u8) !void {
    return Tokenizer.tokenize(ctx.alloc, dst, predicate);
}

fn mergeTokens(
    alloc: Allocator,
    m: *std.StringHashMap(std.ArrayList([]const []const u8)),
    keys: *std.ArrayList([]const u8),
    key: []const u8,
    tokens: []const []const u8,
) !void {
    if (tokens.len == 0) {
        return;
    }

    var g = try m.getOrPut(key);
    if (!g.found_existing) {
        try keys.append(alloc, key);
        g.value_ptr.* = std.ArrayList([]const u8).empty;
    }
    // tokens belong to a stack buffer, so we copy
    const tokensCopy = try alloc.dupe([]const u8, tokens);
    errdefer alloc.free(tokensCopy);
    try g.value_ptr.append(alloc, tokensCopy);
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
