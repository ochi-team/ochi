const std = @import("std");
const testing = std.testing;

const Query = @import("../../query/Query.zig");
const FilterExpression = Query.FilterExpression;

const clause = @import("clause.zig");

fn pred(a: std.mem.Allocator, key: []const u8, value: []const u8) !*const FilterExpression {
    const n = try a.create(FilterExpression);
    n.* = .{ .predicate = .{ .key = key, .value = value, .op = .equal } };
    return n;
}

fn orOp(a: std.mem.Allocator, l: *const FilterExpression, r: *const FilterExpression) !*const FilterExpression {
    const n = try a.create(FilterExpression);
    n.* = .{ .orOp = .{ l, r } };
    return n;
}
fn andOp(a: std.mem.Allocator, l: *const FilterExpression, r: *const FilterExpression) !*const FilterExpression {
    const n = try a.create(FilterExpression);
    n.* = .{ .andOp = .{ l, r } };
    return n;
}

fn expectUnwrapEqual(
    call: *const fn (dst: *std.ArrayList(*const FilterExpression), expr: [2]*const FilterExpression) bool,
    expected: []const *const FilterExpression,
    expr: [2]*const FilterExpression,
    expectedRes: bool,
    capacity: usize,
) !void {
    var buf: [32]*const FilterExpression = undefined;
    var ors = std.ArrayList(*const FilterExpression).initBuffer(buf[0..capacity]);

    const res = call(&ors, expr);

    try testing.expectEqual(expectedRes, res);
    try testing.expectEqual(expected.len, ors.items.len);
    for (0..expected.len) |i| {
        try testing.expectEqualDeep(expected[i], ors.items[i]);
    }
}

test "unwrapOrs" {
    const f = struct {
        fn f(expected: []const *const FilterExpression, expr: [2]*const FilterExpression) !void {
            return expectUnwrapEqual(clause.unwrapOrs, expected, expr, true, 32);
        }
    }.f;

    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const alloc = arena.allocator();

    const x = try pred(alloc, "x", "1");
    const y = try pred(alloc, "y", "2");
    const z = try pred(alloc, "z", "3");

    const yOrz = try orOp(alloc, y, z);
    const xOry = try orOp(alloc, x, y);

    const xOryOrz = try orOp(alloc, xOry, z);

    try f(&.{ x, y }, .{ x, y });
    // yOrz unwraps to y and z
    try f(&.{ x, y, z }, .{ x, yOrz });
    try f(&.{ x, y, z }, .{ xOry, z });
    try f(&.{ x, y, y, z }, .{ xOry, yOrz });
    try f(&.{ x, y, z, x }, .{ xOryOrz, x });

    const a = try pred(alloc, "a", "1");
    const b = try pred(alloc, "b", "2");
    const c = try pred(alloc, "c", "3");

    const aAndb = try andOp(alloc, a, b);
    const bAndc = try andOp(alloc, b, c);

    try f(&.{ a, b }, .{ a, b });
    // doesn't unwrap b and c
    try f(&.{ a, bAndc }, .{ a, bAndc });
    try f(&.{ aAndb, c }, .{ aAndb, c });
    try f(&.{ aAndb, bAndc }, .{ aAndb, bAndc });
}

test "unwrapAnds" {
    const f = struct {
        fn f(expected: []const *const FilterExpression, expr: [2]*const FilterExpression, expectedRes: bool) !void {
            return expectUnwrapEqual(clause.unwrapAnds, expected, expr, expectedRes, 32);
        }
    }.f;

    // to teset overflow
    const fWithCapacity = struct {
        fn fWithCapacity(expected: []const *const FilterExpression, expr: [2]*const FilterExpression, expectedRes: bool, capacity: usize) !void {
            return expectUnwrapEqual(clause.unwrapAnds, expected, expr, expectedRes, capacity);
        }
    }.fWithCapacity;

    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const alloc = arena.allocator();

    const x = try pred(alloc, "x", "1");
    const y = try pred(alloc, "y", "2");
    const z = try pred(alloc, "z", "3");

    const yAndz = try andOp(alloc, y, z);
    const xAndy = try andOp(alloc, x, y);
    const xAndyAndz = try andOp(alloc, xAndy, z);
    const xAnd_yAndz = try andOp(alloc, x, yAndz);

    try f(&.{ x, y }, .{ x, y }, true);
    try f(&.{ x, y, z }, .{ x, yAndz }, true);
    try f(&.{ x, y, z }, .{ xAndy, z }, true);
    try f(&.{ x, y, y, z }, .{ xAndy, yAndz }, true);
    try f(&.{ x, y, z, x }, .{ xAndyAndz, x }, true);
    try f(&.{ x, y, z, z }, .{ xAnd_yAndz, z }, true);

    const a = try pred(alloc, "a", "1");
    const b = try pred(alloc, "b", "2");
    const c = try pred(alloc, "c", "3");

    const aOrb = try orOp(alloc, a, b);
    const bOrc = try orOp(alloc, b, c);

    // ors are kept as is
    try f(&.{ a, b }, .{ a, b }, true);
    try f(&.{ a, bOrc }, .{ a, bOrc }, true);
    try f(&.{ aOrb, c }, .{ aOrb, c }, true);
    try f(&.{ aOrb, bOrc }, .{ aOrb, bOrc }, true);
    // ands nested under ors are not touched either
    const orOfAnds = try orOp(alloc, xAndy, z);
    try f(&.{ orOfAnds, x }, .{ orOfAnds, x }, true);

    // buffer overflow: stops unwrapping and reports failure
    try fWithCapacity(&.{ x, yAndz }, .{ x, yAndz }, false, 2);
    try fWithCapacity(&.{ x, y, z }, .{ x, yAndz }, true, 3);
    try fWithCapacity(&.{ xAndy, z }, .{ xAndy, z }, false, 2);
}
