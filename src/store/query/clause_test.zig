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

test "collectOrs" {
    const f = struct {
        fn f(expected: []const *const FilterExpression, expr: [2]*const FilterExpression) !void {
            var buf: [32]*const FilterExpression = undefined;
            var ors = std.ArrayList(*const FilterExpression).initBuffer(&buf);

            clause.collectOrs(&ors, expr);

            try testing.expectEqual(expected.len, ors.items.len);
            try testing.expectEqualDeep(expected, ors.items);
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
    // xOrz unwraps to x and z
    try f(&.{ x, yOrz, y, z }, .{ x, yOrz });
    try f(&.{ xOry, z, x, y }, .{ xOry, z });
    try f(&.{ xOry, yOrz, x, y, y, z }, .{ xOry, yOrz });
    try f(&.{ x, y, z, x }, .{ xOryOrz, x });

    const a = try pred(alloc, "a", "1");
    const b = try pred(alloc, "b", "2");
    const c = try pred(alloc, "c", "3");

    const aAndb = try andOp(alloc, a, b);
    const bAndc = try andOp(alloc, a, b);

    try f(&.{ a, b }, .{ a, b });
    // doesn't unwrap b and c
    try f(&.{ a, bAndc }, .{ a, bAndc });
    try f(&.{ aAndb, c }, .{ aAndb, c });
    try f(&.{ aAndb, bAndc }, .{ aAndb, bAndc });
}
