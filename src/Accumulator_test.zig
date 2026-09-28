const std = @import("std");

const Store = @import("Store.zig").Store;
const Field = @import("store/lines.zig").Field;

const Accumulator = @import("Accumulator.zig");
const testing = std.testing;

test "Accumulator.reinit owns stream tags after caller reuses tag storage" {
    const alloc = testing.allocator;
    var store: Store = undefined;
    var accumulator = try Accumulator.init(alloc, &store);
    defer accumulator.deinit(alloc);

    var tags = try std.ArrayList(Field).initCapacity(testing.allocator, 2);
    defer tags.deinit(testing.allocator);
    tags.appendAssumeCapacity(.{ .key = "app", .value = "api" });
    tags.appendAssumeCapacity(.{ .key = "env", .value = "prod" });

    try accumulator.reinit(testing.allocator, tags.items, 0);

    tags.items[0] = .{ .key = "id", .value = "line-1" };
    tags.items[1] = .{ .key = "", .value = "message" };

    const expected = [_]Field{
        .{ .key = "app", .value = "api" },
        .{ .key = "env", .value = "prod" },
    };
    try testing.expectEqualDeep(expected[0..], accumulator.currentTags);
}

test "Accumulator buffers multiple streams and flushes them as separate checkpoints" {
    const alloc = testing.allocator;
    var store: Store = undefined;
    var accumulator = try Accumulator.init(alloc, &store);
    defer accumulator.deinit(alloc);

    var tagsA = [_]Field{.{ .key = "app", .value = "a" }};
    var tagsB = [_]Field{.{ .key = "app", .value = "b" }};

    var lineA1 = [_]Field{.{ .key = "", .value = "line-a1" }};
    var lineA2 = [_]Field{.{ .key = "", .value = "line-a2" }};
    var lineB1 = [_]Field{.{ .key = "", .value = "line-b1" }};

    try accumulator.reinit(alloc, tagsA[0..], 0);
    try accumulator.tryAppendLine(testing.io, alloc, 1, lineA1[0..]);
    try accumulator.tryAppendLine(testing.io, alloc, 2, lineA2[0..]);

    try accumulator.reinit(alloc, tagsB[0..], 0);
    try accumulator.tryAppendLine(testing.io, alloc, 3, lineB1[0..]);

    try testing.expectEqual(2, accumulator.checkpointsLen);
    try testing.expectEqual(2, accumulator.checkpoints[0].i);
    try testing.expectEqual(3, accumulator.checkpoints[1].i);
}
