const std = @import("std");

const Store = @import("Store.zig").Store;
const Field = @import("store/lines.zig").Field;
const copyFields = @import("store/lines.zig").copyFields;
const freeFields = @import("store/lines.zig").freeFields;

const Accumulator = @import("Accumulator.zig");
const SharedBufferPool = @import("SharedBufferPool.zig");
const testing = std.testing;

test "Accumulator.reinit owns stream tags after caller reuses tag storage" {
    const alloc = testing.allocator;
    const buffers = try SharedBufferPool.init(testing.io, alloc, 2);
    defer buffers.deinit();
    var store: Store = undefined;
    store.ingestBuffers = buffers;
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
    try testing.expectEqualDeep(expected[0..], accumulator.currentTags.items);
}

test "Accumulator buffers multiple streams and flushes them as separate checkpoints" {
    const alloc = testing.allocator;
    const buffers = try SharedBufferPool.init(testing.io, alloc, 2);
    defer buffers.deinit();
    var store: Store = undefined;
    store.ingestBuffers = buffers;
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

test "Accumulator reuses equal field bytes from the preceding line" {
    const Reuse = struct { key: bool, value: bool };
    const check = struct {
        fn check(first: []const Field, next: []const Field, expectedReuse: []const Reuse) !void {
            const alloc = testing.allocator;
            const buffers = try SharedBufferPool.init(testing.io, alloc, 2);
            defer buffers.deinit();

            var store: Store = undefined;
            store.ingestBuffers = buffers;
            var accumulator = try Accumulator.init(alloc, &store);
            defer accumulator.deinit(alloc);

            var tags = [_]Field{.{ .key = "app", .value = "dedup" }};
            try accumulator.reinit(alloc, &tags, 1);

            const firstInput = try copyFields(alloc, first);
            defer freeFields(alloc, firstInput);
            try accumulator.tryAppendLine(testing.io, alloc, 1, firstInput.items);

            const nextInput = try copyFields(alloc, next);
            defer freeFields(alloc, nextInput);
            try accumulator.tryAppendLine(testing.io, alloc, 2, nextInput.items);

            // Neither newly copied nor reused bytes may depend on the request storage.
            @memset(firstInput.buf, 0);
            @memset(nextInput.buf, 0);
            const firstFields = accumulator.lines.items[0].fields.items;
            const nextFields = accumulator.lines.items[1].fields.items;
            try testing.expectEqualDeep(first, firstFields);
            try testing.expectEqualDeep(next, nextFields);

            const reused = try alloc.alloc(Reuse, nextFields.len);
            defer alloc.free(reused);
            for (nextFields, 0..) |field, i| {
                reused[i] = .{
                    .key = i < firstFields.len and field.key.ptr == firstFields[i].key.ptr,
                    .value = i < firstFields.len and field.value.ptr == firstFields[i].value.ptr,
                };
            }
            try testing.expectEqualDeep(expectedReuse, reused);
            if (firstFields.len > 0 and nextFields.len > 0) {
                try testing.expect(firstFields.ptr != nextFields.ptr);
            }
        }
    }.check;

    const first = [_]Field{
        .{ .key = "level", .value = "info" },
        .{ .key = "message", .value = "same payload" },
    };
    try check(&first, &first, &.{ .{ .key = true, .value = true }, .{ .key = true, .value = true } });
    try check(&first, &.{.{ .key = "level", .value = "warn" }}, &.{.{ .key = true, .value = false }});
    try check(&first, &.{.{ .key = "severity", .value = "info" }}, &.{.{ .key = false, .value = true }});
    try check(&first, &.{ first[1], first[0] }, &.{ .{ .key = false, .value = false }, .{ .key = false, .value = false } });
    try check(&first, first[0..1], &.{.{ .key = true, .value = true }});
    try check(first[0..1], &first, &.{ .{ .key = true, .value = true }, .{ .key = false, .value = false } });
    try check(&first, &.{}, &.{});
    try check(&.{}, first[0..1], &.{.{ .key = false, .value = false }});
}
