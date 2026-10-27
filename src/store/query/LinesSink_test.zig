const std = @import("std");
const testing = std.testing;
const BlockResult = @import("BlockResult.zig");
const Column = @import("../data/Column.zig");
const lines = @import("../lines.zig");

test "LinesSink copies reused BlockResult and skips empty regular values" {
    const a = testing.allocator;
    var sink: lines.LinesSink = .{ .alloc = a };
    defer sink.deinit();
    var result: BlockResult = .{};
    defer result.deinit(a);
    var key = [_]u8{ 'm', 's', 'g' };
    var value = [_]u8{ 'o', 'n', 'e' };
    for (0..2) |pass| {
        result.reset(a);
        try result.timestamps.ensureUnusedCapacity(a, 2);
        try result.columns.ensureUnusedCapacity(a, 1);
        try result.invariants.ensureUnusedCapacity(a, 1);
        result.timestamps.appendSliceAssumeCapacity(&.{ pass * 2 + 1, pass * 2 + 2 });
        result.columns.appendAssumeCapacity(.{ .key = &key, .values = try a.dupe([]const u8, &.{ &value, "" }) });
        result.invariants.appendAssumeCapacity(.{ .key = "host", .values = try a.dupe([]const u8, &.{"local"}) });
        try sink.writeBlock().write(&result);
        @memcpy(&key, "log");
        @memcpy(&value, "two");
    }
    result.reset(a);
    @memset(&key, 'x');
    @memset(&value, 'x');
    const expected = [_]lines.Line{
        .{ .timestampNs = 1, .fields = .view(@constCast(&[_]lines.Field{ .{ .key = "host", .value = "local" }, .{ .key = "msg", .value = "one" } })) },
        .{ .timestampNs = 2, .fields = .view(@constCast(&[_]lines.Field{.{ .key = "host", .value = "local" }})) },
        .{ .timestampNs = 3, .fields = .view(@constCast(&[_]lines.Field{ .{ .key = "host", .value = "local" }, .{ .key = "log", .value = "two" } })) },
        .{ .timestampNs = 4, .fields = .view(@constCast(&[_]lines.Field{.{ .key = "host", .value = "local" }})) },
    };
    // Compare content, not the Fields ownership buffers.
    for (expected, sink.lines.items) |want, got| {
        try testing.expectEqualDeep(.{ want.timestampNs, want.fields.items }, .{ got.timestampNs, got.fields.items });
    }
}

test "LinesSink allocation failures roll back the current block" {
    try testing.checkAllAllocationFailures(testing.allocator, struct {
        fn run(a: std.mem.Allocator) !void {
            var sink: lines.LinesSink = .{ .alloc = a };
            defer sink.deinit();
            const timestamps = [_]u64{ 1, 2 };
            const values = [_][]const u8{ "first", "second" };
            const columns = [_]Column{.{ .key = "msg", .values = @constCast(&values) }};
            const result: BlockResult = .{
                .timestamps = .{ .items = @constCast(&timestamps), .capacity = 0 },
                .columns = .{ .items = @constCast(&columns), .capacity = 0 },
            };
            sink.writeBlock().write(&result) catch |err| {
                try testing.expectEqual(0, sink.lines.items.len);
                return err;
            };
            try testing.expectEqual(2, sink.lines.items.len);
        }
    }.run, .{});
}
