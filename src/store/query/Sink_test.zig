const std = @import("std");
const testing = std.testing;
const QueryResult = @import("QueryResult.zig");
const Line = @import("../../store/lines.zig").Line;
const Field = @import("../../store/lines.zig").Field;
const Sink = @import("Sink.zig");

test "Sink copies reused QueryResult and skips empty regular values" {
    const a = testing.allocator;
    var sink: Sink = .{};
    defer sink.deinit(a);
    const key = "msg";
    const value = "one";

    var result: QueryResult = .{ .hits = try .initEmpty(a, 0), .len = 2 };
    defer result.deinit(a);

    try result.timestamps.appendSlice(a, &.{ 1, 2 });
    try result.columns.append(a, .{ .key = key, .values = try a.dupe([]const u8, &.{ value, "" }) });
    try result.invariants.append(a, .{ .key = "host", .values = try a.dupe([]const u8, &.{"local"}) });

    try sink.write(a, &result);

    var f1 = [_]Field{ .{ .key = "host", .value = "local" }, .{ .key = "msg", .value = "one" } };
    var f2 = [_]Field{.{ .key = "host", .value = "local" }};
    const expected = [_]Line{
        .{ .timestampNs = 1, .fields = .view(&f1) },
        .{ .timestampNs = 2, .fields = .view(&f2) },
    };
    for (expected, sink.lines.items) |want, got| {
        try testing.expectEqualDeep(.{ want.timestampNs, want.fields.items }, .{ got.timestampNs, got.fields.items });
    }
}
