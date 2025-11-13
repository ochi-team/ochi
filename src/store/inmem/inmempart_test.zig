const std = @import("std");

const Line = @import("../lines.zig").Line;
const Field = @import("../lines.zig").Field;

const MemPart = @import("inmempart.zig").MemPart;
const Error = @import("inmempart.zig").Error;

test "addLines" {
    var fields1 = [_]Field{
        .{ .key = "level", .value = "info" },
        .{ .key = "app", .value = "seq" },
    };
    var fields2 = [_]Field{
        .{ .key = "level", .value = "warn" },
        .{ .key = "app", .value = "seq" },
    };
    var lines = [_]*const Line{
        &.{
            .timestampNs = 1,
            .sid = .{ .id = 1, .tenantID = "1234" },
            .fields = fields1[0..],
            .encodedTags = undefined,
        },
        &.{
            .timestampNs = 2,
            .sid = .{ .id = 1, .tenantID = "1234" },
            .fields = fields2[0..],
            .encodedTags = undefined,
        },
    };

    const memPart = try MemPart.init(std.testing.allocator);
    defer memPart.deinit(std.testing.allocator);
    try memPart.addLines(std.testing.allocator, lines[0..]);
    const content = memPart.streamWriter.timestampsBuffer.content();
    try std.testing.expectEqualStrings("{ 1, 2 }", content);
    // test empty fields
    // test empty keys
    // test no celles
    // test only cells
    // test reverse order (first )
}

test "addLinesErrorOnEmpty" {
    var lines = [_]*const Line{};
    const memPart = try MemPart.init(std.testing.allocator);
    defer memPart.deinit(std.testing.allocator);
    const err = memPart.addLines(std.testing.allocator, lines[0..]);
    try std.testing.expectError(Error.EmptyLines, err);
}
