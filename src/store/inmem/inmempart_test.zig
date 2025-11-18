const std = @import("std");

const Line = @import("../lines.zig").Line;
const Field = @import("../lines.zig").Field;

const MemPart = @import("inmempart.zig").MemPart;
const Error = @import("inmempart.zig").Error;

test "addLines" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, testAddLines, .{});
}

fn testAddLines(allocator: std.mem.Allocator) !void {
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

    const memPart = try MemPart.init(allocator);
    defer memPart.deinit(allocator);
    try memPart.addLines(allocator, lines[0..]);
    const timestampsContent = memPart.streamWriter.timestampsBuffer.items;
    const indexContent = memPart.streamWriter.indexBuffer.items;
    try std.testing.expectEqualStrings("{ 1, 2 }", timestampsContent);
    // tenant id
    try std.testing.expectEqualStrings("1234", indexContent[0..4]);
    try std.testing.expectEqualStrings(&std.mem.zeroes([12]u8), indexContent[4..16]);
    // stream id
    try std.testing.expectEqual(1, indexContent[16]);
    try std.testing.expectEqualStrings(&std.mem.zeroes([15]u8), indexContent[17..32]);
    // block header size
    try std.testing.expectEqual(188, indexContent[32]);
    try std.testing.expectEqualStrings(&std.mem.zeroes([7]u8), indexContent[33..40]);
    // header len
    try std.testing.expectEqual(2, indexContent[40]);
    try std.testing.expectEqualStrings(&std.mem.zeroes([3]u8), indexContent[41..44]);
    // offset 0
    try std.testing.expectEqualStrings(&std.mem.zeroes([8]u8), indexContent[44..52]);
    // size is 8
    try std.testing.expectEqual(8, indexContent[52]);
    try std.testing.expectEqualStrings(&std.mem.zeroes([7]u8), indexContent[53..60]);
    // min timestamp
    try std.testing.expectEqual(1, indexContent[60]);
    try std.testing.expectEqualStrings(&std.mem.zeroes([7]u8), indexContent[61..68]);
    // max timestamp
    try std.testing.expectEqual(2, indexContent[68]);
    try std.testing.expectEqualStrings(&std.mem.zeroes([7]u8), indexContent[69..76]);
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
