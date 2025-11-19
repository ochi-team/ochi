const std = @import("std");

const Line = @import("../lines.zig").Line;
const Field = @import("../lines.zig").Field;

const MemPart = @import("inmempart.zig").MemPart;
const Error = @import("inmempart.zig").Error;

const BlockHeader = @import("block_header.zig").BlockHeader;
const encode = @import("encode.zig");

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

    // Validate timestamps
    {
        const decodedTimestamps = try encode.decodeTimestamps(allocator, timestampsContent);
        defer allocator.free(decodedTimestamps);

        try std.testing.expectEqualDeep(&[_]u64{ 1, 2 }, decodedTimestamps);
    }

    // Validate block header
    {
        const blockHeader = try BlockHeader.decode(indexContent);

        try std.testing.expectEqualStrings("1234", blockHeader.sid.tenantID);
        try std.testing.expectEqual(1, blockHeader.sid.id);
        try std.testing.expectEqual(188, blockHeader.size);
        try std.testing.expectEqual(2, blockHeader.len);

        try std.testing.expectEqual(0, blockHeader.timestampsHeader.offset);
        try std.testing.expectEqual(8, blockHeader.timestampsHeader.size);
        try std.testing.expectEqual(1, blockHeader.timestampsHeader.min);
        try std.testing.expectEqual(2, blockHeader.timestampsHeader.max);
    }
}

test "addLinesErrorOnEmpty" {
    var lines = [_]*const Line{};
    const memPart = try MemPart.init(std.testing.allocator);
    defer memPart.deinit(std.testing.allocator);
    const err = memPart.addLines(std.testing.allocator, lines[0..]);
    try std.testing.expectError(Error.EmptyLines, err);
}
