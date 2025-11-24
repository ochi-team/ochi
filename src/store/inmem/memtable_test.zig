const std = @import("std");

const Line = @import("../lines.zig").Line;
const Field = @import("../lines.zig").Field;

const MemTable = @import("memtable.zig").MemTable;
const Error = @import("memtable.zig").Error;

const BlockHeader = @import("block_header.zig").BlockHeader;
const IndexBlockHeader = @import("index_block_header.zig").IndexBlockHeader;
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

    const memTable = try MemTable.init(allocator);
    defer memTable.deinit(allocator);
    try memTable.addLines(allocator, lines[0..]);

    const timestampsContent = memTable.streamWriter.timestampsBuffer.items;
    const indexContent = memTable.streamWriter.indexBuffer.items;

    // Validate timestamps
    {
        var decodedTimestamps = try encode.decodeTimestamps(allocator, timestampsContent);
        defer decodedTimestamps.deinit(allocator);

        try std.testing.expectEqualDeep(&[_]u64{ 1, 2 }, decodedTimestamps.items);
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

    // validate meta index
    {
        const metaIndexContent = memTable.streamWriter.metaIndexBuf.items;
        try std.testing.expect(metaIndexContent.len > 0);

        const decodedIndexBlockHeader = try IndexBlockHeader.decode(metaIndexContent);

        try std.testing.expectEqualStrings("1234", decodedIndexBlockHeader.sid.?.tenantID);
        try std.testing.expectEqual(1, decodedIndexBlockHeader.sid.?.id);
        try std.testing.expectEqual(1, decodedIndexBlockHeader.minTs);
        try std.testing.expectEqual(2, decodedIndexBlockHeader.maxTs);
        try std.testing.expectEqual(0, decodedIndexBlockHeader.offset);
        try std.testing.expectEqual(@as(u64, @intCast(indexContent.len)), decodedIndexBlockHeader.size);
    }
}

test "addLinesErrorOnEmpty" {
    var lines = [_]*const Line{};
    const memTable = try MemTable.init(std.testing.allocator);
    defer memTable.deinit(std.testing.allocator);
    const err = memTable.addLines(std.testing.allocator, lines[0..]);
    try std.testing.expectError(Error.EmptyLines, err);
}
