const std = @import("std");

const ColumnsHeaderIndex = @import("ColumnsHeaderIndex.zig");

const testing = std.testing;

test "encode columns and invariantColumns" {
    try testing.checkAllAllocationFailures(testing.allocator, testEncode, .{});
}

fn testEncode(allocator: std.mem.Allocator) !void {
    const Entry = struct {
        id: u16,
        offset: u32,
    };
    const Case = struct {
        columns: []const Entry,
        invariantColumns: []const Entry,
    };

    const cases = [_]Case{
        .{
            .columns = &.{
                .{ .id = 1, .offset = 10 },
                .{ .id = 2, .offset = 20 },
            },
            .invariantColumns = &.{
                .{ .id = 3, .offset = 30 },
                .{ .id = 4, .offset = 40 },
            },
        },
        .{
            .columns = &.{
                .{ .id = 5, .offset = 50 },
                .{ .id = 6, .offset = 60 },
                .{ .id = 7, .offset = 70 },
            },
            .invariantColumns = &.{},
        },
        .{
            .columns = &.{},
            .invariantColumns = &.{
                .{ .id = 8, .offset = 80 },
                .{ .id = 9, .offset = 90 },
            },
        },
    };

    for (cases) |case| {
        var columnIDs: [6]u16 = undefined;
        var columnOffsets: [6]u32 = undefined;
        var s = ColumnsHeaderIndex.initBufferKnown(
            &columnIDs,
            &columnOffsets,
            3,
        );

        for (case.columns) |entry| {
            s.appendColumnAssumeCapacity(.{
                .id = entry.id,
                .offset = entry.offset,
            });
        }
        for (case.invariantColumns) |entry| {
            s.appendInvariantColumnAssumeCapacity(.{
                .id = entry.id,
                .offset = entry.offset,
            });
        }

        var buf = try allocator.alloc(u8, s.encodeBound());
        defer allocator.free(buf);

        var decColumnIDs: [4]u16 = undefined;
        var decColumnOffsets: [4]u32 = undefined;
        var decoded = ColumnsHeaderIndex.initBufferUnknown(&decColumnIDs, &decColumnOffsets);
        const written = s.encode(buf);

        decoded.decode(buf[0..written]);

        try testing.expectEqual(case.columns.len, decoded.columnsIDs.items.len);
        try testing.expectEqual(case.invariantColumns.len, decoded.invariantColumnsIDs.items.len);

        for (case.columns, 0..) |entry, i| {
            try testing.expectEqual(entry.id, decoded.columnID(i));
            try testing.expectEqual(entry.offset, decoded.columnOffset(i));
        }
        for (case.invariantColumns, 0..) |entry, i| {
            try testing.expectEqual(entry.id, decoded.invariantColumnID(i));
            try testing.expectEqual(entry.offset, decoded.invariantColumnOffset(i));
        }
    }
}
