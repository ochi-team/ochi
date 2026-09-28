const std = @import("std");

const Column = @import("Column.zig");
const ColumnsHeader = @import("ColumnsHeader.zig");
const TimestampsEncoder = @import("TimestampsEncoder.zig");
const CompressionPool = @import("../compression/CompressionPool.zig");
const DecompressionPool = @import("../compression/DecompressionPool.zig");

const BlockData = @import("BlockData.zig");

const Line = @import("../lines.zig").Line;
const Field = @import("../lines.zig").Field;
const MemTable = @import("MemTable.zig");
const BlockReader = @import("BlockReader.zig");
const Table = @import("../data/Table.zig");

const testing = std.testing;

test "BlockData initEmpty and deinit without header" {
    var bd = BlockData.initEmpty();
    try testing.expectEqual(@as(?*ColumnsHeader, null), bd.columnsHeader);
    try testing.expectEqual(@as(?[]Column, null), bd.invariantColumns);

    // Should not crash when deinit is called with no decoded data.
    bd.deinit(testing.allocator);
}

const SampleLines = struct {
    fields1: [2]Field,
    fields2: [2]Field,
    fields3: [2]Field,
    lines: [3]Line,
};

fn populateSampleLines(sample: *SampleLines) void {
    sample.fields1 = .{
        .{ .key = "level", .value = "info" },
        .{ .key = "app", .value = "seq" },
    };
    sample.fields2 = .{
        .{ .key = "level", .value = "warn" },
        .{ .key = "app", .value = "seq" },
    };
    sample.fields3 = .{
        .{ .key = "level", .value = "warn" },
        .{ .key = "app", .value = "seq" },
    };
    sample.lines = .{
        .{
            .timestampNs = 1,
            .fields = sample.fields1[0..],
        },
        .{
            .timestampNs = 2,
            .fields = sample.fields2[0..],
        },
        .{
            .timestampNs = 3,
            .fields = sample.fields3[0..],
        },
    };
}

test "BlockData readFrom populates columnsData and invariantColumns" {
    const allocator = testing.allocator;
    const io = testing.io;
    const timestampsEncoders = try TimestampsEncoder.TimestampsEncoderPool.init(allocator, 1);
    defer timestampsEncoders.deinit(allocator);

    var sample: SampleLines = .{
        .fields1 = undefined,
        .fields2 = undefined,
        .fields3 = undefined,
        .lines = undefined,
    };
    populateSampleLines(&sample);

    var lines = [3]Line{
        sample.lines[0],
        sample.lines[1],
        sample.lines[2],
    };

    const compressionPool = try CompressionPool.init(allocator, 1);
    defer compressionPool.deinit(allocator);
    const decompressionPool = try DecompressionPool.init(allocator, 1);
    defer decompressionPool.deinit(allocator);
    const memTable = try MemTable.init(allocator);
    const table = try Table.fromMem(io, allocator, memTable, decompressionPool);
    defer table.close(io);
    try memTable.addLinesForSid(
        io,
        allocator,
        timestampsEncoders,
        compressionPool,
        .{ .id = 1, .tenantID = 1111 },
        lines[0..],
    );

    const blockReader = try BlockReader.initFromMemTable(io, allocator, table, decompressionPool);
    defer blockReader.deinit(allocator);

    // Read first block, which should populate BlockData.
    try testing.expect(try blockReader.nextBlock(io, allocator));

    const bd = &blockReader.blockData;
    try testing.expect(bd.columnsHeader != null);
    const ch = bd.columnsHeader.?;

    // BlockData must mirror the number of column headers.
    try testing.expectEqual(ch.headers.len, bd.columnsData.items.len);

    // When there are any column headers, each ColumnData should correspond to its ColumnHeader.
    for (ch.headers, bd.columnsData.items) |*header, col| {
        try testing.expectEqualStrings(header.key, col.key);
        try testing.expectEqual(header.type, col.type);
        try testing.expectEqual(header.size, col.bloomValues.len);
        try testing.expectEqual(&header.dict, col.dict);
    }

    // Second call to nextBlock exercises BlockData reuse path (columnsHeader deinit + re-decode).
    _ = try blockReader.nextBlock(io, allocator);
}
