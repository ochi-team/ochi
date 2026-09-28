const std = @import("std");

const SID = @import("../lines.zig").SID;
const Line = @import("../lines.zig").Line;
const Field = @import("../lines.zig").Field;

const TableWriter = @import("../data/TableWriter.zig");
const BlockWriter = @import("../data/BlockWriter.zig");
const MemTable = @import("../data/MemTable.zig");
const Table = @import("Table.zig");
const Block = @import("../data/Block.zig");
const BlockReader = @import("../data/BlockReader.zig");
const Unpacker = @import("../data/Unpacker.zig").Unpacker;
const ValuesDecoder = @import("../data/ValuesDecoder.zig");
const TimestampsEncoder = @import("../data/TimestampsEncoder.zig");
const CompressionPool = @import("../compression/CompressionPool.zig");
const DecompressionPool = @import("../compression/DecompressionPool.zig");

const Consts = @import("../../Consts.zig");

const maxBlockSize = Consts.maxBlockSize;
const StreamMerger = @import("merge.zig").StreamMerger;
const mergeLines = @import("merge.zig").mergeLines;
const mergeBlocks = @import("merge.zig").mergeBlocks;
const testing = std.testing;

test "mergeLines" {
    const alloc = testing.allocator;
    const Case = struct {
        left: []const Line,
        right: []const Line,
        expected: []const Line,
    };

    var la = [_]Field{.{ .key = "a", .value = "left" }};
    var ra = [_]Field{.{ .key = "b", .value = "right" }};
    var la1 = [_]Field{.{ .key = "a", .value = "left-1" }};
    var la2 = [_]Field{.{ .key = "a", .value = "left-2" }};
    var ra1 = [_]Field{.{ .key = "b", .value = "right-1" }};
    var ra2 = [_]Field{.{ .key = "b", .value = "right-2" }};
    var lb1 = [_]Field{.{ .key = "a", .value = "left-1" }};
    var lb2 = [_]Field{.{ .key = "a", .value = "left-2" }};
    var rb1 = [_]Field{.{ .key = "b", .value = "right-1" }};
    var rb2 = [_]Field{.{ .key = "b", .value = "right-2" }};

    const cases = [_]Case{
        .{
            .left = &.{},
            .right = &.{},
            .expected = &.{},
        },
        .{
            .left = &[_]Line{
                .{ .timestampNs = 123, .fields = &la },
            },
            .right = &.{},
            .expected = &[_]Line{
                .{ .timestampNs = 123, .fields = &la },
            },
        },
        .{
            .left = &[_]Line{
                .{ .timestampNs = 123, .fields = &la },
            },
            .right = &[_]Line{
                .{ .timestampNs = 456, .fields = &ra },
            },
            .expected = &[_]Line{
                .{ .timestampNs = 123, .fields = &la },
                .{ .timestampNs = 456, .fields = &ra },
            },
        },
        .{
            .left = &[_]Line{
                .{ .timestampNs = 123, .fields = &la1 },
                .{ .timestampNs = 456, .fields = &la2 },
            },
            .right = &[_]Line{
                .{ .timestampNs = 123, .fields = &ra1 },
                .{ .timestampNs = 456, .fields = &ra2 },
            },
            .expected = &[_]Line{
                .{ .timestampNs = 123, .fields = &la1 },
                .{ .timestampNs = 123, .fields = &ra1 },
                .{ .timestampNs = 456, .fields = &la2 },
                .{ .timestampNs = 456, .fields = &ra2 },
            },
        },
        .{
            .left = &[_]Line{
                .{ .timestampNs = 12, .fields = &lb1 },
                .{ .timestampNs = 123456, .fields = &lb2 },
            },
            .right = &[_]Line{
                .{ .timestampNs = 1, .fields = &rb1 },
                .{ .timestampNs = 456, .fields = &rb2 },
            },
            .expected = &[_]Line{
                .{ .timestampNs = 1, .fields = &rb1 },
                .{ .timestampNs = 12, .fields = &lb1 },
                .{ .timestampNs = 456, .fields = &rb2 },
                .{ .timestampNs = 123456, .fields = &lb2 },
            },
        },
    };

    for (cases) |case| {
        var merged = try std.ArrayList(Line).initCapacity(alloc, case.left.len + case.right.len);
        defer merged.deinit(alloc);

        mergeLines(&merged, case.left, case.right);
        try testing.expectEqualDeep(case.expected, merged.items);
    }
}

test "StreamMerger.decodeLines frees unpacker garbage buffers" {
    const alloc = testing.allocator;
    const io = testing.io;
    const timestampsEncoders = try TimestampsEncoder.TimestampsEncoderPool.init(alloc, 1);
    defer timestampsEncoders.deinit(alloc);
    const compressionPool = try CompressionPool.init(alloc, 1);
    defer compressionPool.deinit(alloc);
    const decompressionPool = try DecompressionPool.init(alloc, 1);
    defer decompressionPool.deinit(alloc);
    const sid = SID{ .tenantID = 1, .id = 42 };

    const memTable = try MemTable.init(alloc);
    const table = try Table.fromMem(io, alloc, memTable, decompressionPool);
    defer table.close(io);

    // one column with 2 fields create col values buffer to reproduce a leak
    var fields1 = [_]Field{.{ .key = "key", .value = "value-1" }};
    var fields2 = [_]Field{.{ .key = "key", .value = "value-2" }};
    var lines = [_]Line{
        .{ .timestampNs = 1, .fields = fields1[0..] },
        .{ .timestampNs = 2, .fields = fields2[0..] },
    };
    try memTable.addLinesForSid(io, alloc, timestampsEncoders, compressionPool, sid, lines[0..]);

    const reader = try BlockReader.initFromMemTable(io, alloc, table, decompressionPool);
    defer reader.deinit(alloc);

    var readers = try std.ArrayList(*BlockReader).initCapacity(alloc, 1);
    defer readers.deinit(alloc);
    try readers.append(alloc, reader);

    var merger = try StreamMerger.init(io, alloc, timestampsEncoders, decompressionPool, &readers);
    defer merger.deinit();

    try merger.decodeLines(io, &reader.blockData);
}

test "mergeData keeps merged memtable buffers alive after source memtables deinit" {
    const alloc = testing.allocator;
    const io = testing.io;
    const timestampsEncoders = try TimestampsEncoder.TimestampsEncoderPool.init(alloc, 1);
    defer timestampsEncoders.deinit(alloc);
    const compressionPool = try CompressionPool.init(alloc, 1);
    defer compressionPool.deinit(alloc);
    const decompressionPool = try DecompressionPool.init(alloc, 1);
    defer decompressionPool.deinit(alloc);
    const sid = SID{ .tenantID = 1, .id = 42 };

    const expected = [_]struct {
        timestampNs: u64,
        value: []const u8,
    }{
        .{ .timestampNs = 1, .value = "left-1" },
        .{ .timestampNs = 2, .value = "right-1" },
        .{ .timestampNs = 3, .value = "left-2" },
        .{ .timestampNs = 4, .value = "right-2" },
    };

    const fieldKey = "key";

    const mergedMemTable = blk: {
        const leftMemTable = try MemTable.init(alloc);
        const leftTable = try Table.fromMem(io, alloc, leftMemTable, decompressionPool);
        defer leftTable.close(io);
        const rightMemTable = try MemTable.init(alloc);
        const rightTable = try Table.fromMem(io, alloc, rightMemTable, decompressionPool);
        defer rightTable.close(io);

        var leftFields1 = [_]Field{.{ .key = fieldKey, .value = "left-1" }};
        var leftFields2 = [_]Field{.{ .key = fieldKey, .value = "left-2" }};
        var rightFields1 = [_]Field{.{ .key = fieldKey, .value = "right-1" }};
        var rightFields2 = [_]Field{.{ .key = fieldKey, .value = "right-2" }};

        var leftLines = [_]Line{
            .{ .timestampNs = 1, .fields = leftFields1[0..] },
            .{ .timestampNs = 3, .fields = leftFields2[0..] },
        };
        var rightLines = [_]Line{
            .{ .timestampNs = 2, .fields = rightFields1[0..] },
            .{ .timestampNs = 4, .fields = rightFields2[0..] },
        };

        try leftMemTable.addLinesForSid(io, alloc, timestampsEncoders, compressionPool, sid, leftLines[0..]);
        try rightMemTable.addLinesForSid(io, alloc, timestampsEncoders, compressionPool, sid, rightLines[0..]);

        var readers = try std.ArrayList(*BlockReader).initCapacity(alloc, 2);
        defer readers.deinit(alloc);

        try readers.append(alloc, try BlockReader.initFromMemTable(io, alloc, leftTable, decompressionPool));
        try readers.append(alloc, try BlockReader.initFromMemTable(io, alloc, rightTable, decompressionPool));

        const dstMemTable = try MemTable.init(alloc);
        errdefer dstMemTable.deinit(alloc);
        const streamWriter = try TableWriter.initMem(alloc, dstMemTable, timestampsEncoders, compressionPool);
        defer streamWriter.deinit(alloc);
        dstMemTable.tableHeader = try mergeBlocks(
            io,
            alloc,
            timestampsEncoders,
            decompressionPool,
            streamWriter,
            &readers,
            null,
        );

        try testing.expect(dstMemTable.indexBuf.items.len > 0);
        try testing.expect(dstMemTable.metaIndexBuf.items.len > 0);
        try testing.expect(dstMemTable.columnsHeaderIndexBuf.items.len > 0);
        try testing.expect(dstMemTable.columnsHeaderBuf.items.len > 0);
        try testing.expect(dstMemTable.timestampsBuf.items.len > 0);

        break :blk dstMemTable;
    };
    const mergedTable = try Table.fromMem(io, alloc, mergedMemTable, decompressionPool);
    defer mergedTable.close(io);

    var mergedReader = try BlockReader.initFromMemTable(io, alloc, mergedTable, decompressionPool);
    defer mergedReader.deinit(alloc);

    var unpacker = Unpacker(false).init(decompressionPool);
    defer unpacker.deinit(alloc);
    var decoder: ValuesDecoder = .{};
    defer decoder.deinit(alloc);

    var expectedI: usize = 0;
    while (try mergedReader.nextBlock(io, alloc)) {
        var block = try Block.initFromData(io, alloc, timestampsEncoders, &mergedReader.blockData, false, &unpacker, &decoder);
        defer block.deinit(alloc);

        var lines = std.ArrayList(Line).empty;
        defer {
            for (lines.items) |line| {
                alloc.free(line.fields);
            }
            lines.deinit(alloc);
        }
        try block.gatherLines(alloc, &lines);

        for (lines.items) |line| {
            try testing.expect(expectedI < expected.len);
            try testing.expectEqual(expected[expectedI].timestampNs, line.timestampNs);
            try testing.expectEqual(@as(usize, 1), line.fields.len);
            try testing.expectEqualStrings(fieldKey, line.fields[0].key);
            try testing.expectEqualStrings(expected[expectedI].value, line.fields[0].value);
            expectedI += 1;
        }
    }

    try testing.expectEqual(expected.len, expectedI);
}

test "mergeData flushes maxLines for one stream" {
    const alloc = testing.allocator;
    const io = testing.io;
    const timestampsEncoders = try TimestampsEncoder.TimestampsEncoderPool.init(alloc, 1);
    defer timestampsEncoders.deinit(alloc);
    const compressionPool = try CompressionPool.init(alloc, 1);
    defer compressionPool.deinit(alloc);
    const decompressionPool = try DecompressionPool.init(alloc, 1);
    defer decompressionPool.deinit(alloc);
    const sid = SID{ .tenantID = 1, .id = 42 };

    const srcMemTable = try MemTable.init(alloc);
    const srcTable = try Table.fromMem(io, alloc, srcMemTable, decompressionPool);
    defer srcTable.close(io);

    var field = [_]Field{.{ .key = "level", .value = "info" }};
    var lines: [Block.maxLines]Line = undefined;
    for (0..Block.maxLines) |i| {
        lines[i] = .{
            .timestampNs = @intCast(i),
            .fields = &field,
        };
    }

    try srcMemTable.addLinesForSid(io, alloc, timestampsEncoders, compressionPool, sid, &lines);

    var readers = try std.ArrayList(*BlockReader).initCapacity(alloc, 1);
    defer readers.deinit(alloc);
    try readers.append(alloc, try BlockReader.initFromMemTable(io, alloc, srcTable, decompressionPool));

    const dstMemTable = try MemTable.init(alloc);
    const dstTable = try Table.fromMem(io, alloc, dstMemTable, decompressionPool);
    defer dstTable.close(io);

    const streamWriter = try TableWriter.initMem(
        alloc,
        dstMemTable,
        timestampsEncoders,
        compressionPool,
    );
    defer streamWriter.deinit(alloc);
    dstMemTable.tableHeader = try mergeBlocks(
        io,
        alloc,
        timestampsEncoders,
        decompressionPool,
        streamWriter,
        &readers,
        null,
    );

    var mergedReader = try BlockReader.initFromMemTable(io, alloc, dstTable, decompressionPool);
    defer mergedReader.deinit(alloc);

    var actualRows: usize = 0;
    while (try mergedReader.nextBlock(io, alloc)) {
        actualRows += mergedReader.blockData.len;
    }

    try testing.expectEqual(Block.maxLines, actualRows);
}

test "writeBlock flushes without merging when the incoming block is already full-sized" {
    const alloc = testing.allocator;
    const io = testing.io;
    const timestampsEncoders = try TimestampsEncoder.TimestampsEncoderPool.init(alloc, 1);
    defer timestampsEncoders.deinit(alloc);
    const compressionPool = try CompressionPool.init(alloc, 1);
    defer compressionPool.deinit(alloc);
    const decompressionPool = try DecompressionPool.init(alloc, 1);
    defer decompressionPool.deinit(alloc);
    const sid = SID{ .tenantID = 1, .id = 7 };

    const memTable1 = try MemTable.init(alloc);
    const table1 = try Table.fromMem(io, alloc, memTable1, decompressionPool);
    defer table1.close(io);
    var fields1 = [_]Field{.{ .key = "k", .value = "v1" }};
    var lines1 = [_]Line{.{ .timestampNs = 1, .fields = fields1[0..] }};
    try memTable1.addLinesForSid(io, alloc, timestampsEncoders, compressionPool, sid, lines1[0..]);
    const reader1 = try BlockReader.initFromMemTable(io, alloc, table1, decompressionPool);
    defer reader1.deinit(alloc);

    const memTable2 = try MemTable.init(alloc);
    const table2 = try Table.fromMem(io, alloc, memTable2, decompressionPool);
    defer table2.close(io);
    var fields2 = [_]Field{.{ .key = "k", .value = "v2" }};
    var lines2 = [_]Line{.{ .timestampNs = 2, .fields = fields2[0..] }};
    try memTable2.addLinesForSid(io, alloc, timestampsEncoders, compressionPool, sid, lines2[0..]);
    const reader2 = try BlockReader.initFromMemTable(io, alloc, table2, decompressionPool);
    defer reader2.deinit(alloc);

    var readers = try std.ArrayList(*BlockReader).initCapacity(alloc, 1);
    defer readers.deinit(alloc);
    try readers.append(alloc, reader1);

    var merger = try StreamMerger.init(io, alloc, timestampsEncoders, decompressionPool, &readers);
    defer merger.deinit();

    const dstMemTable = try MemTable.init(alloc);
    defer dstMemTable.deinit(alloc);
    const streamWriter = try TableWriter.initMem(alloc, dstMemTable, timestampsEncoders, compressionPool);
    defer streamWriter.deinit(alloc);
    const blockWriter = try BlockWriter.init(alloc);
    defer blockWriter.deinit(alloc);

    try merger.writeBlock(io, alloc, blockWriter, streamWriter, &reader1.blockData);
    try testing.expect(merger.block.len > 0);
    try testing.expectEqual(0, merger.lines.items.len);

    // force having second block max size in order to test it's flushed on write
    try testing.expect(try reader2.nextBlock(io, alloc));
    reader2.blockData.uncompressedSizeBytes = maxBlockSize;

    try merger.writeBlock(io, alloc, blockWriter, streamWriter, &reader2.blockData);

    // an already-full block must be flushed+written as-is, never decoded into lines
    try testing.expectEqual(0, merger.lines.items.len);
}

test "mergeData multi tenant" {
    const alloc = testing.allocator;
    const io = testing.io;
    const timestampsEncoders = try TimestampsEncoder.TimestampsEncoderPool.init(alloc, 1);
    defer timestampsEncoders.deinit(alloc);
    const compressionPool = try CompressionPool.init(alloc, 1);
    defer compressionPool.deinit(alloc);
    const decompressionPool = try DecompressionPool.init(alloc, 1);
    defer decompressionPool.deinit(alloc);

    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    const rootPath = try tmp.dir.realPathFileAlloc(io, ".", alloc);
    defer alloc.free(rootPath);

    const tenantIDs = [_]u64{ 1, 2, 3, 4, 5, 6, 7, 8 };

    var readers = try std.ArrayList(*BlockReader).initCapacity(alloc, tenantIDs.len);
    var tables = try std.ArrayList(*Table).initCapacity(alloc, tenantIDs.len);
    defer {
        for (readers.items) |reader| {
            reader.deinit(alloc);
        }
        readers.deinit(alloc);
        for (tables.items) |table| {
            table.close(io);
        }
        tables.deinit(alloc);
    }

    for (tenantIDs, 0..) |tenantID, i| {
        const tablePath = try std.fmt.allocPrint(
            alloc,
            "{s}/table-{d}",
            .{ rootPath, i },
        );

        const memTable = try MemTable.init(alloc);
        defer memTable.deinit(alloc);

        var fields = [_]Field{
            .{ .key = "app", .value = "repro" },
            .{ .key = "level", .value = "info" },
        };
        var lines = [_]Line{.{
            .timestampNs = @intCast(i + 1),
            .fields = fields[0..],
        }};

        try memTable.addLinesForSid(
            io,
            alloc,
            timestampsEncoders,
            compressionPool,
            .{ .tenantID = tenantID, .id = 1 },
            lines[0..],
        );
        try memTable.storeToDisk(io, tablePath);

        const table = try Table.open(io, alloc, tablePath, decompressionPool);
        try tables.append(alloc, table);
        const reader = try BlockReader.initFromDiskTable(io, alloc, table, decompressionPool);
        try readers.append(alloc, reader);
    }

    const dstMemTable = try MemTable.init(alloc);
    defer dstMemTable.deinit(alloc);

    const streamWriter = try TableWriter.initMem(
        alloc,
        dstMemTable,
        timestampsEncoders,
        compressionPool,
    );
    defer streamWriter.deinit(alloc);
    dstMemTable.tableHeader = try mergeBlocks(
        io,
        alloc,
        timestampsEncoders,
        decompressionPool,
        streamWriter,
        &readers,
        null,
    );

    try testing.expectEqual(@as(u32, tenantIDs.len), dstMemTable.tableHeader.len);
}
