const std = @import("std");
const Allocator = std.mem.Allocator;
const Io = std.Io;
const Dir = Io.Dir;

const filenames = @import("../../filenames.zig");
const fs = @import("../../fs.zig");
const MemTable = @import("../data/MemTable.zig");
const IndexBlockHeader = @import("../data/IndexBlockHeader.zig");
const TimestampsEncoder = @import("../data/TimestampsEncoder.zig");
const CompressionPool = @import("../compression/CompressionPool.zig");
const DecompressionPool = @import("../compression/DecompressionPool.zig");

const Line = @import("../lines.zig").Line;
const SID = @import("../lines.zig").SID;
const freeFields = @import("../lines.zig").freeFields;
const deinitLinesFull = @import("../lines.zig").deinitLinesFull;
const Query = @import("../../query/Query.zig");

const testing = std.testing;
const stesting = @import("../../stds/testing.zig");

const ColumnDict = @import("ColumnDict.zig");

const Table = @import("Table.zig");

test "release keeps table unless toRemove is set, then removes table dir" {
    const alloc = testing.allocator;
    const io = testing.io;

    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    const rootPath = try tmp.dir.realPathFileAlloc(io, ".", alloc);
    defer alloc.free(rootPath);
    var tablePathBuf: [std.fs.max_path_bytes]u8 = undefined;
    var tablePathWriter = std.Io.Writer.fixed(&tablePathBuf);
    try std.fs.path.fmtJoin(&.{ rootPath, "table-1" }).format(&tablePathWriter);
    const tablePath = tablePathWriter.buffered();

    const memTable = try MemTable.init(alloc);
    defer memTable.deinit(alloc);
    const timestampsEncoders = try TimestampsEncoder.TimestampsEncoderPool.init(alloc, 1);
    defer timestampsEncoders.deinit(alloc);
    const compressionPool = try CompressionPool.init(alloc, 1);
    defer compressionPool.deinit(alloc);
    const decompressionPool = try DecompressionPool.init(alloc, 1);
    defer decompressionPool.deinit(alloc);

    var fields1 = [_]Field{
        .{ .key = "level", .value = "info" },
        .{ .key = "app", .value = "seq" },
    };
    var fields2 = [_]Field{
        .{ .key = "level", .value = "warn" },
        .{ .key = "app", .value = "seq" },
    };
    const line1 = Line{
        .timestampNs = 1,
        .fields = fields1[0..],
    };
    const line2 = Line{
        .timestampNs = 2,
        .fields = fields2[0..],
    };

    var lines = [_]Line{ line1, line2 };
    try memTable.addLinesForSid(io, alloc, timestampsEncoders, compressionPool, .{ .id = 1, .tenantID = 1234 }, lines[0..]);
    try memTable.storeToDisk(io, tablePath);

    const table1Path = try alloc.dupe(u8, tablePath);
    const table1 = try Table.open(io, alloc, table1Path, decompressionPool);
    table1.release(io);
    try Dir.accessAbsolute(io, tablePath, .{});

    const table2Path = try alloc.dupe(u8, tablePath);
    const table2 = try Table.open(io, alloc, table2Path, decompressionPool);
    table2.toRemove.store(true, .release);
    table2.release(io);
    try testing.expectError(error.FileNotFound, Dir.accessAbsolute(io, tablePath, .{}));
}

test "release fromMem does not affect filesystem path" {
    const alloc = testing.allocator;
    const io = testing.io;

    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    const rootPath = try tmp.dir.realPathFileAlloc(io, ".", alloc);
    defer alloc.free(rootPath);
    var sentinelPathBuf: [std.fs.max_path_bytes]u8 = undefined;
    var sentinelPathWriter = std.Io.Writer.fixed(&sentinelPathBuf);
    try std.fs.path.fmtJoin(&.{ rootPath, "sentinel" }).format(&sentinelPathWriter);
    const sentinelPath = sentinelPathWriter.buffered();
    // create a real directory to verify it remains
    try testing.expectError(error.FileNotFound, Dir.accessAbsolute(io, sentinelPath, .{}));
    try Dir.createDirAbsolute(io, sentinelPath, .default_dir);

    const decompressionPool = try DecompressionPool.init(alloc, 1);
    defer decompressionPool.deinit(alloc);
    const memTable = try MemTable.init(alloc);

    const table = try Table.fromMem(io, alloc, memTable, decompressionPool);
    // eve if set we expect it to keep the created path when disk == null,
    // so close must not delete it
    table.toRemove.store(true, .release);
    // we expected only second release close cleans the table, otherwise it's a memory leak
    table.retain();

    try Dir.accessAbsolute(io, sentinelPath, .{});
    table.release(io);
    try Dir.accessAbsolute(io, sentinelPath, .{});
    table.release(io);
    try Dir.accessAbsolute(io, sentinelPath, .{});
}

const Field = @import("../lines.zig").Field;

fn deinitQueriedLines(alloc: Allocator, lines: *std.ArrayList(Line)) void {
    for (lines.items) |line| {
        freeFields(alloc, line.fields);
    }
    lines.deinit(alloc);
}

test "fromMem creates proper table from mem table with populated data" {
    const alloc = testing.allocator;
    const io = testing.io;
    const memTable = try MemTable.init(alloc);
    const timestampsEncoders = try TimestampsEncoder.TimestampsEncoderPool.init(alloc, 1);
    defer timestampsEncoders.deinit(alloc);
    const compressionPool = try CompressionPool.init(alloc, 1);
    defer compressionPool.deinit(alloc);
    const decompressionPool = try DecompressionPool.init(alloc, 1);
    defer decompressionPool.deinit(alloc);

    var fields1 = [_]Field{
        .{ .key = "level", .value = "info" },
        .{ .key = "app", .value = "seq" },
    };
    var fields2 = [_]Field{
        .{ .key = "level", .value = "warn" },
        .{ .key = "app", .value = "seq" },
    };
    const line1 = Line{
        .timestampNs = 1,
        .fields = fields1[0..],
    };
    const line2 = Line{
        .timestampNs = 2,
        .fields = fields2[0..],
    };

    var lines = [_]Line{ line1, line2 };

    try memTable.addLinesForSid(io, alloc, timestampsEncoders, compressionPool, .{ .id = 1, .tenantID = 1234 }, lines[0..]);

    const table = try Table.fromMem(io, alloc, memTable, decompressionPool);
    defer table.release(io);

    var indexBuf: [128]u8 = undefined;
    const indexN = try table.readIndex(io, &indexBuf, 0);
    var columnsHeaderIndexBuf: [128]u8 = undefined;
    const columnsHeaderIndexN = try table.readColumnsHeaderIndex(io, &columnsHeaderIndexBuf, 0);
    var columnsHeaderBuf: [128]u8 = undefined;
    const columnsHeaderN = try table.readColumnsHeader(io, &columnsHeaderBuf, 0);
    var timestampsBuf: [128]u8 = undefined;
    const timestampsN = try table.readTimestamps(io, &timestampsBuf, 0);

    try testing.expect(indexBuf[0..indexN].len > 0);
    try testing.expect(columnsHeaderIndexBuf[0..columnsHeaderIndexN].len > 0);
    try testing.expect(columnsHeaderBuf[0..columnsHeaderN].len > 0);
    try testing.expect(timestampsBuf[0..timestampsN].len > 0);

    // if "info" or "seq" is added, the bloom values should be generated
    try testing.expect(table.inner.mem.bloomValuesBuf.items.len > 0);

    try testing.expect(table.columnIdxs.count() > 0);
    try testing.expect(table.columnIDGen.keyIDs.count() > 0);

    try testing.expectEqual(memTable.indexBuf.items.len, indexBuf[0..indexN].len);
    try testing.expectEqual(memTable.columnsHeaderIndexBuf.items.len, columnsHeaderIndexBuf[0..columnsHeaderIndexN].len);
    try testing.expectEqual(memTable.columnsHeaderBuf.items.len, columnsHeaderBuf[0..columnsHeaderN].len);
    try testing.expectEqual(memTable.timestampsBuf.items.len, timestampsBuf[0..timestampsN].len);
}

test "open reads table from disk" {
    const alloc = testing.allocator;
    const io = testing.io;

    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    const rootPath = try tmp.dir.realPathFileAlloc(io, ".", alloc);
    defer alloc.free(rootPath);
    var tablePathBuf: [std.fs.max_path_bytes]u8 = undefined;
    var tablePathWriter = std.Io.Writer.fixed(&tablePathBuf);
    try std.fs.path.fmtJoin(&.{ rootPath, "table-1" }).format(&tablePathWriter);
    const tablePath = tablePathWriter.buffered();

    const memTable = try MemTable.init(alloc);
    defer memTable.deinit(alloc);
    const timestampsEncoders = try TimestampsEncoder.TimestampsEncoderPool.init(alloc, 1);
    defer timestampsEncoders.deinit(alloc);
    const compressionPool = try CompressionPool.init(alloc, 1);
    defer compressionPool.deinit(alloc);
    const decompressionPool = try DecompressionPool.init(alloc, 1);
    defer decompressionPool.deinit(alloc);

    var fields1 = [_]Field{
        .{ .key = "level", .value = "info" },
        .{ .key = "app", .value = "seq" },
    };
    var fields2 = [_]Field{
        .{ .key = "level", .value = "warn" },
        .{ .key = "app", .value = "seq" },
    };
    const line1 = Line{
        .timestampNs = 1,
        .fields = fields1[0..],
    };
    const line2 = Line{
        .timestampNs = 2,
        .fields = fields2[0..],
    };

    var lines = [_]Line{ line1, line2 };
    try memTable.addLinesForSid(io, alloc, timestampsEncoders, compressionPool, .{ .id = 1, .tenantID = 1234 }, lines[0..]);
    try memTable.storeToDisk(io, tablePath);

    const tablePathOwned = try alloc.dupe(u8, tablePath);
    const table = try Table.open(io, alloc, tablePathOwned, decompressionPool);
    defer table.release(io);

    try testing.expect(table.inner == .disk);

    var buf: [128]u8 = undefined;
    var n = try table.readIndex(io, buf[0..memTable.indexBuf.items.len], 0);
    try testing.expectEqual(memTable.indexBuf.items.len, n);
    try testing.expectEqualSlices(u8, memTable.indexBuf.items, buf[0..n]);

    n = try table.readColumnsHeaderIndex(io, buf[0..memTable.columnsHeaderIndexBuf.items.len], 0);
    try testing.expectEqual(memTable.columnsHeaderIndexBuf.items.len, n);
    try testing.expectEqualSlices(u8, memTable.columnsHeaderIndexBuf.items, buf[0..n]);

    n = try table.readColumnsHeader(io, buf[0..memTable.columnsHeaderBuf.items.len], 0);
    try testing.expectEqual(memTable.columnsHeaderBuf.items.len, n);
    try testing.expectEqualSlices(u8, memTable.columnsHeaderBuf.items, buf[0..n]);

    n = try table.readTimestamps(io, buf[0..memTable.timestampsBuf.items.len], 0);
    try testing.expectEqual(memTable.timestampsBuf.items.len, n);
    try testing.expectEqualSlices(u8, memTable.timestampsBuf.items, buf[0..n]);

    n = try table.readMessageBloomTokens(io, buf[0..memTable.messageBloomTokensBuf.items.len], 0);
    try testing.expectEqual(memTable.messageBloomTokensBuf.items.len, n);
    try testing.expectEqualSlices(u8, memTable.messageBloomTokensBuf.items, buf[0..n]);

    n = try table.readMessageBloomValues(io, buf[0..memTable.messageBloomValuesBuf.items.len], 0);
    try testing.expectEqual(memTable.messageBloomValuesBuf.items.len, n);
    try testing.expectEqualSlices(u8, memTable.messageBloomValuesBuf.items, buf[0..n]);

    try testing.expectEqual(
        memTable.tableHeader.bloomValuesBuffersAmount,
        table.tableHeader().bloomValuesBuffersAmount,
    );
    try testing.expectEqual(@as(usize, 1), table.tableHeader().bloomValuesBuffersAmount);

    n = try table.readBloomTokens(io, buf[0..memTable.bloomTokensBuf.items.len], 0, 0);
    try testing.expectEqual(memTable.bloomTokensBuf.items.len, n);
    try testing.expectEqualSlices(u8, memTable.bloomTokensBuf.items, buf[0..n]);

    n = try table.readBloomValues(io, buf[0..memTable.bloomValuesBuf.items.len], 0, 0);
    try testing.expectEqual(memTable.bloomValuesBuf.items.len, n);
    try testing.expectEqualSlices(u8, memTable.bloomValuesBuf.items, buf[0..n]);

    try testing.expect(table.columnIDGen.keyIDs.count() > 0);
    try testing.expect(table.columnIdxs.count() > 0);

    const expectedHeaders = try IndexBlockHeader.readIndexBlockHeaders(
        io,
        alloc,
        decompressionPool,
        memTable.metaIndexBuf.items,
    );
    defer if (expectedHeaders.len > 0) alloc.free(expectedHeaders);
    try testing.expectEqual(expectedHeaders.len, table.indexBlockHeaders.len);
    for (expectedHeaders, table.indexBlockHeaders) |expected, actual| {
        try testing.expectEqualDeep(expected, actual);
    }
}

test "open reads bloom buffers" {
    const alloc = testing.allocator;
    const io = testing.io;

    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    const rootPath = try tmp.dir.realPathFileAlloc(io, ".", alloc);
    defer alloc.free(rootPath);
    var tablePathBuf: [std.fs.max_path_bytes]u8 = undefined;
    var tablePathWriter = std.Io.Writer.fixed(&tablePathBuf);
    try std.fs.path.fmtJoin(&.{ rootPath, "table-1" }).format(&tablePathWriter);
    const tablePath = tablePathWriter.buffered();

    const memTable = try MemTable.init(alloc);
    defer memTable.deinit(alloc);
    const timestampsEncoders = try TimestampsEncoder.TimestampsEncoderPool.init(alloc, 1);
    defer timestampsEncoders.deinit(alloc);
    const compressionPool = try CompressionPool.init(alloc, 1);
    defer compressionPool.deinit(alloc);
    const decompressionPool = try DecompressionPool.init(alloc, 1);
    defer decompressionPool.deinit(alloc);

    const linesCount = 9;
    // otherwise the data goes into a dict and it doesn't reproduce a bug
    try testing.expect(linesCount > ColumnDict.maxDictColumnValuesLen);
    var lines: [linesCount]Line = undefined;
    var fields: [linesCount][1]Field = undefined;
    var values: [linesCount][16]u8 = undefined;
    for (0..linesCount) |i| {
        const v = try std.fmt.bufPrint(&values[i], "unique-value-{d}", .{i});
        fields[i] = .{.{ .key = "msg", .value = v }};
        lines[i] = .{ .timestampNs = @intCast(i + 1), .fields = fields[i][0..] };
    }

    try memTable.addLinesForSid(io, alloc, timestampsEncoders, compressionPool, .{ .id = 1, .tenantID = 1234 }, lines[0..]);
    try testing.expect(memTable.bloomTokensBuf.items.len > 0);

    try memTable.storeToDisk(io, tablePath);

    const tablePathOwned = try alloc.dupe(u8, tablePath);
    const table = try Table.open(io, alloc, tablePathOwned, decompressionPool);
    defer table.release(io);

    var buf: [512]u8 = undefined;
    const n = try table.readBloomTokens(io, buf[0..memTable.bloomTokensBuf.items.len], 0, 0);
    try testing.expectEqual(memTable.bloomTokensBuf.items.len, n);
    try testing.expectEqualSlices(u8, memTable.bloomTokensBuf.items, buf[0..n]);
}

fn testOpenAll(io: Io, alloc: Allocator, rootPath: []const u8, decompressionPool: *DecompressionPool) !void {
    var tables = try Table.openAll(io, alloc, rootPath, decompressionPool);
    defer {
        for (tables.items) |table| table.close(io);
        tables.deinit(alloc);
    }
}

test "openAll handles all io failures" {
    const alloc = testing.allocator;
    const io = testing.io;

    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    const rootPath = try tmp.dir.realPathFileAlloc(io, ".", alloc);
    defer alloc.free(rootPath);

    var table1PathBuf: [std.fs.max_path_bytes]u8 = undefined;
    var table1PathWriter = std.Io.Writer.fixed(&table1PathBuf);
    try std.fs.path.fmtJoin(&.{ rootPath, "table-1" }).format(&table1PathWriter);
    const table1Path = table1PathWriter.buffered();

    var table2PathBuf: [std.fs.max_path_bytes]u8 = undefined;
    var table2PathWriter = std.Io.Writer.fixed(&table2PathBuf);
    try std.fs.path.fmtJoin(&.{ rootPath, "table-2" }).format(&table2PathWriter);
    const table2Path = table2PathWriter.buffered();

    const memTable = try MemTable.init(alloc);
    defer memTable.deinit(alloc);
    const timestampsEncoders = try TimestampsEncoder.TimestampsEncoderPool.init(alloc, 1);
    defer timestampsEncoders.deinit(alloc);
    const compressionPool = try CompressionPool.init(alloc, 1);
    defer compressionPool.deinit(alloc);

    var fields = [_]Field{
        .{ .key = "level", .value = "info" },
        .{ .key = "app", .value = "seq" },
    };
    var lines = [_]Line{.{
        .timestampNs = 1,
        .fields = fields[0..],
    }};

    try memTable.addLinesForSid(io, alloc, timestampsEncoders, compressionPool, .{ .id = 1, .tenantID = 1234 }, lines[0..]);
    try memTable.storeToDisk(io, table1Path);
    try memTable.storeToDisk(io, table2Path);

    var tablesFilePathBuf: [std.fs.max_path_bytes]u8 = undefined;
    var tablesFilePathWriter = std.Io.Writer.fixed(&tablesFilePathBuf);
    try std.fs.path.fmtJoin(&.{ rootPath, filenames.tables }).format(&tablesFilePathWriter);
    const tablesFilePath = tablesFilePathWriter.buffered();
    try fs.writeBufferToFileAtomic(io, tablesFilePath, "[\"table-1\",\"table-2\"]", true);

    const decompressionPool = try DecompressionPool.init(alloc, 1);
    defer decompressionPool.deinit(alloc);
    try stesting.checkAllIoFailures(io, testOpenAll, .{ alloc, rootPath, decompressionPool });
}

test "queryLines" {
    const alloc = testing.allocator;
    const io = testing.io;
    const ExpectedLine = struct {
        timestampNs: u64,
        app: []const u8,
    };
    const Case = struct {
        requestedSIDs: []const SID,
        query: Query,
        expected: []const ExpectedLine,
    };

    const sidBlock = SID{ .id = 10, .tenantID = 1234 };
    const sid1 = SID{ .id = 1, .tenantID = 1234 };
    const sid2 = SID{ .id = 2, .tenantID = 1234 };
    const sid3 = SID{ .id = 3, .tenantID = 1234 };
    const sid5 = SID{ .id = 5, .tenantID = 1234 };
    const sidMissing = SID{ .id = 4, .tenantID = 1234 };
    const sidTenantA = SID{ .id = 1, .tenantID = 1111 };
    const sidTenantB = SID{ .id = 1, .tenantID = 2222 };

    var seqInfo = [_]Field{ .{ .key = "app", .value = "seq" }, .{ .key = "level", .value = "info" } };
    var seqWarn = [_]Field{ .{ .key = "app", .value = "seq" }, .{ .key = "level", .value = "warn" } };
    var seqError = [_]Field{ .{ .key = "app", .value = "seq" }, .{ .key = "level", .value = "error" } };
    var apiInfo = [_]Field{ .{ .key = "app", .value = "api" }, .{ .key = "level", .value = "info" } };
    var apiErrorA = [_]Field{ .{ .key = "app", .value = "api" }, .{ .key = "level", .value = "error" } };
    var apiErrorB = [_]Field{ .{ .key = "app", .value = "api" }, .{ .key = "level", .value = "error" } };
    var workerInfo = [_]Field{ .{ .key = "app", .value = "worker" }, .{ .key = "level", .value = "info" } };
    var tenantAFields = [_]Field{ .{ .key = "app", .value = "api-a" }, .{ .key = "level", .value = "info" } };
    var tenantBFields = [_]Field{ .{ .key = "app", .value = "api-b" }, .{ .key = "level", .value = "info" } };

    var lines = [_]Line{
        .{ .timestampNs = 1, .fields = seqInfo[0..] },
        .{ .timestampNs = 2, .fields = seqWarn[0..] },
        .{ .timestampNs = 3, .fields = seqError[0..] },
        .{ .timestampNs = 1, .fields = seqInfo[0..] },
        .{ .timestampNs = 2, .fields = seqError[0..] },
        .{ .timestampNs = 3, .fields = apiErrorA[0..] },
        .{ .timestampNs = 6, .fields = apiErrorB[0..] },
        .{ .timestampNs = 3, .fields = apiInfo[0..] },
        .{ .timestampNs = 10, .fields = workerInfo[0..] },
        .{ .timestampNs = 1, .fields = tenantAFields[0..] },
        .{ .timestampNs = 2, .fields = tenantBFields[0..] },
    };

    const memTable = try MemTable.init(alloc);
    const timestampsEncoders = try TimestampsEncoder.TimestampsEncoderPool.init(alloc, 1);
    defer timestampsEncoders.deinit(alloc);
    const compressionPool = try CompressionPool.init(alloc, 1);
    defer compressionPool.deinit(alloc);
    const decompressionPool = try DecompressionPool.init(alloc, 1);
    defer decompressionPool.deinit(alloc);
    var sids = [_]SID{ sidBlock, sid1, sid2, sid3, sid5, sidTenantA, sidTenantB };
    var linesBySid = [_][]Line{
        lines[0..3],
        lines[3..5],
        lines[5..7],
        lines[7..8],
        lines[8..9],
        lines[9..10],
        lines[10..11],
    };
    try memTable.addLines(io, alloc, timestampsEncoders, compressionPool, sids[0..], linesBySid[0..]);

    const table = try Table.fromMem(io, alloc, memTable, decompressionPool);
    defer table.release(io);

    const noTagsExpr: Query.FilterExpression = .{ .predicate = .{ .key = "", .value = "", .op = .equal } };
    const warnExpr: Query.FilterExpression = .{ .predicate = .{ .key = "level", .value = "warn", .op = .equal } };
    const fatalExpr: Query.FilterExpression = .{ .predicate = .{ .key = "level", .value = "fatal", .op = .equal } };
    const errorExpr: Query.FilterExpression = .{ .predicate = .{ .key = "level", .value = "error", .op = .equal } };

    const cases = [_]Case{
        .{
            .requestedSIDs = &.{sidBlock},
            .query = .{ .start = 1, .end = 3, .tagsExpr = &noTagsExpr, .fieldsExpr = null },
            .expected = &.{
                .{ .timestampNs = 1, .app = "seq" },
                .{ .timestampNs = 2, .app = "seq" },
                .{ .timestampNs = 3, .app = "seq" },
            },
        },
        .{
            .requestedSIDs = &.{sidBlock},
            .query = .{ .start = 1, .end = 3, .tagsExpr = &noTagsExpr, .fieldsExpr = &warnExpr },
            .expected = &.{.{ .timestampNs = 2, .app = "seq" }},
        },
        .{
            .requestedSIDs = &.{sidBlock},
            .query = .{ .start = 1, .end = 3, .tagsExpr = &noTagsExpr, .fieldsExpr = &fatalExpr },
            .expected = &.{},
        },
        .{
            .requestedSIDs = &.{sidBlock},
            .query = .{ .start = 10, .end = 20, .tagsExpr = &noTagsExpr, .fieldsExpr = null },
            .expected = &.{},
        },
        .{
            .requestedSIDs = &.{ sidMissing, sid5 },
            .query = .{ .start = 0, .end = 20, .tagsExpr = &noTagsExpr, .fieldsExpr = null },
            .expected = &.{.{ .timestampNs = 10, .app = "worker" }},
        },
        .{
            .requestedSIDs = &.{ sid1, sid2 },
            .query = .{ .start = 2, .end = 5, .tagsExpr = &noTagsExpr, .fieldsExpr = &errorExpr },
            .expected = &.{
                .{ .timestampNs = 2, .app = "seq" },
                .{ .timestampNs = 3, .app = "api" },
            },
        },
        .{
            .requestedSIDs = &.{ sid1, sid1 },
            .query = .{ .start = 0, .end = 10, .tagsExpr = &noTagsExpr, .fieldsExpr = null },
            .expected = &.{
                .{ .timestampNs = 1, .app = "seq" },
                .{ .timestampNs = 2, .app = "seq" },
            },
        },
        .{
            .requestedSIDs = &.{sidTenantB},
            .query = .{ .start = 0, .end = 10, .tagsExpr = &noTagsExpr, .fieldsExpr = null },
            .expected = &.{.{ .timestampNs = 2, .app = "api-b" }},
        },
        .{
            .requestedSIDs = &.{ sidMissing, sid5 },
            .query = .{ .start = 0, .end = 9, .tagsExpr = &noTagsExpr, .fieldsExpr = null },
            .expected = &.{},
        },
        .{
            .requestedSIDs = &.{},
            .query = .{ .start = 0, .end = 10, .tagsExpr = &noTagsExpr, .fieldsExpr = null },
            .expected = &.{},
        },
        .{
            .requestedSIDs = &.{},
            .query = .{ .start = 0, .end = 10, .tagsExpr = null, .fieldsExpr = &warnExpr },
            .expected = &.{.{ .timestampNs = 2, .app = "seq" }},
        },
    };

    var queried = std.ArrayList(Line).empty;
    defer deinitQueriedLines(alloc, &queried);

    for (cases) |case| {
        for (queried.items) |line| {
            freeFields(alloc, line.fields);
        }
        queried.clearRetainingCapacity();

        const requested = try alloc.dupe(SID, case.requestedSIDs);
        defer alloc.free(requested);

        try table.queryLines(io, alloc, false, timestampsEncoders, decompressionPool, &queried, requested, case.query);
        try testing.expectEqual(case.expected.len, queried.items.len);

        for (case.expected, 0..) |expected, i| {
            try testing.expectEqual(expected.timestampNs, queried.items[i].timestampNs);
            var app: ?[]const u8 = null;
            for (queried.items[i].fields) |field| {
                if (std.mem.eql(u8, field.key, "app")) {
                    app = field.value;
                    break;
                }
            }
            try testing.expect(app != null);
            try testing.expectEqualStrings(expected.app, app.?);
        }
    }
}

test "queryLinesAllBlocks reads later index blocks using size as length" {
    const alloc = testing.allocator;
    const io = testing.io;

    const streamsCount = 3000;
    var sids: [streamsCount]SID = undefined;
    var linesBySid: [streamsCount][]Line = undefined;
    var lines: [streamsCount]Line = undefined;
    var fields: [streamsCount][2]Field = undefined;

    for (0..streamsCount) |i| {
        sids[i] = .{ .id = @intCast(i + 1), .tenantID = 1234 };
        fields[i] = .{
            .{ .key = "app", .value = "bulk" },
            .{ .key = "level", .value = "info" },
        };
        lines[i] = .{ .timestampNs = @intCast(i + 1), .fields = fields[i][0..] };
        linesBySid[i] = lines[i .. i + 1];
    }

    const memTable = try MemTable.init(alloc);
    const timestampsEncoders = try TimestampsEncoder.TimestampsEncoderPool.init(alloc, 1);
    defer timestampsEncoders.deinit(alloc);
    const compressionPool = try CompressionPool.init(alloc, 1);
    defer compressionPool.deinit(alloc);
    const decompressionPool = try DecompressionPool.init(alloc, 1);
    defer decompressionPool.deinit(alloc);
    try memTable.addLines(io, alloc, timestampsEncoders, compressionPool, &sids, &linesBySid);

    const table = try Table.fromMem(io, alloc, memTable, decompressionPool);
    defer table.release(io);

    try testing.expect(table.indexBlockHeaders.len > 1);
    try testing.expect(table.indexBlockHeaders[1].offset > table.indexBlockHeaders[1].size);

    var queried = try std.ArrayList(Line).initCapacity(alloc, streamsCount);
    defer deinitQueriedLines(alloc, &queried);
    const infoExpr: Query.FilterExpression = .{ .predicate = .{ .key = "level", .value = "info", .op = .equal } };
    const query = Query{ .start = 1, .end = streamsCount, .tagsExpr = null, .fieldsExpr = &infoExpr };

    try table.queryLines(io, alloc, false, timestampsEncoders, decompressionPool, &queried, &.{}, query);
    try testing.expectEqual(streamsCount, queried.items.len);
    try testing.expectEqual(1, queried.items[0].timestampNs);
    try testing.expectEqual(streamsCount, queried.items[queried.items.len - 1].timestampNs);
}

// TODO: test flushed mem table is the same as an opened one,
// kinda round trippness property,
// and do the same with index tables (probably already done)

test "queryLinesReproducerWhenMixedEmptyKeyAndNonEmptyKey" {
    const alloc = testing.allocator;
    const io: Io = testing.io;

    const sid = SID{ .id = 1, .tenantID = 1234 };

    var fields1 = [_]Field{
        .{ .key = "", .value = "message-1" },
        .{ .key = "level", .value = "info" },
    };
    var fields2 = [_]Field{
        .{ .key = "", .value = "message-2" },
        .{ .key = "level", .value = "warn" },
    };
    var lines = [_]Line{
        .{ .timestampNs = 1, .fields = fields1[0..] },
        .{ .timestampNs = 2, .fields = fields2[0..] },
    };

    const memTable = try MemTable.init(alloc);
    const timestampsEncoders = try TimestampsEncoder.TimestampsEncoderPool.init(alloc, 1);
    defer timestampsEncoders.deinit(alloc);
    const compressionPool = try CompressionPool.init(alloc, 1);
    defer compressionPool.deinit(alloc);
    const decompressionPool = try DecompressionPool.init(alloc, 1);
    defer decompressionPool.deinit(alloc);
    try memTable.addLinesForSid(io, alloc, timestampsEncoders, compressionPool, sid, lines[0..]);

    const table = try Table.fromMem(io, alloc, memTable, decompressionPool);
    defer table.release(io);

    var queried = std.ArrayList(Line).empty;
    defer deinitLinesFull(alloc, &queried);

    const noTagsExpr: Query.FilterExpression = .{ .predicate = .{ .key = "", .value = "", .op = .equal } };
    const query = Query{ .start = 0, .end = 10, .tagsExpr = &noTagsExpr, .fieldsExpr = null };
    var requested = [_]SID{sid};

    try table.queryLines(io, alloc, false, timestampsEncoders, decompressionPool, &queried, requested[0..], query);
}

test "queryLines reads disk table fields after open" {
    const alloc = testing.allocator;
    const io = testing.io;

    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    const rootPath = try tmp.dir.realPathFileAlloc(io, ".", alloc);
    defer alloc.free(rootPath);
    var tablePathBuf: [std.fs.max_path_bytes]u8 = undefined;
    var tablePathWriter = std.Io.Writer.fixed(&tablePathBuf);
    try std.fs.path.fmtJoin(&.{ rootPath, "table" }).format(&tablePathWriter);
    const tablePath = tablePathWriter.buffered();

    const sid = SID{ .id = 1, .tenantID = 11 };
    var fields1 = [_]Field{
        .{ .key = "", .value = "GET /api/health 200 3ms" },
        .{ .key = "id", .value = "web-001" },
        .{ .key = "status", .value = "200" },
        .{ .key = "path", .value = "/api/health" },
    };
    var fields2 = [_]Field{
        .{ .key = "", .value = "POST /api/orders 500 timeout" },
        .{ .key = "id", .value = "web-002" },
        .{ .key = "status", .value = "500" },
        .{ .key = "path", .value = "/api/orders" },
    };
    var lines = [_]Line{
        .{ .timestampNs = 1, .fields = fields1[0..] },
        .{ .timestampNs = 2, .fields = fields2[0..] },
    };

    const memTable = try MemTable.init(alloc);
    defer memTable.deinit(alloc);
    const timestampsEncoders = try TimestampsEncoder.TimestampsEncoderPool.init(alloc, 1);
    defer timestampsEncoders.deinit(alloc);
    const compressionPool = try CompressionPool.init(alloc, 1);
    defer compressionPool.deinit(alloc);
    const decompressionPool = try DecompressionPool.init(alloc, 1);
    defer decompressionPool.deinit(alloc);
    try memTable.addLinesForSid(io, alloc, timestampsEncoders, compressionPool, sid, lines[0..]);
    try memTable.storeToDisk(io, tablePath);

    const tablePathOwned = try alloc.dupe(u8, tablePath);
    const table = try Table.open(io, alloc, tablePathOwned, decompressionPool);
    defer table.release(io);

    var queried = std.ArrayList(Line).empty;
    defer deinitQueriedLines(alloc, &queried);

    const noTagsExpr: Query.FilterExpression = .{ .predicate = .{ .key = "", .value = "", .op = .equal } };
    const statusExpr: Query.FilterExpression = .{ .predicate = .{ .key = "status", .value = "200", .op = .equal } };
    const query = Query{ .start = 0, .end = 10, .tagsExpr = &noTagsExpr, .fieldsExpr = &statusExpr };
    var requested = [_]SID{sid};

    try table.queryLines(io, alloc, false, timestampsEncoders, decompressionPool, &queried, requested[0..], query);
    try testing.expectEqual(@as(usize, 1), queried.items.len);
    try testing.expectEqual(@as(u64, 1), queried.items[0].timestampNs);
    try testing.expectEqualSlices(u8, "id", queried.items[0].fields[1].key);
    try testing.expectEqualSlices(u8, "web-001", queried.items[0].fields[1].value);
}
