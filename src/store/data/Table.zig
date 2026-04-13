const std = @import("std");
const Allocator = std.mem.Allocator;

const filenames = @import("../../filenames.zig");
const fs = @import("../../fs.zig");
const MemTable = @import("../inmem/MemTable.zig");
const DiskTable = @import("DiskTable.zig");
const IndexBlockHeader = @import("../inmem/IndexBlockHeader.zig");
const TableHeader = @import("../inmem/TableHeader.zig");
const ColumnIDGen = @import("../inmem/ColumnIDGen.zig");

const Line = @import("../lines.zig").Line;
const SID = @import("../lines.zig").SID;
const Query = @import("../query.zig").Query;

const catalog = @import("../table/catalog.zig");

const Table = @This();

// either one has to be available
// // NOTE: adding a third one like object table may complicated it,
// so then it would require implement at able as an interface
disk: ?*DiskTable,
mem: ?*MemTable,

// fields for all the tables
indexBlockHeaders: []IndexBlockHeader,
tableHeader: *TableHeader,
// size is amount of bytes of compressed buffers content
size: u64,
path: []const u8,

indexBuf: []const u8,
columnsHeaderIndexBuf: []const u8,
columnsHeaderBuf: []const u8,
timestampsBuf: []const u8,

messageBloomTokens: []const u8,
messageBloomValues: []const u8,
bloomTokensShards: [][]const u8,
bloomValuesShards: [][]const u8,

columnIDGen: *ColumnIDGen,
columnIdxs: std.StringHashMapUnmanaged(u16),

// holds ownership,
// it's necessary in order to support ref counter
alloc: Allocator,

// state

// inMerge defines whether the table is taken by a merge job
inMerge: bool = false,
// toRemove defines if the table must be removed on releasing,
// we do it via a flag instead of a direct removal,
// because a table could be retained in a reader
toRemove: std.atomic.Value(bool) = .init(false),
// refCounter follows how many clients open a table,
// first time it's open on start up,
// then readers can retain it
refCounter: std.atomic.Value(u32),

pub fn openAll(parentAlloc: Allocator, path: []const u8) !std.ArrayList(*Table) {
    std.fs.makeDirAbsolute(path) catch |err| switch (err) {
        // TODO: if the foler already exists we must read it's content and log an error
        // in case the tables on the disk are missing in the tables list
        std.posix.MakeDirError.PathAlreadyExists => {},
        else => std.debug.panic(
            "failed to create a table dir '{s}': {s}",
            .{ path, @errorName(err) },
        ),
    };

    // fsync after opening tables because it creates the files
    defer fs.syncPathAndParentDir(path);

    var fba = std.heap.stackFallback(2048, parentAlloc);
    const alloc = fba.get();

    // read table names,
    // they are given either from a file or listed directories in the path
    const tablesFilePath = try std.fs.path.join(alloc, &[_][]const u8{ path, filenames.tables });
    defer alloc.free(tablesFilePath);
    var tableNames = try catalog.readNames(alloc, tablesFilePath, true);
    defer {
        for (tableNames.items) |tableName| alloc.free(tableName);
        tableNames.deinit(alloc);
    }

    // syncing tables with a json, make sure all the listed dirs exist
    try catalog.validateTablesExist(alloc, path, tableNames.items);

    // syncing tables with the given names remove all the not listed dirs
    try catalog.removeUnusedTables(alloc, path, tableNames.items);

    // open tables
    var tables = try std.ArrayList(*Table).initCapacity(parentAlloc, tableNames.items.len);
    errdefer {
        tables.deinit(parentAlloc);
    }
    for (tableNames.items) |tableName| {
        // don't clean tablePath, Table owns it
        const tablePath = try std.fs.path.join(parentAlloc, &.{ path, tableName });
        errdefer parentAlloc.free(tablePath);
        const table = try Table.open(parentAlloc, tablePath);
        tables.appendAssumeCapacity(table);
    }

    return tables;
}

pub fn open(alloc: Allocator, path: []const u8) !*Table {
    var fba = std.heap.stackFallback(2048, alloc);
    const fbaAlloc = fba.get();

    const header = try TableHeader.readFile(alloc, path);

    const disk = try alloc.create(DiskTable);
    errdefer alloc.destroy(disk);
    disk.* = .{
        .tableHeader = header,
    };

    const columnKeysPath = try std.fs.path.join(fbaAlloc, &.{ path, filenames.columnKeys });
    defer fbaAlloc.free(columnKeysPath);
    const columnIdxsPath = try std.fs.path.join(fbaAlloc, &.{ path, filenames.columnIdxs });
    defer fbaAlloc.free(columnIdxsPath);
    const metaindexPath = try std.fs.path.join(fbaAlloc, &.{ path, filenames.metaindex });
    defer fbaAlloc.free(metaindexPath);
    const indexPath = try std.fs.path.join(fbaAlloc, &.{ path, filenames.index });
    defer fbaAlloc.free(indexPath);
    const columnsHeaderIndexPath = try std.fs.path.join(fbaAlloc, &.{ path, filenames.columnsHeaderIndex });
    defer fbaAlloc.free(columnsHeaderIndexPath);
    const columnsHeaderPath = try std.fs.path.join(fbaAlloc, &.{ path, filenames.columnsHeader });
    defer fbaAlloc.free(columnsHeaderPath);
    const timestampsPath = try std.fs.path.join(fbaAlloc, &.{ path, filenames.timestamps });
    defer fbaAlloc.free(timestampsPath);
    const messageBloomTokensPath = try std.fs.path.join(fbaAlloc, &.{ path, filenames.messageTokens });
    defer fbaAlloc.free(messageBloomTokensPath);
    const messageBloomValuesPath = try std.fs.path.join(fbaAlloc, &.{ path, filenames.messageValues });
    defer fbaAlloc.free(messageBloomValuesPath);

    const columnKeysContent = try fs.readAll(alloc, columnKeysPath);
    defer alloc.free(columnKeysContent);
    var columnIDGen: *ColumnIDGen = blk: {
        if (columnKeysContent.len > 0) {
            break :blk try ColumnIDGen.decode(alloc, columnKeysContent);
        } else {
            break :blk try ColumnIDGen.init(alloc);
        }
    };
    errdefer columnIDGen.deinit(alloc);

    var columnIdxs = std.StringHashMapUnmanaged(u16){};
    errdefer columnIdxs.deinit(alloc);

    const columnIdxsContent = try fs.readAll(alloc, columnIdxsPath);
    defer alloc.free(columnIdxsContent);
    if (columnIdxsContent.len > 0) {
        columnIdxs = try columnIDGen.decodeColumnIdxs(alloc, columnIdxsContent);
    }

    const metaindexContent = try fs.readAll(alloc, metaindexPath);
    defer alloc.free(metaindexContent);
    var indexBlockHeaders: []IndexBlockHeader = &.{};
    if (metaindexContent.len > 0) {
        indexBlockHeaders = try IndexBlockHeader.readIndexBlockHeaders(alloc, metaindexContent);
    }
    errdefer {
        for (indexBlockHeaders) |*hdr| hdr.deinitRead(alloc);
        if (indexBlockHeaders.len > 0) alloc.free(indexBlockHeaders);
    }

    const indexBuf = try fs.readAll(alloc, indexPath);
    errdefer alloc.free(indexBuf);
    const columnsHeaderIndexBuf = try fs.readAll(alloc, columnsHeaderIndexPath);
    errdefer alloc.free(columnsHeaderIndexBuf);
    const columnsHeaderBuf = try fs.readAll(alloc, columnsHeaderPath);
    errdefer alloc.free(columnsHeaderBuf);
    const timestampsBuf = try fs.readAll(alloc, timestampsPath);
    errdefer alloc.free(timestampsBuf);
    const messageBloomTokensBuf = try fs.readAll(alloc, messageBloomTokensPath);
    errdefer alloc.free(messageBloomTokensBuf);
    const messageBloomValuesBuf = try fs.readAll(alloc, messageBloomValuesPath);
    errdefer alloc.free(messageBloomValuesBuf);

    const shardCount: usize = @intCast(header.bloomValuesBuffersAmount);
    var bloomTokensShards = try alloc.alloc([]const u8, shardCount);
    errdefer alloc.free(bloomTokensShards);
    var bloomValuesShards = try alloc.alloc([]const u8, shardCount);
    errdefer alloc.free(bloomValuesShards);

    var shardIdx: usize = 0;
    errdefer {
        for (bloomTokensShards[0..shardIdx]) |shard| {
            alloc.free(shard);
        }
        for (bloomValuesShards[0..shardIdx]) |shard| {
            alloc.free(shard);
        }
    }
    while (shardIdx < shardCount) : (shardIdx += 1) {
        const bloomTokensPath = try MemTable.getBloomTokensFilePath(
            fbaAlloc,
            path,
            @intCast(shardIdx),
        );
        defer fbaAlloc.free(bloomTokensPath);
        const bloomValuesPath = try MemTable.getBloomValuesFilePath(
            fbaAlloc,
            path,
            @intCast(shardIdx),
        );
        defer fbaAlloc.free(bloomValuesPath);
        const bloomTokensBuf = try fs.readAll(alloc, bloomTokensPath);
        errdefer alloc.free(bloomTokensBuf);
        const bloomValuesBuf = try fs.readAll(alloc, bloomValuesPath);
        errdefer alloc.free(bloomValuesBuf);
        bloomTokensShards[shardIdx] = bloomTokensBuf;
        bloomValuesShards[shardIdx] = bloomValuesBuf;
    }

    const table = try alloc.create(Table);
    table.* = .{
        .mem = null,
        .disk = disk,
        .size = header.compressedSize,
        .path = path,
        .indexBlockHeaders = indexBlockHeaders,
        .tableHeader = &disk.tableHeader,
        .indexBuf = indexBuf,
        .columnsHeaderIndexBuf = columnsHeaderIndexBuf,
        .columnsHeaderBuf = columnsHeaderBuf,
        .timestampsBuf = timestampsBuf,
        .messageBloomTokens = messageBloomTokensBuf,
        .messageBloomValues = messageBloomValuesBuf,
        .bloomTokensShards = bloomTokensShards,
        .bloomValuesShards = bloomValuesShards,
        .columnIDGen = columnIDGen,
        .columnIdxs = columnIdxs,
        .refCounter = .init(1),
        .alloc = alloc,
    };

    return table;
}

pub fn close(self: *Table) void {
    if (self.disk) |disk| {
        self.alloc.free(self.indexBuf);
        self.alloc.free(self.columnsHeaderIndexBuf);
        self.alloc.free(self.columnsHeaderBuf);
        self.alloc.free(self.timestampsBuf);
        self.alloc.free(self.messageBloomTokens);
        self.alloc.free(self.messageBloomValues);
        for (self.bloomTokensShards) |shard| {
            self.alloc.free(shard);
        }
        for (self.bloomValuesShards) |shard| {
            self.alloc.free(shard);
        }
        disk.deinit(self.alloc);
    }

    if (self.mem) |mem| {
        mem.deinit(self.alloc);
    }

    for (self.indexBlockHeaders) |*hdr| hdr.deinitRead(self.alloc);
    if (self.indexBlockHeaders.len > 0) self.alloc.free(self.indexBlockHeaders);

    self.columnIDGen.deinit(self.alloc);
    self.columnIdxs.deinit(self.alloc);
    self.alloc.free(self.bloomTokensShards);
    self.alloc.free(self.bloomValuesShards);

    const shouldRemove = self.disk != null and self.toRemove.load(.acquire);
    if (shouldRemove) {
        // TODO: replace to an error log
        // TODO: review it to make removing more reliable,
        // e.g. deletion must be intrrupted in the middle leaving a half baked table
        std.fs.deleteTreeAbsolute(self.path) catch |err| {
            std.debug.panic("failed to delete table '{s}': {s}", .{ self.path, @errorName(err) });
        };
    }

    if (self.path.len > 0) {
        self.alloc.free(self.path);
    }

    self.alloc.destroy(self);
}

pub fn fromMem(alloc: Allocator, memTable: *MemTable) !*Table {
    std.debug.assert(memTable.streamWriter.size() == memTable.tableHeader.compressedSize);

    // TODO: move ownership of the original meta index to the table, not only the buffers,
    // but it requires index collecting during ingestion
    var indexBlockHeaders: []IndexBlockHeader = &.{};
    const metaIndexBuf = memTable.streamWriter.metaIndexDst.asSliceAssumeBuffer();
    if (metaIndexBuf.len > 0) {
        indexBlockHeaders = try IndexBlockHeader.readIndexBlockHeaders(alloc, metaIndexBuf);
    }
    errdefer {
        for (indexBlockHeaders) |*hdr| hdr.deinitRead(alloc);
        if (indexBlockHeaders.len > 0) alloc.free(indexBlockHeaders);
    }

    // TODO: avoid decoding column ids, we can simply assign what we have from the stream writer
    var columnIDGen: *ColumnIDGen = undefined;
    if (memTable.streamWriter.columnKeysBuf.asSliceAssumeBuffer().len > 0) {
        columnIDGen = try ColumnIDGen.decode(alloc, memTable.streamWriter.columnKeysBuf.asSliceAssumeBuffer());
    } else {
        columnIDGen = try ColumnIDGen.init(alloc);
    }
    errdefer columnIDGen.deinit(alloc);

    var columnIdxs = std.StringHashMapUnmanaged(u16){};
    errdefer columnIdxs.deinit(alloc);

    if (memTable.streamWriter.columnIdxsBuf.asSliceAssumeBuffer().len > 0) {
        columnIdxs = try columnIDGen.decodeColumnIdxs(alloc, memTable.streamWriter.columnIdxsBuf.asSliceAssumeBuffer());
    }

    const bloomTokensList = memTable.streamWriter.bloomTokensList.items;
    const bloomValuesList = memTable.streamWriter.bloomValuesList.items;
    var bloomTokensShards = try alloc.alloc([]const u8, bloomTokensList.len);
    errdefer alloc.free(bloomTokensShards);
    var bloomValuesShards = try alloc.alloc([]const u8, bloomValuesList.len);
    errdefer alloc.free(bloomValuesShards);

    for (0..bloomValuesList.len) |i| {
        bloomTokensShards[i] = bloomTokensList[i].asSliceAssumeBuffer();
        bloomValuesShards[i] = bloomValuesList[i].asSliceAssumeBuffer();
    }

    const table = try alloc.create(Table);
    table.* = .{
        .mem = memTable,
        .disk = null,
        .size = memTable.tableHeader.compressedSize,
        .path = "",
        .indexBlockHeaders = indexBlockHeaders,
        .tableHeader = &memTable.tableHeader,
        .indexBuf = memTable.streamWriter.indexDst.asSliceAssumeBuffer(),
        .columnsHeaderIndexBuf = memTable.streamWriter.columnsHeaderIndexDst.asSliceAssumeBuffer(),
        .columnsHeaderBuf = memTable.streamWriter.columnsHeaderDst.asSliceAssumeBuffer(),
        .timestampsBuf = memTable.streamWriter.timestampsDst.asSliceAssumeBuffer(),
        .messageBloomTokens = memTable.streamWriter.messageBloomTokensDst.asSliceAssumeBuffer(),
        .messageBloomValues = memTable.streamWriter.messageBloomValuesDst.asSliceAssumeBuffer(),
        .bloomTokensShards = bloomTokensShards,
        .bloomValuesShards = bloomValuesShards,
        .columnIDGen = columnIDGen,
        .columnIdxs = columnIdxs,
        .refCounter = .init(1),
        .alloc = alloc,
    };

    return table;
}

pub fn writeNames(alloc: Allocator, path: []const u8, tables: []*Table) anyerror!void {
    var stackFba = std.heap.stackFallback(1024, alloc);
    const fba = stackFba.get();

    var tableNames = try std.ArrayList([]const u8).initCapacity(fba, tables.len);
    defer tableNames.deinit(fba);

    for (tables) |table| {
        std.debug.assert(table.disk != null);
        tableNames.appendAssumeCapacity(std.fs.path.basename(table.path));
    }

    // TODO: worth migrating json to names suparated by new line \n
    // since they are limited to 16 symbols hex symbols [0-9A-F]
    const data = try std.json.Stringify.valueAlloc(fba, tableNames.items, .{
        .whitespace = .minified,
    });
    defer fba.free(data);

    const tablesFilePath = try std.fs.path.join(fba, &.{ path, filenames.tables });
    defer fba.free(tablesFilePath);

    try fs.writeBufferToFileAtomic(tablesFilePath, data, true);
}

pub fn retain(self: *Table) void {
    _ = self.refCounter.fetchAdd(1, .acquire);
}

pub fn release(self: *Table) void {
    const prev = self.refCounter.fetchSub(1, .acq_rel);
    std.debug.assert(prev > 0);

    if (prev != 1) return;

    self.close();
}

pub fn queryLines(self: *Table, alloc: Allocator, dst: *std.ArrayList(Line), sids: []SID, query: Query) !void {
    _ = self;
    _ = alloc;
    _ = dst;
    _ = sids;
    _ = query;
}

pub fn lessThan(_: void, one: *Table, another: *Table) bool {
    return one.size < another.size;
}

const testing = std.testing;

test "release keeps table unless toRemove is set, then removes table dir" {
    const alloc = testing.allocator;

    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    const rootPath = try tmp.dir.realpathAlloc(alloc, ".");
    defer alloc.free(rootPath);
    const tablePath = try std.fs.path.join(alloc, &.{ rootPath, "table-1" });
    defer alloc.free(tablePath);

    const memTable = try MemTable.init(alloc);
    defer memTable.deinit(alloc);

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
        .sid = .{ .id = 1, .tenantID = "1234" },
        .fields = fields1[0..],
    };
    const line2 = Line{
        .timestampNs = 2,
        .sid = .{ .id = 1, .tenantID = "1234" },
        .fields = fields2[0..],
    };

    var lines = [_]Line{ line1, line2 };
    try memTable.addLines(alloc, lines[0..]);
    try memTable.storeToDisk(alloc, tablePath);

    const table1Path = try alloc.dupe(u8, tablePath);
    const table1 = try Table.open(alloc, table1Path);
    table1.release();
    try std.fs.accessAbsolute(tablePath, .{});

    const table2Path = try alloc.dupe(u8, tablePath);
    const table2 = try Table.open(alloc, table2Path);
    table2.toRemove.store(true, .release);
    table2.release();
    try testing.expectError(error.FileNotFound, std.fs.accessAbsolute(tablePath, .{}));
}

test "release fromMem does not affect filesystem path" {
    const alloc = testing.allocator;

    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    const rootPath = try tmp.dir.realpathAlloc(alloc, ".");
    defer alloc.free(rootPath);
    const sentinelPath = try std.fs.path.join(alloc, &.{ rootPath, "sentinel" });
    defer alloc.free(sentinelPath);
    // create a real directory to verify it remains
    try testing.expectError(error.FileNotFound, std.fs.accessAbsolute(sentinelPath, .{}));
    try std.fs.makeDirAbsolute(sentinelPath);

    const memTable = try MemTable.init(alloc);

    const table = try Table.fromMem(alloc, memTable);
    // eve if set we expect it to keep the created path when disk == null,
    // so close must not delete it
    table.toRemove.store(true, .release);
    // we expected only second release close cleans the table, otherwise it's a memory leak
    table.retain();

    try std.fs.accessAbsolute(sentinelPath, .{});
    table.release();
    try std.fs.accessAbsolute(sentinelPath, .{});
    table.release();
    try std.fs.accessAbsolute(sentinelPath, .{});
}

const Field = @import("../lines.zig").Field;

test "fromMem creates proper table from mem table with populated data" {
    const alloc = testing.allocator;
    const memTable = try MemTable.init(alloc);

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
        .sid = .{ .id = 1, .tenantID = "1234" },
        .fields = fields1[0..],
    };
    const line2 = Line{
        .timestampNs = 2,
        .sid = .{ .id = 1, .tenantID = "1234" },
        .fields = fields2[0..],
    };

    var lines = [_]Line{ line1, line2 };

    try memTable.addLines(alloc, lines[0..]);

    const table = try Table.fromMem(alloc, memTable);
    defer table.release();

    try testing.expect(table.indexBuf.len > 0);
    try testing.expect(table.columnsHeaderIndexBuf.len > 0);
    try testing.expect(table.columnsHeaderBuf.len > 0);
    try testing.expect(table.timestampsBuf.len > 0);

    try testing.expectEqual(@as(usize, 1), table.bloomValuesShards.len);
    // wait, if "info" or "seq" is added, the bloom filters and values could be generated
    try testing.expect(table.bloomValuesShards[0].len > 0);

    try testing.expect(table.columnIdxs.count() > 0);
    try testing.expect(table.columnIDGen.keyIDs.count() > 0);

    try testing.expectEqual(memTable.streamWriter.indexDst.asSliceAssumeBuffer().len, table.indexBuf.len);
    try testing.expectEqual(memTable.streamWriter.columnsHeaderIndexDst.asSliceAssumeBuffer().len, table.columnsHeaderIndexBuf.len);
    try testing.expectEqual(memTable.streamWriter.columnsHeaderDst.asSliceAssumeBuffer().len, table.columnsHeaderBuf.len);
    try testing.expectEqual(memTable.streamWriter.timestampsDst.asSliceAssumeBuffer().len, table.timestampsBuf.len);
}

test "open reads table from disk" {
    const alloc = testing.allocator;

    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    const rootPath = try tmp.dir.realpathAlloc(alloc, ".");
    defer alloc.free(rootPath);
    const tablePath = try std.fs.path.join(alloc, &.{ rootPath, "table-1" });
    defer alloc.free(tablePath);

    const memTable = try MemTable.init(alloc);
    defer memTable.deinit(alloc);

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
        .sid = .{ .id = 1, .tenantID = "1234" },
        .fields = fields1[0..],
    };
    const line2 = Line{
        .timestampNs = 2,
        .sid = .{ .id = 1, .tenantID = "1234" },
        .fields = fields2[0..],
    };

    var lines = [_]Line{ line1, line2 };
    try memTable.addLines(alloc, lines[0..]);
    try memTable.storeToDisk(alloc, tablePath);

    const tablePathOwned = try alloc.dupe(u8, tablePath);
    const table = try Table.open(alloc, tablePathOwned);
    defer table.release();

    try testing.expect(table.disk != null);
    try testing.expectEqual(memTable.streamWriter.indexDst.asSliceAssumeBuffer().len, table.indexBuf.len);
    try testing.expectEqualSlices(u8, memTable.streamWriter.indexDst.asSliceAssumeBuffer(), table.indexBuf);
    try testing.expectEqualSlices(u8, memTable.streamWriter.columnsHeaderIndexDst.asSliceAssumeBuffer(), table.columnsHeaderIndexBuf);
    try testing.expectEqualSlices(u8, memTable.streamWriter.columnsHeaderDst.asSliceAssumeBuffer(), table.columnsHeaderBuf);
    try testing.expectEqualSlices(u8, memTable.streamWriter.timestampsDst.asSliceAssumeBuffer(), table.timestampsBuf);
    try testing.expectEqualSlices(u8, memTable.streamWriter.messageBloomTokensDst.asSliceAssumeBuffer(), table.messageBloomTokens);
    try testing.expectEqualSlices(u8, memTable.streamWriter.messageBloomValuesDst.asSliceAssumeBuffer(), table.messageBloomValues);

    try testing.expectEqual(
        memTable.tableHeader.bloomValuesBuffersAmount,
        table.tableHeader.bloomValuesBuffersAmount,
    );
    try testing.expectEqual(@as(usize, memTable.streamWriter.bloomValuesList.items.len), table.bloomValuesShards.len);
    for (table.bloomValuesShards, 0..) |shard, i| {
        try testing.expectEqualSlices(u8, memTable.streamWriter.bloomTokensList.items[i].asSliceAssumeBuffer(), table.bloomTokensShards[i]);
        try testing.expectEqualSlices(u8, memTable.streamWriter.bloomValuesList.items[i].asSliceAssumeBuffer(), shard);
    }

    try testing.expect(table.columnIDGen.keyIDs.count() > 0);
    try testing.expect(table.columnIdxs.count() > 0);

    const expectedHeaders = try IndexBlockHeader.readIndexBlockHeaders(
        alloc,
        memTable.streamWriter.metaIndexDst.asSliceAssumeBuffer(),
    );
    defer {
        for (expectedHeaders) |*hdr| hdr.deinitRead(alloc);
        if (expectedHeaders.len > 0) alloc.free(expectedHeaders);
    }
    try testing.expectEqual(expectedHeaders.len, table.indexBlockHeaders.len);
    for (expectedHeaders, table.indexBlockHeaders) |expected, actual| {
        try testing.expectEqualDeep(expected, actual);
    }
}

// TODO: test flushed mem table is the same as an opened one
// and do the same with index tables (probably already done)
