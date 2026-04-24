const std = @import("std");
const builtin = @import("builtin");
const Allocator = std.mem.Allocator;
const Io = std.Io;
const Dir = Io.Dir;

const filenames = @import("../../filenames.zig");
const fs = @import("../../fs.zig");
const TableHeader = @import("TableHeader.zig");
const MemTable = @import("MemTable.zig");
const DiskTable = @import("DiskTable.zig");
const MetaIndex = @import("MetaIndex.zig");
const BlockWriter = @import("BlockWriter.zig");
const MemBlock = @import("MemBlock.zig");

const catalog = @import("../table/catalog.zig");

const Table = @This();

// either one has to be available,
// NOTE: adding a third one like object table may complicated it,
// so then it would require implement at able as an interface
mem: ?*MemTable,
disk: ?*DiskTable,

// fields for all the tables
metaindexRecords: []MetaIndex,
tableHeader: *TableHeader,
size: u64,
path: []const u8,

indexBuf: []u8,
entriesBuf: []u8,
lensBuf: []u8,

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

pub fn openAll(io: Io, parentAlloc: Allocator, path: []const u8) !std.ArrayList(*Table) {
    Dir.createDirAbsolute(io, path, .default_dir) catch |err| switch (err) {
        // TODO: if the foler already exists we must read it's content and log an error
        // in case the tables on the disk are missing in the tables list
        error.PathAlreadyExists => {},
        else => std.debug.panic(
            "failed to create a table dir '{s}': {s}",
            .{ path, @errorName(err) },
        ),
    };

    // fsync after opening tables because it creates the files
    defer fs.syncPathAndParentDir(io, path);

    var fba = std.heap.stackFallback(2048, parentAlloc);
    const alloc = fba.get();

    // read table names,
    // they are given either from a file or listed directories in the path
    const tablesFilePath = try std.fs.path.join(alloc, &[_][]const u8{ path, filenames.tables });
    defer alloc.free(tablesFilePath);
    var tableNames = try catalog.readNames(io, alloc, tablesFilePath, true);
    defer tableNames.deinit(alloc);

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

pub fn open(io: Io, alloc: Allocator, path: []const u8) !*Table {
    var fba = std.heap.stackFallback(512, alloc);
    const fbaAlloc = fba.get();

    var tableHeader = try TableHeader.readFile(alloc, path);
    errdefer tableHeader.deinit(alloc);

    const decodedMetaindex = try MetaIndex.readFile(alloc, path, tableHeader.blocksCount);
    errdefer if (decodedMetaindex.records.len > 0) alloc.free(decodedMetaindex.records);

    // TODO: open files in parallel to speed up work on high-latency storages, e.g. Ceph
    const indexPath = try std.fs.path.join(fbaAlloc, &.{ path, filenames.index });
    defer fbaAlloc.free(indexPath);
    const entriesPath = try std.fs.path.join(fbaAlloc, &.{ path, filenames.entries });
    defer fbaAlloc.free(entriesPath);
    const lensPath = try std.fs.path.join(fbaAlloc, &.{ path, filenames.lens });
    defer fbaAlloc.free(lensPath);

    var indexFile = try Dir.openFileAbsolute(io, indexPath, .{});
    errdefer indexFile.close();
    const indexSize = (try indexFile.stat()).size;

    var entriesFile = try Dir.openFileAbsolute(io, entriesPath, .{});
    errdefer entriesFile.close();
    const entriesSize = (try entriesFile.stat()).size;

    var lensFile = try Dir.openFileAbsolute(io, lensPath, .{});
    errdefer lensFile.close();
    const lensSize = (try lensFile.stat()).size;
    const disk = try alloc.create(DiskTable);
    errdefer alloc.destroy(disk);
    disk.* = .{
        .tableHeader = tableHeader,
        .indexFile = indexFile,
        .entriesFile = entriesFile,
        .lensFile = lensFile,
    };

    // TODO: this is complete garbage, we must use real reader API here
    const table = try alloc.create(Table);
    const indexBuf = try fs.readAll(alloc, indexPath);
    errdefer alloc.free(indexBuf);
    const entriesBuf = try fs.readAll(alloc, entriesPath);
    errdefer alloc.free(entriesBuf);
    const lensBuf = try fs.readAll(alloc, lensPath);
    errdefer alloc.free(lensBuf);

    table.* = .{
        .mem = null,
        .disk = disk,
        .size = decodedMetaindex.compressedSize + indexSize + entriesSize + lensSize,
        .path = path,
        .metaindexRecords = decodedMetaindex.records,
        .tableHeader = &table.disk.?.tableHeader,
        .refCounter = .init(1),
        .alloc = alloc,

        .indexBuf = indexBuf,
        .entriesBuf = entriesBuf,
        .lensBuf = lensBuf,
    };

    return table;
}

pub fn close(self: *Table) void {
    if (self.disk) |disk| {
        self.alloc.free(self.entriesBuf);
        self.alloc.free(self.lensBuf);
        self.alloc.free(self.indexBuf);

        disk.deinit(self.alloc);
    }
    if (self.mem) |mem| {
        mem.deinit(self.alloc);
    }

    for (self.metaindexRecords) |*rec| rec.deinit(self.alloc);
    if (self.metaindexRecords.len > 0) self.alloc.free(self.metaindexRecords);

    const shouldRemove = self.disk != null and self.toRemove.load(.acquire);
    if (shouldRemove) {
        // TODO: replace to an error log
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
    var decodedMetaindex: MetaIndex.DecodedMetaIndex = .{
        .records = &.{},
        .compressedSize = undefined,
    };
    // TODO: we must generate a real table for this use case,
    // but it requires an moving all the stub models generation from all the test to a designated package/file,
    // then we can remove this dumb condition
    if (!builtin.is_test or memTable.metaindexBuf.items.len > 0) {
        decodedMetaindex = try MetaIndex.decodeDecompress(
            alloc,
            memTable.metaindexBuf.items,
            memTable.tableHeader.blocksCount,
        );
    }
    errdefer if (decodedMetaindex.records.len > 0) alloc.free(decodedMetaindex.records);

    const table = try alloc.create(Table);

    table.* = .{
        .mem = memTable,
        .disk = null,
        .size = memTable.size(),
        .path = "",
        .metaindexRecords = decodedMetaindex.records,
        .tableHeader = &memTable.tableHeader,
        .refCounter = .init(1),
        .alloc = alloc,

        .entriesBuf = memTable.entriesBuf.items,
        .lensBuf = memTable.lensBuf.items,
        .indexBuf = memTable.indexBuf.items,
    };

    return table;
}

pub fn lessThan(_: void, one: *Table, another: *Table) bool {
    return one.size < another.size;
}

pub fn writeNames(alloc: Allocator, path: []const u8, tables: []*Table) anyerror!void {
    var tableNames = try std.ArrayList([]const u8).initCapacity(alloc, tables.len);
    defer tableNames.deinit(alloc);

    for (tables) |table| {
        if (table.disk == null) {
            // collect only disk table names
            continue;
        }
        tableNames.appendAssumeCapacity(std.fs.path.basename(table.path));
    }

    var stackFba = std.heap.stackFallback(512, alloc);
    const fba = stackFba.get();
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

const testing = std.testing;

fn createTestMemBlock(alloc: Allocator, items: []const []const u8) !*MemBlock {
    var total: u32 = 0;
    for (items) |item| total += @intCast(item.len);

    var block = try MemBlock.init(alloc, total + 16);
    errdefer block.deinit(alloc);
    for (items) |item| {
        const ok = block.add(item);
        try testing.expect(ok);
    }
    block.sortData();
    return block;
}

fn createTestTableDir(alloc: Allocator, tablePath: []const u8) !void {
    const items = [_][]const u8{ "alpha", "beta", "omega" };
    var block = try createTestMemBlock(alloc, &items);
    defer block.deinit(alloc);

    var writer = try BlockWriter.initFromDiskTable(alloc, tablePath, true);
    defer writer.deinit(alloc);
    try writer.writeBlock(alloc, block);
    try writer.close(alloc);

    var header = TableHeader{
        .entriesCount = items.len,
        .blocksCount = 1,
        .firstEntry = items[0],
        .lastEntry = items[items.len - 1],
    };
    try header.writeFile(alloc, tablePath);
}

fn readTestTableFile(alloc: Allocator, tablePath: []const u8, fileName: []const u8) ![]u8 {
    const filePath = try std.fs.path.join(alloc, &.{ tablePath, fileName });
    defer alloc.free(filePath);
    return fs.readAll(alloc, filePath);
}

test "release keeps table unless toRemove is set, then removes table dir" {
    const alloc = testing.allocator;
    const io = testing.io;

    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    const rootPath = try tmp.dir.realPathFileAlloc(io, alloc, ".");
    defer alloc.free(rootPath);
    const tablePath = try std.fs.path.join(alloc, &.{ rootPath, "table-1" });
    defer alloc.free(tablePath);

    try createTestTableDir(alloc, tablePath);

    const table1Path = try alloc.dupe(u8, tablePath);
    const table1 = try Table.open(alloc, table1Path);
    table1.release();
    try Dir.accessAbsolute(io, tablePath, .{});

    const table2Path = try alloc.dupe(u8, tablePath);
    const table2 = try Table.open(alloc, table2Path);
    table2.toRemove.store(true, .release);
    table2.release();
    try testing.expectError(error.FileNotFound, Dir.accessAbsolute(io, tablePath, .{}));
}

test "release fromMem does not affect filesystem path" {
    const alloc = testing.allocator;
    const io = testing.io;

    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    const rootPath = try tmp.dir.realPathFileAlloc(io, alloc, ".");
    defer alloc.free(rootPath);
    const sentinelPath = try std.fs.path.join(alloc, &.{ rootPath, "sentinel" });
    defer alloc.free(sentinelPath);
    // create a real directory to verify it remains
    try testing.expectError(error.FileNotFound, Dir.accessAbsolute(io, sentinelPath, .{}));
    try Dir.createDirAbsolute(io, sentinelPath, .default_dir);

    const memTable = try MemTable.empty(alloc);

    const table = try Table.fromMem(alloc, memTable);
    // eve if set we expect it to keep the created path when disk == null,
    // so close must not delete it
    table.toRemove.store(true, .release);
    // we expected only second release close cleans the table, otherwise it's a memory leak
    table.retain();

    try Dir.accessAbsolute(io, sentinelPath, .{});
    table.release();
    try Dir.accessAbsolute(io, sentinelPath, .{});
    table.release();
    try Dir.accessAbsolute(io, sentinelPath, .{});
}

test "fromMem creates proper table from mem table with populated data" {
    const alloc = testing.allocator;
    const io = testing.io;

    const items = [_][]const u8{ "item-c", "item-a", "item-b" };
    var block = try createTestMemBlock(alloc, &items);
    defer block.deinit(alloc);

    var blocks = [_]*MemBlock{block};
    const memTable = try MemTable.init(io, alloc, blocks[0..]);

    const table = try Table.fromMem(alloc, memTable);
    defer table.release();

    try testing.expect(table.mem != null);
    try testing.expect(table.disk == null);
    try testing.expect(table.entriesBuf.len > 0);
    try testing.expect(table.lensBuf.len > 0);
    try testing.expect(table.indexBuf.len > 0);

    try testing.expectEqual(memTable.size(), table.size);
    try testing.expectEqualSlices(u8, memTable.entriesBuf.items, table.entriesBuf);
    try testing.expectEqualSlices(u8, memTable.lensBuf.items, table.lensBuf);
    try testing.expectEqualSlices(u8, memTable.indexBuf.items, table.indexBuf);

    const expectedMetaindex = try MetaIndex.decodeDecompress(
        alloc,
        memTable.metaindexBuf.items,
        memTable.tableHeader.blocksCount,
    );
    defer {
        for (expectedMetaindex.records) |*rec| rec.deinit(alloc);
        if (expectedMetaindex.records.len > 0) alloc.free(expectedMetaindex.records);
    }

    try testing.expectEqualDeep(expectedMetaindex.records, table.metaindexRecords);
}

test "open reads table from disk" {
    const alloc = testing.allocator;
    const io = testing.io;

    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    const rootPath = try tmp.dir.realPathFileAlloc(io, alloc, ".");
    defer alloc.free(rootPath);
    const tablePath = try std.fs.path.join(alloc, &.{ rootPath, "table-1" });
    defer alloc.free(tablePath);

    try createTestTableDir(alloc, tablePath);

    const tablePathOwned = try alloc.dupe(u8, tablePath);
    const table = try Table.open(alloc, tablePathOwned);
    defer table.release();

    try testing.expect(table.disk != null);

    const expectedIndex = try readTestTableFile(alloc, tablePath, filenames.index);
    defer alloc.free(expectedIndex);
    const expectedEntries = try readTestTableFile(alloc, tablePath, filenames.entries);
    defer alloc.free(expectedEntries);
    const expectedLens = try readTestTableFile(alloc, tablePath, filenames.lens);
    defer alloc.free(expectedLens);
    const expectedMetaindexCompressed = try readTestTableFile(alloc, tablePath, filenames.metaindex);
    defer alloc.free(expectedMetaindexCompressed);

    try testing.expectEqualSlices(u8, expectedIndex, table.indexBuf);
    try testing.expectEqualSlices(u8, expectedEntries, table.entriesBuf);
    try testing.expectEqualSlices(u8, expectedLens, table.lensBuf);

    const expectedMetaindex = try MetaIndex.decodeDecompress(
        alloc,
        expectedMetaindexCompressed,
        table.tableHeader.blocksCount,
    );
    defer {
        for (expectedMetaindex.records) |*rec| rec.deinit(alloc);
        if (expectedMetaindex.records.len > 0) alloc.free(expectedMetaindex.records);
    }

    try testing.expectEqualDeep(expectedMetaindex.records, table.metaindexRecords);

    const expectedSize = @as(u64, expectedMetaindexCompressed.len + expectedIndex.len + expectedEntries.len + expectedLens.len);
    try testing.expectEqual(expectedSize, table.size);
}
