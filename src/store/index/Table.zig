const std = @import("std");
const builtin = @import("builtin");
const Allocator = std.mem.Allocator;
const Io = std.Io;
const Dir = Io.Dir;

const filenames = @import("../../filenames.zig");
const Logger = @import("logging");
const fs = @import("../../fs.zig");
const TableHeader = @import("TableHeader.zig");
const MemTable = @import("MemTable.zig");
const DiskTable = @import("DiskTable.zig");
const MetaIndex = @import("MetaIndex.zig");
const BlockWriter = @import("BlockWriter.zig");
const MemBlock = @import("MemBlock.zig");
const CompressionPool = @import("../CompressionPool.zig").CompressionPool;
const DecompressionPool = @import("../CompressionPool.zig").DecompressionPool;

const catalog = @import("../table/catalog.zig");

const InnerTag = enum { mem, disk };
const Inner = union(InnerTag) {
    mem: *MemTable,
    disk: *DiskTable,
};

const Table = @This();

inner: Inner,

// fields for all the tables
metaIndexRecords: []MetaIndex,
size: u64,
path: []const u8,

// holds ownership,
// it's necessary in order to support ref counter
alloc: Allocator,

// state

// inMerge defines whether the table is taken by a merge job
// TODO: maybe do it an atomic flag not to lock during the merges
inMerge: bool = false,
// toRemove defines if the table must be removed on releasing,
// we do it via a flag instead of a direct removal,
// because a table could be retained in a reader
toRemove: std.atomic.Value(bool) = .init(false),
// refCounter follows how many clients open a table,
// first time it's open on start up,
// then readers can retain it
refCounter: std.atomic.Value(u32),

pub fn openAll(io: Io, parentAlloc: Allocator, path: []const u8, decompressionPool: anytype) !std.ArrayList(*Table) {
    Dir.createDirAbsolute(io, path, .default_dir) catch |err| switch (err) {
        // TODO: if the foler already exists we must read it's content and log an error
        // in case the tables on the disk are missing in the tables list
        error.PathAlreadyExists => {},
        else => std.debug.panic(
            "failed to create a table dir '{s}': {s}",
            .{ path, @errorName(err) },
        ),
    };

    var fba = std.heap.stackFallback(2048, parentAlloc);
    const alloc = fba.get();

    // read table names,
    // they are given either from a file or listed directories in the path
    const tablesFilePath = try std.fs.path.join(alloc, &[_][]const u8{ path, filenames.tables });
    defer alloc.free(tablesFilePath);
    var tableNames = try catalog.readNames(io, alloc, tablesFilePath, true);
    defer {
        for (tableNames.items) |name| alloc.free(name);
        tableNames.deinit(alloc);
    }

    // syncing tables with a json, make sure all the listed dirs exist
    try catalog.validateTablesExist(io, alloc, path, tableNames.items);

    // syncing tables with the given names remove all the not listed dirs
    try catalog.removeUnusedTables(io, alloc, path, tableNames.items);

    // open tables
    var tables = try std.ArrayList(*Table).initCapacity(parentAlloc, tableNames.items.len);
    errdefer {
        tables.deinit(parentAlloc);
    }
    for (tableNames.items) |tableName| {
        // don't clean tablePath, Table owns it
        const tablePath = try std.fs.path.join(parentAlloc, &.{ path, tableName });
        errdefer parentAlloc.free(tablePath);
        const table = try Table.open(io, parentAlloc, tablePath, decompressionPool);
        tables.appendAssumeCapacity(table);
    }

    // fsync after opening tables because it creates the files
    try fs.syncPathAndParentDir(io, path);

    return tables;
}

pub fn open(io: Io, alloc: Allocator, path: []const u8, decompressionPool: anytype) !*Table {
    var parsedTableHeader = try TableHeader.readFile(io, alloc, path);
    errdefer parsedTableHeader.deinit(alloc);

    const decodedMetaindex = try MetaIndex.readFileWithCompressionPool(io, alloc, decompressionPool, path, parsedTableHeader.blocksCount);
    errdefer if (decodedMetaindex.records.len > 0) alloc.free(decodedMetaindex.records);

    // TODO: open files in parallel to speed up work on high-latency storages, e.g. Ceph
    var pathBuf: [std.fs.max_path_bytes]u8 = undefined;
    var pathWriter = std.Io.Writer.fixed(&pathBuf);

    try std.fs.path.fmtJoin(&.{ path, filenames.index }).format(&pathWriter);
    var indexFile = try Dir.openFileAbsolute(io, pathWriter.buffered(), .{});
    errdefer indexFile.close(io);
    const indexSize = (try indexFile.stat(io)).size;
    pathWriter.end = 0;

    try std.fs.path.fmtJoin(&.{ path, filenames.entries }).format(&pathWriter);
    var entriesFile = try Dir.openFileAbsolute(io, pathWriter.buffered(), .{});
    errdefer entriesFile.close(io);
    const entriesSize = (try entriesFile.stat(io)).size;
    pathWriter.end = 0;

    try std.fs.path.fmtJoin(&.{ path, filenames.lens }).format(&pathWriter);
    var lensFile = try Dir.openFileAbsolute(io, pathWriter.buffered(), .{});
    errdefer lensFile.close(io);
    const lensSize = (try lensFile.stat(io)).size;

    const disk = try alloc.create(DiskTable);
    errdefer alloc.destroy(disk);
    disk.* = .{
        .tableHeader = parsedTableHeader,
        .indexFile = indexFile,
        .entriesFile = entriesFile,
        .lensFile = lensFile,
    };

    const table = try alloc.create(Table);

    Logger.log(.debug, "index.Table: open disk table", .{
        .path = path,
        .metaindexRecordsSize = decodedMetaindex.compressedSize,
        .metaindexRecordsLen = decodedMetaindex.records.len,
        .indexStatsSize = indexSize,
        .entriesStatsSize = entriesSize,
        .lensStatsSize = lensSize,
    });

    table.* = .{
        .inner = .{ .disk = disk },
        .size = decodedMetaindex.compressedSize + indexSize + entriesSize + lensSize,
        .path = path,
        .metaIndexRecords = decodedMetaindex.records,
        .refCounter = .init(1),
        .alloc = alloc,
    };

    return table;
}

pub fn close(self: *Table, io: Io) void {
    switch (self.inner) {
        .disk => |disk| disk.deinit(io, self.alloc),
        .mem => |mem| mem.deinit(self.alloc),
    }

    for (self.metaIndexRecords) |*rec| rec.deinit(self.alloc);
    if (self.metaIndexRecords.len > 0) self.alloc.free(self.metaIndexRecords);

    const shouldRemove = self.inner == .disk and self.toRemove.load(.acquire);
    if (shouldRemove) {
        // TODO: replace to an error log
        fs.deleteTreeAbsolute(io, self.path) catch |err| {
            std.debug.panic("failed to delete table '{s}': {s}", .{ self.path, @errorName(err) });
        };
    }

    if (self.path.len > 0) {
        self.alloc.free(self.path);
    }

    self.alloc.destroy(self);
}

pub fn fromMem(io: Io, alloc: Allocator, memTable: *MemTable, decompressionPool: anytype) !*Table {
    var decodedMetaindex: MetaIndex.DecodedMetaIndex = .{
        .records = &.{},
        .compressedSize = 0,
    };
    // TODO: we must generate a real table for this use case,
    // but it requires moving all the stub models generation from all the test to a designated package/file,
    // then we can remove this dumb condition
    if (!builtin.is_test or memTable.metaindexBuf.items.len > 0) {
        decodedMetaindex = try MetaIndex.decodeDecompressWithCompressionPool(
            io,
            alloc,
            decompressionPool,
            memTable.metaindexBuf.items,
            memTable.tableHeader.blocksCount,
        );
    }
    errdefer if (decodedMetaindex.records.len > 0) alloc.free(decodedMetaindex.records);

    const table = try alloc.create(Table);

    table.* = .{
        .inner = .{ .mem = memTable },
        .size = memTable.size(),
        .path = "",
        .metaIndexRecords = decodedMetaindex.records,
        .refCounter = .init(1),
        .alloc = alloc,
    };

    return table;
}

pub fn tableHeader(self: *const Table) TableHeader {
    switch (self.inner) {
        .disk => |disk| return disk.tableHeader,
        .mem => |mem| return mem.tableHeader,
    }
}

pub fn readLens(self: *const Table, io: Io, buf: []u8, offset: u64) !usize {
    switch (self.inner) {
        .disk => |disk| return readFile(disk.lensFile, io, buf, offset),
        .mem => |mem| return readBuf(buf, mem.lensBuf.items, offset),
    }
}

pub fn readEntries(self: *const Table, io: Io, buf: []u8, offset: u64) !usize {
    switch (self.inner) {
        .disk => |disk| return readFile(disk.entriesFile, io, buf, offset),
        .mem => |mem| return readBuf(buf, mem.entriesBuf.items, offset),
    }
}

pub fn readIndex(self: *const Table, io: Io, buf: []u8, offset: u64) !usize {
    switch (self.inner) {
        .disk => |disk| return readFile(disk.indexFile, io, buf, offset),
        .mem => |mem| return readBuf(buf, mem.indexBuf.items, offset),
    }
}

fn readFile(file: Io.File, io: Io, buf: []u8, offset: u64) !usize {
    const n = try file.readPositionalAll(io, buf, offset);
    return n;
}

fn readBuf(dst: []u8, src: []const u8, offset: u64) !usize {
    const start: usize = @intCast(offset);
    if (start >= src.len) return 0;
    const n = @min(dst.len, src.len - start);
    @memcpy(dst[0..n], src[start .. start + n]);
    return n;
}

pub fn lessThan(_: void, one: *Table, another: *Table) bool {
    return one.size < another.size;
}

pub fn writeNames(io: Io, alloc: Allocator, path: []const u8, tables: []*Table) anyerror!void {
    var tableNames = try std.ArrayList([]const u8).initCapacity(alloc, tables.len);
    defer tableNames.deinit(alloc);

    for (tables) |table| {
        if (table.inner != .disk) {
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

    try fs.writeBufferToFileAtomic(io, tablesFilePath, data, true);
}

pub fn retain(self: *Table) void {
    _ = self.refCounter.fetchAdd(1, .acquire);
}

pub fn release(self: *Table, io: Io) void {
    const prev = self.refCounter.fetchSub(1, .acq_rel);
    std.debug.assert(prev > 0);

    if (prev != 1) return;

    self.close(io);
}

const testing = std.testing;

fn createTestMemBlock(alloc: Allocator, items: []const []const u8) !*MemBlock {
    var total: u32 = 0;
    for (items) |item| total += @intCast(item.len);

    var block = try MemBlock.init(alloc, .{
        .maxMemBlockSize = total + 16,
        .blocksCountHint = items.len,
    });
    errdefer block.deinit(alloc);
    for (items) |item| {
        const ok = block.add(item);
        try testing.expect(ok);
    }
    block.sortData();
    return block;
}

fn createTestTableDir(io: Io, alloc: Allocator, tablePath: []const u8) !void {
    const compressionPool = try CompressionPool.init(alloc, 1);
    defer compressionPool.deinit(alloc);

    const items = [_][]const u8{ "alpha", "beta", "omega" };
    var block = try createTestMemBlock(alloc, &items);
    defer block.deinit(alloc);

    var writer = try BlockWriter.initFromDiskTableWithCompressionPool(io, tablePath, true, compressionPool);
    defer writer.deinit(alloc);
    try writer.writeBlock(io, alloc, block);
    try writer.close(io, alloc);

    var header = TableHeader{
        .entriesCount = items.len,
        .blocksCount = 1,
        .firstEntry = items[0],
        .lastEntry = items[items.len - 1],
    };
    try header.writeFile(io, alloc, tablePath);
}

// TODO: bunch of tests/things is duplicated between data/index tables,
// there must be a way to make them more generic
test "release keeps table unless toRemove is set, then removes table dir" {
    const alloc = testing.allocator;
    const io = testing.io;

    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    const rootPath = try tmp.dir.realPathFileAlloc(io, ".", alloc);
    defer alloc.free(rootPath);
    const tablePath = try std.fs.path.join(alloc, &.{ rootPath, "table-1" });
    defer alloc.free(tablePath);

    const compressionPool = try CompressionPool.init(alloc, 1);
    defer compressionPool.deinit(alloc);
    try createTestTableDir(io, alloc, tablePath);

    const table1Path = try alloc.dupe(u8, tablePath);
    const table1 = try Table.open(io, alloc, table1Path, compressionPool);
    table1.release(io);
    try Dir.accessAbsolute(io, tablePath, .{});

    const table2Path = try alloc.dupe(u8, tablePath);
    const table2 = try Table.open(io, alloc, table2Path, compressionPool);
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
    const sentinelPath = try std.fs.path.join(alloc, &.{ rootPath, "sentinel" });
    defer alloc.free(sentinelPath);
    // create a real directory to verify it remains
    try testing.expectError(error.FileNotFound, Dir.accessAbsolute(io, sentinelPath, .{}));
    try Dir.createDirAbsolute(io, sentinelPath, .default_dir);

    const compressionPool = try CompressionPool.init(alloc, 1);
    defer compressionPool.deinit(alloc);
    const memTable = try MemTable.empty(alloc);

    const table = try Table.fromMem(io, alloc, memTable, compressionPool);
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

test "fromMem creates proper table from mem table with populated data" {
    const alloc = testing.allocator;
    const io = testing.io;
    const compressionPool = try CompressionPool.init(alloc, 1);
    defer compressionPool.deinit(alloc);

    const items = [_][]const u8{ "item-c", "item-a", "item-b" };
    const block = try createTestMemBlock(alloc, &items);

    var blocks = [_]*MemBlock{block};
    const memTable = try MemTable.init(io, alloc, blocks[0..], compressionPool);

    const table = try Table.fromMem(io, alloc, memTable, compressionPool);
    defer table.release(io);

    try testing.expect(table.inner == .mem);
    var buf: [64]u8 = undefined;
    var i: usize = 0;

    i = try table.readLens(io, &buf, 0);
    try testing.expect(i > 0);
    try testing.expectEqualSlices(u8, memTable.lensBuf.items, buf[0..i]);
    i = try table.readEntries(io, &buf, 0);
    try testing.expect(i > 0);
    try testing.expectEqualSlices(u8, memTable.entriesBuf.items, buf[0..i]);
    i = try table.readIndex(io, &buf, 0);
    try testing.expect(i > 0);
    try testing.expectEqualSlices(u8, memTable.indexBuf.items, buf[0..i]);

    const expectedMetaindex = try MetaIndex.decodeDecompressWithCompressionPool(
        std.testing.io,
        alloc,
        compressionPool,
        memTable.metaindexBuf.items,
        memTable.tableHeader.blocksCount,
    );
    defer {
        for (expectedMetaindex.records) |*rec| rec.deinit(alloc);
        if (expectedMetaindex.records.len > 0) alloc.free(expectedMetaindex.records);
    }

    try testing.expectEqualDeep(expectedMetaindex.records, table.metaIndexRecords);
}

test "open reads table from disk" {
    const alloc = testing.allocator;
    const io = testing.io;

    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    const rootPath = try tmp.dir.realPathFileAlloc(io, ".", alloc);
    defer alloc.free(rootPath);
    const tablePath = try std.fs.path.join(alloc, &.{ rootPath, "table-1" });
    defer alloc.free(tablePath);

    const compressionPool = try CompressionPool.init(alloc, 1);
    defer compressionPool.deinit(alloc);
    try createTestTableDir(io, alloc, tablePath);

    const tablePathOwned = try alloc.dupe(u8, tablePath);
    const table = try Table.open(io, alloc, tablePathOwned, compressionPool);
    defer table.release(io);

    try testing.expect(table.inner == .disk);

    const expectedIndex = try BlockWriter.readTableFile(io, alloc, tablePath, filenames.index);
    defer alloc.free(expectedIndex);
    const expectedEntries = try BlockWriter.readTableFile(io, alloc, tablePath, filenames.entries);
    defer alloc.free(expectedEntries);
    const expectedLens = try BlockWriter.readTableFile(io, alloc, tablePath, filenames.lens);
    defer alloc.free(expectedLens);
    const expectedMetaindexCompressed = try BlockWriter.readTableFile(io, alloc, tablePath, filenames.metaindex);
    defer alloc.free(expectedMetaindexCompressed);

    var buf = try alloc.alloc(u8, @max(expectedIndex.len, @max(expectedEntries.len, expectedLens.len)));
    defer alloc.free(buf);

    var n = try table.readIndex(io, buf[0..expectedIndex.len], 0);
    try testing.expectEqual(expectedIndex.len, n);
    try testing.expectEqualSlices(u8, expectedIndex, buf[0..n]);
    n = try table.readEntries(io, buf[0..expectedEntries.len], 0);
    try testing.expectEqual(expectedEntries.len, n);
    try testing.expectEqualSlices(u8, expectedEntries, buf[0..n]);
    n = try table.readLens(io, buf[0..expectedLens.len], 0);
    try testing.expectEqual(expectedLens.len, n);
    try testing.expectEqualSlices(u8, expectedLens, buf[0..n]);

    const expectedMetaindex = try MetaIndex.decodeDecompressWithCompressionPool(
        io,
        alloc,
        compressionPool,
        expectedMetaindexCompressed,
        table.tableHeader().blocksCount,
    );
    defer {
        for (expectedMetaindex.records) |*rec| rec.deinit(alloc);
        if (expectedMetaindex.records.len > 0) alloc.free(expectedMetaindex.records);
    }

    try testing.expectEqualDeep(expectedMetaindex.records, table.metaIndexRecords);

    const expectedSize: u64 = expectedMetaindexCompressed.len +
        expectedIndex.len + expectedEntries.len + expectedLens.len;
    try testing.expectEqual(expectedSize, table.size);
}

test "openAll frees catalog table names on error" {
    try testing.checkAllAllocationFailures(testing.allocator, testOpenAllFreesCatalogTableNamesOnError, .{testing.io});
}

fn testOpenAllFreesCatalogTableNamesOnError(alloc: Allocator, io: Io) !void {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    const rootPath = try tmp.dir.realPathFileAlloc(io, ".", alloc);
    defer alloc.free(rootPath);

    const tablesFilePath = try std.fs.path.join(alloc, &.{ rootPath, filenames.tables });
    defer alloc.free(tablesFilePath);
    try fs.writeBufferToFileAtomic(io, tablesFilePath, "[\"table-a\",\"table-b\"]", true);

    const compressionPool = try CompressionPool.init(alloc, 1);
    defer compressionPool.deinit(alloc);
    const res = Table.openAll(io, alloc, rootPath, compressionPool);
    try testing.expectError(error.TableDoesNotExist, res);
}

test "openAll frees spilled catalog table names on error" {
    const alloc = testing.allocator;
    const io = testing.io;

    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    const rootPath = try tmp.dir.realPathFileAlloc(io, ".", alloc);
    defer alloc.free(rootPath);

    const tablesFilePath = try std.fs.path.join(alloc, &.{ rootPath, filenames.tables });
    defer alloc.free(tablesFilePath);

    const longName = "table" ** 10;

    var content = try std.ArrayList(u8).initCapacity(alloc, 4096);
    defer content.deinit(alloc);
    try content.append(alloc, '[');
    for (0..5) |i| {
        if (i > 0) try content.append(alloc, ',');
        try content.append(alloc, '"');
        try content.appendSlice(alloc, longName);
        try content.append(alloc, '"');
    }
    try content.append(alloc, ']');

    try fs.writeBufferToFileAtomic(io, tablesFilePath, content.items, true);

    const compressionPool = try CompressionPool.init(alloc, 1);
    defer compressionPool.deinit(alloc);
    try testing.expectError(error.TableDoesNotExist, Table.openAll(io, alloc, rootPath, compressionPool));
}
