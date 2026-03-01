const std = @import("std");
const builtin = @import("builtin");
const Allocator = std.mem.Allocator;

const Filenames = @import("../../Filenames.zig");
const fs = @import("../../fs.zig");
const strings = @import("../../stds/strings.zig");
const TableHeader = @import("TableHeader.zig");
const MemTable = @import("MemTable.zig");
const DiskTable = @import("DiskTable.zig");
const MetaIndex = @import("MetaIndex.zig");
const BlockWriter = @import("BlockWriter.zig");
const MemBlock = @import("MemBlock.zig");

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

pub fn openAll(parentAlloc: Allocator, path: []const u8) !std.ArrayList(*Table) {
    std.fs.makeDirAbsolute(path) catch |err| switch (err) {
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
    const tablesFilePath = try std.fs.path.join(alloc, &[_][]const u8{ path, Filenames.tables });
    defer alloc.free(tablesFilePath);
    var tableNames = try readNames(alloc, tablesFilePath);
    defer tableNames.deinit(alloc);

    // syncing tables with a json, make sure all the listed dirs exist
    for (tableNames.items) |tableName| {
        const tablePath = try std.fs.path.join(alloc, &.{ path, tableName });
        defer alloc.free(tablePath);
        std.fs.accessAbsolute(tablePath, .{}) catch |err| switch (err) {
            error.FileNotFound => std.debug.panic(
                "table '{s}' is missing on disk, but listed in '{s}'\n" ++
                    "make sure the content is not corrupted, remove '{s}' from file '{s}' or restore the missing file",
                .{ tablePath, tablesFilePath, tablePath, tablesFilePath },
            ),
            else => return err,
        };
    }

    // syncing tables with a json, remove all the not listed dirs,
    var dir = try std.fs.cwd().openDir(path, .{ .iterate = true });
    defer dir.close();
    var it = dir.iterate();
    while (try it.next()) |entry| {
        if (entry.kind != .directory and entry.kind != .sym_link) continue;

        // TODO: benchmark against filling a map lookup
        if (strings.contains(tableNames.items, entry.name)) continue;

        const pathToDelete = try std.fs.path.join(alloc, &.{ path, entry.name });
        defer alloc.free(pathToDelete);
        std.debug.print("removing '{s}' file, sycning table dirs\n", .{pathToDelete});
        std.fs.deleteTreeAbsolute(pathToDelete) catch |err| std.debug.panic(
            "failed to remove unlisted table dir '{s}': '{s}'\n",
            .{ pathToDelete, @errorName(err) },
        );
    }

    // open tables
    var tables = try std.ArrayList(*Table).initCapacity(parentAlloc, tableNames.items.len);
    errdefer {
        // for (tables.items) |table| table.close(parentAlloc);
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
    var fba = std.heap.stackFallback(512, alloc);
    const fbaAlloc = fba.get();

    var tableHeader = try TableHeader.readFile(alloc, path);
    errdefer tableHeader.deinit(alloc);

    const decodedMetaindex = try MetaIndex.readFile(alloc, path, tableHeader.blocksCount);
    errdefer if (decodedMetaindex.records.len > 0) alloc.free(decodedMetaindex.records);

    // TODO: open files in parallel to speed up work on high-latency storages, e.g. Ceph
    const indexPath = try std.fs.path.join(fbaAlloc, &.{ path, Filenames.index });
    defer fbaAlloc.free(indexPath);
    const entriesPath = try std.fs.path.join(fbaAlloc, &.{ path, Filenames.entries });
    defer fbaAlloc.free(entriesPath);
    const lensPath = try std.fs.path.join(fbaAlloc, &.{ path, Filenames.lens });
    defer fbaAlloc.free(lensPath);

    var indexFile = try std.fs.openFileAbsolute(indexPath, .{});
    errdefer indexFile.close();
    const indexSize = (try indexFile.stat()).size;

    var entriesFile = try std.fs.openFileAbsolute(entriesPath, .{});
    errdefer entriesFile.close();
    const entriesSize = (try entriesFile.stat()).size;

    var lensFile = try std.fs.openFileAbsolute(lensPath, .{});
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
        .tableHeader = undefined,
        .refCounter = .init(1),
        .alloc = alloc,

        .indexBuf = indexBuf,
        .entriesBuf = entriesBuf,
        .lensBuf = lensBuf,
    };
    table.tableHeader = &table.disk.?.tableHeader;

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

// nothing specific, we simply don't expected a small json file to be larger than that
const maxFileBytes = 16 * 1024 * 1024;
fn readNames(alloc: Allocator, tablesFilePath: []const u8) !std.ArrayList([]const u8) {
    if (std.fs.cwd().openFile(tablesFilePath, .{})) |file| {
        defer file.close();

        const data = try file.readToEndAlloc(alloc, maxFileBytes);
        defer alloc.free(data);

        const parsed = try std.json.parseFromSlice(std.json.Value, alloc, data, .{});
        defer parsed.deinit();

        if (parsed.value != .array) {
            return error.TablesFileExpectedArray;
        }

        var tableNames = try std.ArrayList([]const u8).initCapacity(alloc, parsed.value.array.items.len);
        errdefer {
            for (tableNames.items) |name| alloc.free(name);
            tableNames.deinit(alloc);
        }
        for (parsed.value.array.items) |item| {
            if (item != .string) {
                return error.TablesFileExpectedStringItems;
            }
            const nameCopy = try alloc.dupe(u8, item.string);
            try tableNames.append(alloc, nameCopy);
        }

        return tableNames;
    } else |err| switch (err) {
        error.FileNotFound => {
            const f = try std.fs.createFileAbsolute(tablesFilePath, .{});
            defer f.close();
            try f.writeAll("[]");
            std.debug.print("write initial state to '{s}'\n", .{tablesFilePath});
            return .empty;
        },
        else => return err,
    }
}

pub fn writeNames(alloc: Allocator, path: []const u8, tables: []*Table) !void {
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

    const tablesFilePath = try std.fs.path.join(fba, &.{ path, Filenames.tables });
    defer fba.free(tablesFilePath);

    try fs.writeBufferToFileAtomic(alloc, tablesFilePath, data, true);
}

pub fn retain(self: *Table) void {
    _ = self.refCounter.fetchAdd(1, .acquire);
}

pub fn release(self: *Table) void {
    const prev = self.refCounter.fetchSub(1, .acq_rel);
    std.debug.assert(prev > 0);

    if (prev != 1) return;

    const shouldRemove = self.disk != null and self.toRemove.load(.acquire);
    if (shouldRemove) {
        // TODO: replace to an error log
        std.fs.deleteTreeAbsolute(self.path) catch |err| {
            std.debug.panic("failed to delete table '{s}': {s}", .{ self.path, @errorName(err) });
        };
    }
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
        .itemsCount = items.len,
        .blocksCount = 1,
        .firstItem = items[0],
        .lastItem = items[items.len - 1],
    };
    try header.writeFile(alloc, tablePath);
}

test "readNames" {
    const Case = struct {
        content: []const u8,
        expected: []const []const u8,
        expectedErr: ?anyerror = null,
    };

    const alloc = testing.allocator;
    const cases = [_]Case{
        .{
            .content = "[\"table-a\",\"table-b\"]",
            .expected = &.{ "table-a", "table-b" },
        },
        .{
            .content = "not-json",
            .expected = &.{},
            .expectedErr = error.SyntaxError,
        },
        .{
            .content = "{\"name\":\"table-a\"}",
            .expected = &.{},
            .expectedErr = error.TablesFileExpectedArray,
        },
        .{
            .content = "[\"table-a\",42]",
            .expected = &.{},
            .expectedErr = error.TablesFileExpectedStringItems,
        },
    };

    for (cases) |case| {
        var tmp = testing.tmpDir(.{});
        defer tmp.cleanup();

        const rootPath = try tmp.dir.realpathAlloc(alloc, ".");
        defer alloc.free(rootPath);
        const tablesFilePath = try std.fs.path.join(alloc, &.{ rootPath, "tables.json" });
        defer alloc.free(tablesFilePath);

        try fs.writeBufferToFileAtomic(alloc, tablesFilePath, case.content, true);

        if (case.expectedErr) |expectedErr| {
            try testing.expectError(expectedErr, readNames(alloc, tablesFilePath));
            continue;
        }

        var tableNames = try readNames(alloc, tablesFilePath);
        defer {
            for (tableNames.items) |name| alloc.free(name);
            tableNames.deinit(alloc);
        }
        try testing.expectEqual(case.expected.len, tableNames.items.len);
        for (case.expected, 0..) |expected, i| {
            try testing.expectEqualStrings(expected, tableNames.items[i]);
        }
    }
}

test "readNames creates empty file when missing" {
    const alloc = testing.allocator;

    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    const rootPath = try tmp.dir.realpathAlloc(alloc, ".");
    defer alloc.free(rootPath);
    const tablesFilePath = try std.fs.path.join(alloc, &.{ rootPath, "tables.json" });
    defer alloc.free(tablesFilePath);

    var tableNames = try readNames(alloc, tablesFilePath);
    defer tableNames.deinit(alloc);
    try testing.expectEqual(@as(usize, 0), tableNames.items.len);

    const data = try fs.readAll(alloc, tablesFilePath);
    defer alloc.free(data);
    try testing.expectEqualStrings("[]", data);
}

test "readNames returns error when missing parent path cannot be created" {
    const alloc = testing.allocator;

    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    const rootPath = try tmp.dir.realpathAlloc(alloc, ".");
    defer alloc.free(rootPath);
    const missingDirTablesPath = try std.fs.path.join(alloc, &.{ rootPath, "missing", "tables.json" });
    defer alloc.free(missingDirTablesPath);

    try testing.expectError(error.FileNotFound, readNames(alloc, missingDirTablesPath));
}

test "release keeps table unless toRemove is set, then removes table dir" {
    const alloc = testing.allocator;

    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    const rootPath = try tmp.dir.realpathAlloc(alloc, ".");
    defer alloc.free(rootPath);
    const tablePath = try std.fs.path.join(alloc, &.{ rootPath, "table-1" });
    defer alloc.free(tablePath);

    try createTestTableDir(alloc, tablePath);

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
    const missingPath = try std.fs.path.join(alloc, &.{ rootPath, "never-created" });
    defer alloc.free(missingPath);

    const memTable = try MemTable.empty(alloc);

    const table = try Table.fromMem(alloc, memTable);
    // we expected only second release close cleans the table, otherwise it's a memory leak
    table.retain();

    try testing.expectError(error.FileNotFound, std.fs.accessAbsolute(missingPath, .{}));
    table.release();
    try testing.expectError(error.FileNotFound, std.fs.accessAbsolute(missingPath, .{}));
    table.release();
    try testing.expectError(error.FileNotFound, std.fs.accessAbsolute(missingPath, .{}));
}
