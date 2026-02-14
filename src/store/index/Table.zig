const std = @import("std");
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
tableHeader: *TableHeader,
size: u64,
path: []const u8,

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
    std.fs.makeDirAbsolute(path) catch |err| {
        std.debug.panic(
            "failed to create a table dir '{s}': {s}",
            .{ path, @errorName(err) },
        );
    };

    // fsync after opening tables because it creates the files
    defer fs.syncPathAndParentDir(path);

    var fba = std.heap.stackFallback(2048, parentAlloc);
    const alloc = fba.get();

    // read table names,
    // they are given either from a file or listed directories in the path
    const tablesFilePath = try std.fs.path.join(alloc, &[_][]const u8{ path, Filenames.tables });
    const tableNames = try readNames(alloc, tablesFilePath);

    // syncing tables with a json, make sure all the listed dirs exist
    for (tableNames.items) |tableName| {
        const tablePath = try std.fs.path.join(alloc, &.{ path, tableName });
        defer alloc.free(tablePath);
        if (std.fs.cwd().openFile(tablesFilePath, .{})) |file| {
            file.close();
        } else |err| switch (err) {
            error.FileNotFound => std.debug.panic(
                "table '{s}' is missing on disk, but listed in '{s}'\n" ++
                    "make sure the content is not corrupted, remove '{s}' from file '{s}' or restore the missing file",
                .{ tablePath, tablesFilePath, tablePath, tablesFilePath },
            ),
            else => return err,
        }
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
        const tablePath = try std.fs.path.join(alloc, &.{ path, tableName });
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
        .metaindexRecords = decodedMetaindex.records,
        .indexFile = indexFile,
        .entriesFile = entriesFile,
        .lensFile = lensFile,
    };

    const table = try alloc.create(Table);
    table.* = .{
        .mem = null,
        .disk = disk,
        .size = decodedMetaindex.compressedSize + indexSize + entriesSize + lensSize,
        .path = path,
        .tableHeader = undefined,
        .refCounter = .init(1),
    };
    table.tableHeader = &table.disk.?.tableHeader;

    return table;
}

pub fn close(self: *Table, alloc: Allocator) void {
    // TODO: close files in parallel
    if (self.disk) |disk| {
        disk.indexFile.close();
        disk.entriesFile.close();
        disk.lensFile.close();
        disk.tableHeader.deinit(alloc);
        for (disk.metaindexRecords) |*rec| rec.deinit(alloc);
        if (disk.metaindexRecords.len > 0) alloc.free(disk.metaindexRecords);
        alloc.destroy(disk);
    }
    alloc.destroy(self);
}

pub fn fromMem(alloc: Allocator, memTable: *MemTable) !*Table {
    const table = try alloc.create(Table);
    table.* = .{
        .mem = memTable,
        .disk = null,
        .size = memTable.size(),
        .path = "",
        .tableHeader = &memTable.tableHeader,
        .refCounter = .init(1),
    };

    return table;
}

// nothing specific, we simply don't expected a small json file to be larger than that
const maxFileBytes = 16 * 1024 * 1024;
fn readNames(alloc: Allocator, tablesFilePath: []const u8) !std.ArrayList([]const u8) {
    if (std.fs.cwd().openFile(tablesFilePath, .{})) |file| {
        defer file.close();

        const data = file.readToEndAlloc(alloc, maxFileBytes) catch |err| {
            std.debug.panic("can't read tables file '{s}': {s}", .{ tablesFilePath, @errorName(err) });
        };
        defer alloc.free(data);

        const parsed = std.json.parseFromSlice(std.json.Value, alloc, data, .{}) catch |err| {
            std.debug.panic("can't parse tables file '{s}': {s}", .{ tablesFilePath, @errorName(err) });
        };
        defer parsed.deinit();

        if (parsed.value != .array) {
            std.debug.panic("tables file '{s}' must contain a JSON array", .{tablesFilePath});
        }

        var tableNames = try std.ArrayList([]const u8).initCapacity(alloc, parsed.value.array.items.len);
        errdefer tableNames.deinit(alloc);
        for (parsed.value.array.items) |item| {
            if (item != .string) {
                std.debug.panic("tables json '{s}' must contain a JSON array of strings", .{tablesFilePath});
            }
            const nameCopy = try alloc.dupe(u8, item.string);
            try tableNames.append(alloc, nameCopy);
        }

        return tableNames;
    } else |err| switch (err) {
        error.FileNotFound => {
            const f = std.fs.createFileAbsolute(tablesFilePath, .{}) catch |createErr| {
                std.debug.panic(
                    "failed to initiate tables file '{s}': '{s}'",
                    .{ tablesFilePath, @errorName(createErr) },
                );
            };
            _ = f.write("[]") catch |writeErr| {
                std.debug.panic(
                    "failed to initial empty state to '{s}': '{s}'",
                    .{ tablesFilePath, @errorName(writeErr) },
                );
            };
            f.close();
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

pub fn release(self: *Table, alloc: Allocator) void {
    const prev = self.refCounter.fetchSub(1, .acq_rel);
    std.debug.assert(prev > 0);

    if (prev != 1) return;

    const shouldRemove = self.disk != null and self.toRemove.load(.acquire);
    const pathCopy = if (shouldRemove) alloc.dupe(u8, self.path) catch |err| {
        std.debug.panic("failed to copy table path '{s}': {s}", .{ self.path, @errorName(err) });
    } else null;
    defer if (pathCopy) |p| alloc.free(p);

    self.close(alloc);
    if (pathCopy) |p| {
        std.fs.deleteTreeAbsolute(p) catch |err| {
            std.debug.panic("failed to delete table '{s}': {s}", .{ p, @errorName(err) });
        };
    }
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

test "release keeps table unless toRemove is set, then removes table dir" {
    const alloc = testing.allocator;

    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    const rootPath = try tmp.dir.realpathAlloc(alloc, ".");
    defer alloc.free(rootPath);
    const tablePath = try std.fs.path.join(alloc, &.{ rootPath, "table-1" });
    defer alloc.free(tablePath);

    try createTestTableDir(alloc, tablePath);

    const table1 = try Table.open(alloc, tablePath);
    table1.release(alloc);
    try std.fs.accessAbsolute(tablePath, .{});

    const table2 = try Table.open(alloc, tablePath);
    table2.toRemove.store(true, .release);
    table2.release(alloc);
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

    var memTable = try MemTable.empty(alloc);
    defer memTable.deinit(alloc);

    const table = try Table.fromMem(alloc, memTable);
    // we expected only second release close cleans the table, otherwise it's a memory leak
    table.retain();

    try testing.expectError(error.FileNotFound, std.fs.accessAbsolute(missingPath, .{}));
    table.release(alloc);
    try testing.expectError(error.FileNotFound, std.fs.accessAbsolute(missingPath, .{}));
    table.release(alloc);
    try testing.expectError(error.FileNotFound, std.fs.accessAbsolute(missingPath, .{}));
}
