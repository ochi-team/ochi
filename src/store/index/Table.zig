const std = @import("std");
const Allocator = std.mem.Allocator;

const Filenames = @import("../../Filenames.zig");
const fs = @import("../../fs.zig");
const strings = @import("../../stds/strings.zig");
const TableHeader = @import("TableHeader.zig");
const MemTable = @import("MemTable.zig");
const DiskTable = @import("DiskTable.zig");
const MetaIndex = @import("MetaIndex.zig");

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
inMerge: bool = false,

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
    const tableNames = try readTableNames(alloc, tablesFilePath);

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
    };

    return table;
}

// nothing specific, we simply don't expected a small json file to be larger than that
const maxFileBytes = 16 * 1024 * 1024;
fn readTableNames(alloc: Allocator, tablesFilePath: []const u8) !std.ArrayList([]const u8) {
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
