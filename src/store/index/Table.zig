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
const DecompressionPool = @import("../compression/DecompressionPool.zig");

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

pub fn openAll(io: Io, parentAlloc: Allocator, path: []const u8, decompressionPool: *DecompressionPool) !std.ArrayList(*Table) {
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
    var tablesFilePathBuf: [std.fs.max_path_bytes]u8 = undefined;
    var tablesFilePathWriter = std.Io.Writer.fixed(&tablesFilePathBuf);
    try std.fs.path.fmtJoin(&.{ path, filenames.tables }).format(&tablesFilePathWriter);
    var tableNames = try catalog.readNames(io, alloc, tablesFilePathWriter.buffered(), true);
    defer {
        for (tableNames.items) |name| alloc.free(name);
        tableNames.deinit(alloc);
    }

    // syncing tables with a json, make sure all the listed dirs exist
    try catalog.validateTablesExist(io, path, tableNames.items);

    // syncing tables with the given names remove all the not listed dirs
    try catalog.removeUnusedTables(io, path, tableNames.items);

    // open tables
    var tables = try std.ArrayList(*Table).initCapacity(parentAlloc, tableNames.items.len);
    errdefer {
        for (tables.items) |table| table.close(io);
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

pub fn open(io: Io, alloc: Allocator, path: []const u8, decompressionPool: *DecompressionPool) !*Table {
    var parsedTableHeader = try TableHeader.readFile(io, alloc, path);
    errdefer parsedTableHeader.deinit(alloc);

    const decodedMetaindex = try MetaIndex.readFile(io, alloc, decompressionPool, path, parsedTableHeader.blocksCount);
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

pub fn fromMem(io: Io, alloc: Allocator, memTable: *MemTable, decompressionPool: *DecompressionPool) !*Table {
    var decodedMetaindex: MetaIndex.DecodedMetaIndex = .{
        .records = &.{},
        .compressedSize = 0,
    };
    // TODO: we must generate a real table for this use case,
    // but it requires moving all the stub models generation from all the test to a designated package/file,
    // then we can remove this dumb condition
    if (!builtin.is_test or memTable.metaindexBuf.items.len > 0) {
        decodedMetaindex = try MetaIndex.decodeDecompress(
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
    // TODO: amount of max tables per partition must be known in advance,
    // either the json encoding size
    const data = try std.json.Stringify.valueAlloc(fba, tableNames.items, .{
        .whitespace = .minified,
    });
    defer fba.free(data);

    var tablesFilePathBuf: [std.fs.max_path_bytes]u8 = undefined;
    var tablesFilePathWriter = std.Io.Writer.fixed(&tablesFilePathBuf);
    try std.fs.path.fmtJoin(&.{ path, filenames.tables }).format(&tablesFilePathWriter);

    try fs.writeBufferToFileAtomic(io, tablesFilePathWriter.buffered(), data, true);
}

pub fn retain(self: *Table) void {
    _ = self.refCounter.fetchAdd(1, .monotonic);
}

pub fn release(self: *Table, io: Io) void {
    const prev = self.refCounter.fetchSub(1, .acq_rel);
    std.debug.assert(prev > 0);

    if (prev != 1) return;

    self.close(io);
}

test {
    _ = @import("Table_test.zig");
}
