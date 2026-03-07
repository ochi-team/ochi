const std = @import("std");
const Allocator = std.mem.Allocator;

const MemTable = @import("../inmem/MemTable.zig");
const DiskTable = @import("DiskTable.zig");
const IndexBlockHeader = @import("../inmem/IndexBlockHeader.zig");
const TableHeader = @import("../inmem/TableHeader.zig");

const Table = @This();

// either one has to be available
// // NOTE: adding a third one like object table may complicated it,
// so then it would require implement at able as an interface
disk: ?*DiskTable,
mem: ?*MemTable,

// fields for all the tables
indexBlockHeaders: []IndexBlockHeader,
tableHeader: *TableHeader,
size: u64,
path: []const u8,

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

pub fn fromMem(alloc: Allocator, memTable: *MemTable) !*Table {
    const metaIndexBuf = memTable.streamWriter.metaIndexBuf.items;
    var indexBlockHeaders: []IndexBlockHeader = &.{};
    if (metaIndexBuf.len > 0) {
        indexBlockHeaders = try IndexBlockHeader.readIndexBlockHeaders(alloc, metaIndexBuf);
    }
    errdefer {
        for (indexBlockHeaders) |*hdr| hdr.deinitRead(alloc);
        if (indexBlockHeaders.len > 0) alloc.free(indexBlockHeaders);
    }

    const table = try alloc.create(Table);
    table.* = .{
        .mem = memTable,
        .disk = null,
        .size = memTable.streamWriter.size(),
        .path = "",
        .indexBlockHeaders = indexBlockHeaders,
        .tableHeader = &memTable.tableHeader,
        .refCounter = .init(1),
        .alloc = alloc,
    };

    return table;
}

pub fn close(self: *Table) void {
    if (self.disk) |disk| {
        disk.deinit(self.alloc);
    }

    if (self.mem) |mem| {
        mem.deinit(self.alloc);
    }

    for (self.indexBlockHeaders) |*hdr| hdr.deinitRead(self.alloc);
    if (self.indexBlockHeaders.len > 0) self.alloc.free(self.indexBlockHeaders);

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

pub fn retain(self: *Table) void {
    _ = self.refCounter.fetchAdd(1, .acquire);
}

pub fn release(self: *Table) void {
    const prev = self.refCounter.fetchSub(1, .acq_rel);
    std.debug.assert(prev > 0);

    if (prev != 1) return;

    self.close();
}

pub fn lessThan(_: void, one: *Table, another: *Table) bool {
    return one.size < another.size;
}

const testing = std.testing;

test "release fromMem does not affect filesystem path" {
    const alloc = testing.allocator;

    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    const rootPath = try tmp.dir.realpathAlloc(alloc, ".");
    defer alloc.free(rootPath);
    const missingPath = try std.fs.path.join(alloc, &.{ rootPath, "never-created" });
    defer alloc.free(missingPath);

    const memTable = try MemTable.init(alloc);

    const table = try Table.fromMem(alloc, memTable);
    // we expected only second release close cleans the table, otherwise it's a memory leak
    table.retain();

    try testing.expectError(error.FileNotFound, std.fs.accessAbsolute(missingPath, .{}));
    table.release();
    try testing.expectError(error.FileNotFound, std.fs.accessAbsolute(missingPath, .{}));
    table.release();
    try testing.expectError(error.FileNotFound, std.fs.accessAbsolute(missingPath, .{}));
}
