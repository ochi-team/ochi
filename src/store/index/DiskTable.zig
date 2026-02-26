const std = @import("std");
const Allocator = std.mem.Allocator;

const TableHeader = @import("TableHeader.zig");

const DiskTable = @This();

tableHeader: TableHeader,

indexFile: std.fs.File,
entriesFile: std.fs.File,
lensFile: std.fs.File,

pub fn deinit(self: *DiskTable, alloc: Allocator) void {
    // TODO: close files in parallel

    self.indexFile.close();
    self.entriesFile.close();
    self.lensFile.close();

    self.tableHeader.deinit(alloc);

    alloc.destroy(self);
}
