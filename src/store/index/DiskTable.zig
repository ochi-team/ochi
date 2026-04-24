const std = @import("std");
const Allocator = std.mem.Allocator;

const TableHeader = @import("TableHeader.zig");

const DiskTable = @This();

tableHeader: TableHeader,

indexFile: Io.File,
entriesFile: Io.File,
lensFile: Io.File,

pub fn deinit(self: *DiskTable, alloc: Allocator) void {
    // TODO: close files in parallel

    self.indexFile.close();
    self.entriesFile.close();
    self.lensFile.close();

    self.tableHeader.deinit(alloc);

    alloc.destroy(self);
}
