const std = @import("std");
const Allocator = std.mem.Allocator;
const Io = std.Io;

const TableHeader = @import("TableHeader.zig");

const DiskTable = @This();

tableHeader: TableHeader,

indexFile: Io.File,
entriesFile: Io.File,
lensFile: Io.File,

pub fn deinit(self: *DiskTable, alloc: Allocator) void {
    // TODO: close files in parallel

    self.indexFile.close(io);
    self.entriesFile.close(io);
    self.lensFile.close(io);

    self.tableHeader.deinit(alloc);

    alloc.destroy(self);
}
