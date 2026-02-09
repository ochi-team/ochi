const std = @import("std");
const Allocator = std.mem.Allocator;

const MetaIndex = @import("MetaIndex.zig");
const TableHeader = @import("TableHeader.zig");

const DiskTable = @This();

tableHeader: TableHeader,
metaindexRecords: []MetaIndex,

indexFile: std.fs.File,
entriesFile: std.fs.File,
lensFile: std.fs.File,

pub fn deinit(self: *DiskTable, alloc: Allocator) void {
    self.indexFile.close();
    self.entriesFile.close();
    self.lensFile.close();
    if (self.metaindexRecords.len > 0) alloc.free(self.metaindexRecords);
    self.tableHeader.deinit(alloc);
    alloc.destroy(self);
}
