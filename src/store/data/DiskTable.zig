const std = @import("std");
const Allocator = std.mem.Allocator;

const TableHeader = @import("../inmem/TableHeader.zig");

pub const DiskTable = @This();

tableHeader: TableHeader,

indexFile: std.Io.File,
columnsHeaderIndexFile: std.Io.File,
columnsHeaderFile: std.Io.File,
timestampsFile: std.Io.File,

messageBloomTokensFile: std.Io.File,
messageBloomValuesFile: std.Io.File,
bloomTokensFiles: []std.Io.File,
bloomValuesFiles: []std.Io.File,

pub fn deinit(self: *DiskTable, allocator: Allocator) void {
    allocator.destroy(self);
}
