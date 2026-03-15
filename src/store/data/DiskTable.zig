const std = @import("std");
const Allocator = std.mem.Allocator;

const TableHeader = @import("../inmem/TableHeader.zig");

pub const DiskTable = @This();

tableHeader: TableHeader,

pub fn deinit(self: *DiskTable, allocator: Allocator) void {
    allocator.destroy(self);
}
