const std = @import("std");
const Allocator = std.mem.Allocator;

pub const DiskTable = @This();

pub fn deinit(self: *DiskTable, allocator: Allocator) void {
    allocator.destroy(self);
}
