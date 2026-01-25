const std = @import("std");
const Allocator = std.mem.Allocator;

const StorageBlock = @This();

itemsData: std.ArrayList(u8) = .empty,
lensData: std.ArrayList(u8) = .empty,

pub fn deinit(self: *StorageBlock, alloc: Allocator) void {
    self.itemsData.deinit(alloc);
    self.lensData.deinit(alloc);
}

pub fn reset(self: *StorageBlock) void {
    self.itemsData.clearRetainingCapacity();
    self.lensData.clearRetainingCapacity();
}
