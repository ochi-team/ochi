const std = @import("std");
const Allocator = std.mem.Allocator;

const Self = @This();

pub fn init(alloc: Allocator) !*Self {
    return alloc.create(Self);
}

pub fn deinit(self: *Self, alloc: Allocator) void {
    alloc.destroy(self);
}
