const std = @import("std");
const Allocator = std.mem.Allocator;
//TODO the file naming is not consistent

const EntriesBlock = @This();

entriesBuf: std.ArrayList(u8) = .empty,
lensBuf: std.ArrayList(u8) = .empty,

pub fn deinit(self: *EntriesBlock, alloc: Allocator) void {
    self.entriesBuf.deinit(alloc);
    self.lensBuf.deinit(alloc);
}

pub fn reset(self: *EntriesBlock) void {
    self.entriesBuf.clearRetainingCapacity();
    self.lensBuf.clearRetainingCapacity();
}
