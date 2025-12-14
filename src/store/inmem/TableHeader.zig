const std = @import("std");
const Allocator = std.mem.Allocator;

const Self = @This();

version: u8,
uncompressedSize: u32,
compressedSize: u32,
len: u32,
blocksCount: u32,
minTimestamp: u64,
maxTimestamp: u64,
bloomValuesBuffersAmount: u32,

pub fn init(alloc: Allocator) !*Self {
    return alloc.create(Self);
}

pub fn deinit(self: *Self, alloc: Allocator) void {
    alloc.destroy(self);
}
