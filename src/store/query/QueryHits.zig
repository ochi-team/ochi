const std = @import("std");
const Allocator = std.mem.Allocator;

const QueryHits = @This();

/// matched lines slot of the block
hits: std.bit_set.DynamicBitSetUnmanaged,

pub fn deinit(self: *QueryHits, alloc: Allocator) void {
    self.hits.deinit(alloc);
}
