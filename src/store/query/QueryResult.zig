const std = @import("std");
const Allocator = std.mem.Allocator;

const Column = @import("../../store/data/Column.zig");

const QueryResult = @This();

/// matched lines slot of the block
hits: std.bit_set.DynamicBitSetUnmanaged,
len: usize,

// decoded block
timestamps: std.ArrayList(u64) = .empty,
columns: std.ArrayList(Column) = .empty,
// TODO: perhaps known invariants in the end don't have to be Column type, but Field
invariants: std.ArrayList(Column) = .empty,

pub fn deinit(self: *QueryResult, alloc: Allocator) void {
    self.hits.deinit(alloc);

    self.timestamps.deinit(alloc);

    for (self.columns.items) |col| alloc.free(col.values);
    self.columns.deinit(alloc);

    for (self.invariants.items) |col| alloc.free(col.values);
    self.invariants.deinit(alloc);
}
