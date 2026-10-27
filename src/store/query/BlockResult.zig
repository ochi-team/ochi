const std = @import("std");
const Column = @import("../data/Column.zig");
const Allocator = std.mem.Allocator;
const BlockResult = @This();

/// Owns lists and Column.values descriptor slices, but borrows key/value bytes
/// from the producing BlockQuery and its retained table. Consume or copy before
/// that query is deinitialized or this result is reset. Use the producer's allocator.
timestamps: std.ArrayList(u64) = .empty,
columns: std.ArrayList(Column) = .empty,
invariants: std.ArrayList(Column) = .empty,

pub fn len(self: *const BlockResult) usize {
    return self.timestamps.items.len;
}

pub fn reset(self: *BlockResult, alloc: Allocator) void {
    for (self.columns.items) |col| alloc.free(col.values);
    for (self.invariants.items) |col| alloc.free(col.values);
    self.timestamps.clearRetainingCapacity();
    self.columns.clearRetainingCapacity();
    self.invariants.clearRetainingCapacity();
}

pub fn deinit(self: *BlockResult, alloc: Allocator) void {
    self.reset(alloc);
    self.timestamps.deinit(alloc);
    self.columns.deinit(alloc);
    self.invariants.deinit(alloc);
}

/// Synchronous consumer: must not retain the result or its borrowed bytes after write returns.
pub const WriteBlock = struct {
    ctx: *anyopaque,
    writeFn: *const fn (*anyopaque, *const BlockResult) anyerror!void,

    pub fn write(self: WriteBlock, result: *const BlockResult) !void {
        try self.writeFn(self.ctx, result);
    }
};

test "BlockResult reset and reuse" {
    const a = std.testing.allocator;
    var result: BlockResult = .{};
    defer result.deinit(a);
    for (0..3) |_| {
        result.reset(a);
        try std.testing.expectEqual(0, result.len());
        try result.timestamps.ensureUnusedCapacity(a, 1);
        try result.columns.ensureUnusedCapacity(a, 1);
        try result.invariants.ensureUnusedCapacity(a, 1);
        result.timestamps.appendAssumeCapacity(42);
        result.columns.appendAssumeCapacity(.{ .key = "key", .values = try a.dupe([]const u8, &.{"value"}) });
        result.invariants.appendAssumeCapacity(.{ .key = "host", .values = try a.dupe([]const u8, &.{"local"}) });
        try std.testing.expectEqual(1, result.len());
        result.reset(a);
        result.reset(a);
        try std.testing.expectEqualDeep(.{ @as(usize, 0), @as(usize, 0), @as(usize, 0) }, .{ result.len(), result.columns.items.len, result.invariants.items.len });
    }
}
