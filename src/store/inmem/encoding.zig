const std = @import("std");
pub fn encodeTimestamps(allocator: std.mem.Allocator, tss: []u64) ![]u8 {
    return std.fmt.allocPrint(allocator, "{any}", .{tss});
}
