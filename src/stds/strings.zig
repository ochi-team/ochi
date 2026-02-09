const std = @import("std");

pub fn contains(strings: []const []const u8, item: []const u8) bool {
    for (strings) |string| {
        if (std.mem.eql(u8, string, item)) return true;
    }
    return false;
}
