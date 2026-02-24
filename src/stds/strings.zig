const std = @import("std");

pub fn contains(strings: []const []const u8, item: []const u8) bool {
    for (strings) |string| {
        if (std.mem.eql(u8, string, item)) return true;
    }
    return false;
}

pub fn findPrefix(first: []const u8, second: []const u8) []const u8 {
    const n = @min(first.len, second.len);
    var i: usize = 0;
    while (i < n and first[i] == second[i]) : (i += 1) {}
    return first[0..@intCast(i)];
}
