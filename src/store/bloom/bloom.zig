const std = @import("std");

// TODO: it could be vectorize and be x100 faster
// https://www.yagiz.co/eliminating-branches-in-cpp-loops
pub fn isASCII(s: []const u8) bool {
    var ok: bool = true;
    for (s) |b| {
        ok &= (b >= 0) & (b < 0x80);
    }
    return ok;
}

pub inline fn isChar(c: u8) bool {
    return tokenCharTable[c] != 0;
}

const tokenCharTable = blk: {
    var a: [256]u8 = undefined;
    for (0..256) |c| {
        if (c >= 'a' and c <= 'z' or c >= 'A' and c <= 'Z' or c >= '0' and c <= '9' or c == '_') {
            a[c] = 1;
        } else {
            a[c] = 0;
        }
    }
    break :blk a;
};

pub const bucketsSize = 1024;
pub const Bucket = struct {
    value: u64,
    overflows: std.ArrayList(u64),
};
