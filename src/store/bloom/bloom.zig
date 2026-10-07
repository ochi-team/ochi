const std = @import("std");

pub const hashRounds = 6;

// TODO: it could be vectorize and be x100 faster
// https://www.yagiz.co/eliminating-branches-in-cpp-loops
pub fn isASCII(s: []const u8) bool {
    for (s) |b| {
        // TODO: make it unrolled via &= ?
        if (!((b >= 0) & (b < 0x80))) {
            return false;
        }
    }

    return true;
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

pub fn tokenHashes(alloc: std.mem.Allocator, tokens: []const u8) ![]u64 {
    var buf: [8]u8 align(@alignOf(u64)) = undefined;
    const p: *u64 = @ptrCast(&buf);
    p.* = std.hash.XxHash64.hash(0, tokens);

    var hashes: [hashRounds]u64 = undefined;
    for (0..hashRounds) |i| {
        hashes[i] = std.hash.XxHash64.hash(0, &buf);
        p.* += 1;
    }

    return alloc.dupe(u64, &hashes);
}
