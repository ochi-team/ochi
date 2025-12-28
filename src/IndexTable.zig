const std = @import("std");

const Self = @This();

flushInterval: u64,

pub fn init(allocator: std.mem.Allocator, flushInterval: u64) !*Self {
    const t = try allocator.create(Self);
    t.* = .{
        .flushInterval = flushInterval,
    };
    return t;
}

