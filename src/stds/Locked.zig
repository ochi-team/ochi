const std = @import("std");

pub fn Locked(comptime T: type) type {
    return struct {
        val: T,
        mx: std.Io.Mutex = .init,
    };
}
