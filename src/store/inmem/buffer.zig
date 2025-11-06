const std = @import("std");

const size = 4 * 1024; // 4kb

pub const Buffer = struct {
    chunks: [20][size]u8,

    pub fn init(allocator: std.mem.Allocator) !*Buffer {
        const buf = try allocator.create(Buffer);
        return buf;
    }

    pub fn write(self: *Buffer, buf: []u8) void {
        @memcpy(self.chunks[0][0..], buf);
    }
};
