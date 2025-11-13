const std = @import("std");

const size = 4 * 1024; // 4kb

pub const Buffer = struct {
    chunks: [size]u8,
    len: usize,

    pub fn init(allocator: std.mem.Allocator) !*Buffer {
        const buf = try allocator.create(Buffer);
        return buf;
    }

    pub fn deinit(self: *Buffer, allocator: std.mem.Allocator) void {
        allocator.destroy(self);
    }

    pub fn write(self: *Buffer, buf: []u8) void {
        @memcpy(self.chunks[0..buf.len], buf);
        self.len = buf.len;
    }

    pub fn content(self: *Buffer) []u8 {
        return self.chunks[0..self.len];
    }
};
