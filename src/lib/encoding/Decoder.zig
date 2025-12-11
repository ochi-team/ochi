const std = @import("std");

const DecodeError = error{
    InsufficientBuffer,
};

const Self = @This();

/// Decoder provides a single point for reading values from byte buffers.
buf: []const u8,
offset: usize = 0,

pub fn init(buf: []const u8) Self {
    return .{ .buf = buf };
}

/// Read a typed integer value from the buffer using big-endian decoding
pub fn readInt(self: *Self, comptime T: type) !T {
    const size = @sizeOf(T);
    if (self.offset + size > self.buf.len) {
        return DecodeError.InsufficientBuffer;
    }
    const bytes: [size]u8 = self.buf[self.offset..][0..size].*;
    self.offset += size;
    return std.mem.readInt(T, &bytes, .big);
}

/// Read raw bytes from the buffer
pub fn readBytes(self: *Self, len: usize) ![]const u8 {
    if (self.offset + len > self.buf.len) {
        return DecodeError.InsufficientBuffer;
    }
    const result = self.buf[self.offset .. self.offset + len];
    self.offset += len;
    return result;
}

/// Read padded bytes (fixed size with zero padding), return the actual content without padding
pub fn readPadded(self: *Self, totalSize: usize) ![]const u8 {
    const bytes = try self.readBytes(totalSize);
    // Find the length of actual content (before padding zeros)
    const len = std.mem.indexOfScalar(u8, bytes, 0) orelse totalSize;
    return bytes[0..len];
}
