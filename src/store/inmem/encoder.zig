const std = @import("std");

const EncodeError = error{
    NegativePaddingNotAllowed,
};

/// Serializer provides a single point for encoding values into byte buffers.
pub const Encoder = struct {
    buf: *std.ArrayList(u8),

    pub fn init(buf: *std.ArrayList(u8)) Encoder {
        return .{ .buf = buf };
    }

    /// Write a typed integer value to the buffer using bitcast
    pub fn writeInt(self: Encoder, comptime T: type, value: T) !void {
        const bytes: [@sizeOf(T)]u8 = @bitCast(value);
        try self.buf.appendSliceBounded(&bytes);
    }

    /// Write raw bytes to the buffer
    pub fn writeBytes(self: Encoder, bytes: []const u8) !void {
        try self.buf.appendSliceBounded(bytes);
    }

    /// Write bytes padded to a fixed size (padding with zeros)
    pub fn writePadded(self: Encoder, bytes: []const u8, totalSize: usize) !void {
        if (self.buf.capacity - self.buf.items.len < totalSize) unreachable;
        if (bytes.len > totalSize) return EncodeError.NegativePaddingNotAllowed;

        const slice = self.buf.unusedCapacitySlice()[0..totalSize];
        @memset(slice, 0x00);
        @memcpy(slice[0..bytes.len], bytes);
        self.buf.items.len += totalSize;
    }
};
