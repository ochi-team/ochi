const std = @import("std");

const Self = @This();

/// Decoder provides a single point for reading values from byte buffers.
buf: []const u8,
offset: usize = 0,

pub fn init(buf: []const u8) Self {
    return .{ .buf = buf };
}

/// Read a typed integer value from the buffer using big-endian decoding
pub fn readInt(self: *Self, comptime T: type) T {
    const size = @sizeOf(T);
    const bytes: [size]u8 = self.buf[self.offset..][0..size].*;
    self.offset += size;
    return std.mem.readInt(T, &bytes, .big);
}

/// Read raw bytes from the buffer
pub fn readBytes(self: *Self, len: usize) []const u8 {
    const result = self.buf[self.offset .. self.offset + len];
    self.offset += len;
    return result;
}

pub inline fn readString(self: *Self) []const u8 {
    const size = self.readVarInt();
    return self.readBytes(size);
}

/// Read padded bytes (fixed size with zero padding), return the actual content without padding
pub fn readPadded(self: *Self, totalSize: usize) []const u8 {
    const bytes = self.readBytes(totalSize);
    // Find the length of actual content (before padding zeros)
    const len = std.mem.indexOfScalar(u8, bytes, 0) orelse totalSize;
    return bytes[0..len];
}

pub fn readVarInt(self: *Self) u64 {
    const v = readVarIntFromBuf(self.buf[self.offset..]);
    self.offset += v.offset;
    return v.value;
}

pub fn CompressedValue(comptime T: type) type {
    return struct {
        offset: usize,
        value: T,
    };
}

pub fn readVarIntFromBuf(data: []const u8) CompressedValue(u64) {
    var result: u64 = 0;
    var shift: u6 = 0;

    for (0..10) |i| {
        const byte = data[i];
        result |= @as(u64, byte & 0x7f) << shift;

        if ((byte & 0x80) == 0) {
            return .{
                .offset = i + 1,
                .value = result,
            };
        }

        shift += 7;
    }

    @panic("invalid leb128");
}
