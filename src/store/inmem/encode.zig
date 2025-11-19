const std = @import("std");

pub fn encodeTimestamps(allocator: std.mem.Allocator, tss: []u64) ![]u8 {
    return std.fmt.allocPrint(allocator, "{any}", .{tss});
}

/// Decode timestamps from the encoded format (debug print format: "{ 1, 2, 3 }")
pub fn decodeTimestamps(allocator: std.mem.Allocator, encoded: []const u8) ![]u64 {
    var timestamps = try std.ArrayList(u64).initCapacity(allocator, 10);
    errdefer timestamps.deinit(allocator);

    // Format is "{ N, N, ... }"
    var iter = std.mem.tokenizeScalar(u8, encoded, ' ');

    // Skip opening brace
    _ = iter.next();

    while (iter.next()) |token| {
        if (std.mem.eql(u8, token, "}")) {
            break;
        }

        // Remove trailing comma if present
        const num_str = if (std.mem.endsWith(u8, token, ","))
            token[0 .. token.len - 1]
        else
            token;

        const num = std.fmt.parseInt(u64, num_str, 10) catch continue;
        try timestamps.append(allocator, num);
    }

    return timestamps.toOwnedSlice(allocator);
}

/// Serializer provides a single point for encoding values into byte buffers.
pub const Encoder = struct {
    buf: *std.ArrayList(u8),

    pub fn init(buf: *std.ArrayList(u8)) Encoder {
        return .{ .buf = buf };
    }

    /// Write a typed integer value to the buffer using bitcast
    pub fn writeInt(self: Encoder, comptime T: type, value: T) void {
        const bytes: [@sizeOf(T)]u8 = @bitCast(value);
        self.buf.appendSliceAssumeCapacity(&bytes);
    }

    /// Write raw bytes to the buffer
    pub fn writeBytes(self: Encoder, bytes: []const u8) void {
        self.buf.appendSliceAssumeCapacity(bytes);
    }

    /// Write bytes padded to a fixed size (padding with zeros)
    pub fn writePadded(self: Encoder, bytes: []const u8, totalSize: usize) void {
        if (self.buf.capacity - self.buf.items.len < totalSize) unreachable;
        if (bytes.len > totalSize) @panic("negative padding now allowed");

        const slice = self.buf.unusedCapacitySlice()[0..totalSize];
        @memset(slice, 0x00);
        @memcpy(slice[0..bytes.len], bytes);
        self.buf.items.len += totalSize;
    }
};

const DecodeError = error{
    InsufficientBuffer,
};

/// Decoder provides a single point for reading values from byte buffers.
pub const Decoder = struct {
    buf: []const u8,
    offset: usize = 0,

    pub fn init(buf: []const u8) Decoder {
        return .{ .buf = buf, .offset = 0 };
    }

    /// Read a typed integer value from the buffer using bitcast
    pub fn readInt(self: *Decoder, comptime T: type) !T {
        const size = @sizeOf(T);
        if (self.offset + size > self.buf.len) {
            return DecodeError.InsufficientBuffer;
        }
        const bytes: [size]u8 = self.buf[self.offset..][0..size].*;
        self.offset += size;
        return @bitCast(bytes);
    }

    /// Read raw bytes from the buffer
    pub fn readBytes(self: *Decoder, len: usize) ![]const u8 {
        if (self.offset + len > self.buf.len) {
            return DecodeError.InsufficientBuffer;
        }
        const result = self.buf[self.offset .. self.offset + len];
        self.offset += len;
        return result;
    }

    /// Read padded bytes (fixed size with zero padding), return the actual content without padding
    pub fn readPadded(self: *Decoder, totalSize: usize) ![]const u8 {
        const bytes = try self.readBytes(totalSize);
        // Find the length of actual content (before padding zeros)
        const len = std.mem.indexOfScalar(u8, bytes, 0) orelse totalSize;
        return bytes[0..len];
    }

    /// Peek at the current position without advancing offset
    pub fn peek(self: *Decoder, len: usize) ![]const u8 {
        if (self.offset + len > self.buf.len) {
            return DecodeError.InsufficientBuffer;
        }
        defer {
            self.offset += len;
        }
        return self.buf[self.offset .. self.offset + len];
    }

    /// Get remaining bytes from current offset
    pub fn remaining(self: Decoder) []const u8 {
        return self.buf[self.offset..];
    }
};
