const std = @import("std");

const Self = @This();

/// Serializer provides a single point for encoding values into byte buffers.
buf: []u8,
offset: usize = 0,

pub fn init(buf: []u8) Self {
    return .{ .buf = buf };
}

/// Write a typed integer value to the buffer using big-endian encoding
pub fn writeInt(self: *Self, comptime T: type, value: T) void {
    const slice = self.buf[self.offset .. self.offset + @sizeOf(T)];
    if (slice.len < @sizeOf(T)) unreachable;
    self.offset += @sizeOf(T);
    var bytes: [@sizeOf(T)]u8 = undefined;
    std.mem.writeInt(T, &bytes, value, .big);
    @memcpy(slice, bytes[0..]);
}

/// Write raw bytes to the buffer
pub fn writeBytes(self: *Self, bytes: []const u8) void {
    const slice = self.buf[self.offset .. self.offset + bytes.len];
    if (slice.len < bytes.len) unreachable;
    self.offset += bytes.len;
    @memcpy(slice, bytes[0..]);
}

pub inline fn writeString(self: *Self, str: []const u8) void {
    self.writeVarInt(str.len);
    self.writeBytes(str);
}

/// Write bytes padded to a fixed size (padding with zeros)
pub fn writePadded(self: *Self, bytes: []const u8, totalSize: usize) void {
    if (bytes.len > totalSize) @panic("negative padding now allowed");

    const slice = self.buf[self.offset .. self.offset + totalSize];
    if (slice.len < totalSize) unreachable;
    self.offset += totalSize;

    @memset(slice, 0x00);
    @memcpy(slice[0..bytes.len], bytes);
}

/// The maximum number of bytes a varint-encoded 64-bit integer can occupy.
pub const maxVarUint64Len = 10;

/// writeVarInt uses leb128 to encode a u64 into a variable-length byte sequence.
/// Returns error.OutOfMemory if the buffer has not enough capacity.
pub fn writeVarInt(self: *Self, value: u64) void {
    const slice = self.buf[self.offset .. self.offset + 10];

    var i: u8 = 0;
    var v = value;
    while (v >= 0x80) {
        slice[i] = @as(u8, @truncate(v)) | 0x80;
        v >>= 7;
        i += 1;
    }
    slice[i] = @as(u8, @truncate(v));

    self.offset += i + 1;
}

pub fn writeIntBytes(self: *Self, size: usize, value: u64) void {
    var buf: [8]u8 = undefined;
    std.mem.writeInt(u64, &buf, value, .big);
    // For big-endian, the least significant bytes are at the end
    const start = 8 - size;
    self.writeBytes(buf[start..8]);
}

/// Static helper: Encode an integer to bytes using big-endian (one-shot, no state)
pub inline fn toBytes(comptime T: type, value: T) [@sizeOf(T)]u8 {
    var bytes: [@sizeOf(T)]u8 = undefined;
    std.mem.writeInt(T, &bytes, value, .big);
    return bytes;
}

test "Encoder.writeIntBytes" {
    var allocator = std.testing.allocator;
    const Case = struct {
        type: type,
        value: u64,
    };
    inline for ([_]Case{
        .{
            .type = u8,
            .value = 42,
        },
        .{
            .type = u16,
            .value = 501,
        },
        .{
            .type = u32,
            .value = 123456,
        },
    }) |case| {
        const buf1 = try allocator.alloc(u8, @sizeOf(case.type));
        const buf2 = try allocator.alloc(u8, @sizeOf(case.type));
        defer allocator.free(buf1);
        defer allocator.free(buf2);
        var enc1 = Self.init(buf1);
        var enc2 = Self.init(buf2);
        enc1.writeInt(case.type, case.value);
        enc2.writeIntBytes(@sizeOf(case.type), case.value);
        try std.testing.expectEqualSlices(u8, buf1, buf2);
    }
}

test "Encoder.writeVarUint64" {
    const allocator = std.testing.allocator;
    const buf = try allocator.alloc(u8, 20);
    defer allocator.free(buf);

    const Case = struct {
        value: u64,
        expected: []const u8,
    };

    const cases = [_]Case{
        .{ .value = 0, .expected = &[_]u8{0x00} },
        .{ .value = 1, .expected = &[_]u8{0x01} },
        .{ .value = 127, .expected = &[_]u8{0x7f} },
        .{ .value = 128, .expected = &[_]u8{ 0x80, 0x01 } },
        .{ .value = 255, .expected = &[_]u8{ 0xff, 0x01 } },
        .{ .value = 16383, .expected = &[_]u8{ 0xff, 0x7f } },
        .{ .value = 16384, .expected = &[_]u8{ 0x80, 0x80, 0x01 } },
        .{
            .value = std.math.maxInt(u64),
            .expected = &[_]u8{ 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x01 },
        },
    };

    for (cases) |case| {
        var enc = Self.init(buf);
        enc.writeVarInt(case.value);
        try std.testing.expectEqualSlices(u8, case.expected, buf[0..enc.offset]);
    }
}
