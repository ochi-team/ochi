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
    self.offset += @sizeOf(T);
    var bytes: [@sizeOf(T)]u8 = undefined;
    std.mem.writeInt(T, &bytes, value, .big);
    @memcpy(slice, bytes[0..]);
}

/// Write raw bytes to the buffer
pub fn writeBytes(self: *Self, bytes: []const u8) void {
    const slice = self.buf[self.offset .. self.offset + bytes.len];
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
    self.offset += totalSize;

    @memset(slice, 0x00);
    @memcpy(slice[0..bytes.len], bytes);
}

/// The maximum number of bytes a varint-encoded 64-bit integer can occupy.
pub const maxVarUint64Len = 10;

// Calculate the exact number of bytes needed to encode a value as a varint
// TODO: Use this instead of maxVarUint64Len in encodeBound calculations for more precise memory allocation
pub fn varIntBound(value: u64) usize {
    if (value < 0x80) return 1; // < 128
    if (value < 0x4000) return 2; // < 16384
    if (value < 0x200000) return 3; // < 2097152
    if (value < 0x10000000) return 4; // < 268435456
    if (value < 0x800000000) return 5; // < 34359738368
    if (value < 0x40000000000) return 6; // < 4398046511104
    if (value < 0x2000000000000) return 7; // < 562949953421312
    if (value < 0x100000000000000) return 8; // < 72057594037927936
    if (value < 0x8000000000000000) return 9; // < 9223372036854775808
    return 10;
}

pub inline fn varIntsBound(comptime T: type, values: []T) usize {
    var res: usize = 0;
    for (values) |v| {
        res += varIntBound(v);
    }
    return res;
}

/// writeVarInt uses leb128 to encode a u64 into a variable-length byte sequence.
/// Returns error.OutOfMemory if the buffer has not enough capacity.
/// TODO: migrate to std.leb
pub fn writeVarInt(self: *Self, value: u64) void {
    const slice = self.buf[self.offset..];

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

pub inline fn writeVarInts(self: *Self, T: type, values: []T) void {
    for (values) |v| {
        self.writeVarInt(v);
    }
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
        const allocator = std.testing.allocator;
        const bound = Self.varIntBound(case.value);
        const buf = try allocator.alloc(u8, bound);
        defer allocator.free(buf);

        var enc = Self.init(buf);
        enc.writeVarInt(case.value);
        try std.testing.expectEqualSlices(u8, case.expected, buf[0..enc.offset]);
    }
}
