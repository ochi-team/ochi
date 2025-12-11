const std = @import("std");
const encoding = @import("encoding");
const Decoder = encoding.Decoder;
const Packer = @import("Packer.zig");
const areNumbersSame = Packer.areNumbersSame;

const UnpackError = error{
    InvalidCompressionKind,
    InvalidBlockType,
    InsufficientData,
    InsufficientDataLen,
    InvalidLeb128,
    DecompressionFailed,
};

pub fn CompressedValue(comptime T: type) type {
    return struct {
        offset: usize,
        value: T,
    };
}

const Self = @This();

buf: []u8,
pub fn init(allocator: std.mem.Allocator) !*Self {
    const s = try allocator.create(Self);
    s.* = Self{
        .buf = undefined,
    };
    return s;
}
pub fn deinit(self: *Self, allocator: std.mem.Allocator) void {
    if (self.buf.len > 0) {
        allocator.free(self.buf);
    }
    allocator.destroy(self);
}

pub fn unpackValues(self: *Self, allocator: std.mem.Allocator, encoded: []const u8, count: usize) ![][]const u8 {
    var offset: usize = 0;
    const lengths = try unpackU64(allocator, encoded, count, &offset);
    defer allocator.free(lengths);

    const tail = encoded[offset..];
    self.buf = try unpackBytes(allocator, tail, &offset);
    std.debug.assert(offset == encoded.len);

    var res = try allocator.alloc([]const u8, lengths.len);
    // same values first
    if (lengths.len >= 2 and self.buf.len == lengths[0] and areNumbersSame(lengths)) {
        for (0..res.len) |i| {
            res[i] = self.buf;
        }
        return res;
    }

    offset = 0;
    for (0..res.len) |i| {
        const len = lengths[i];
        std.debug.assert(self.buf[offset..].len >= len);
        res[i] = self.buf[offset .. offset + len];
        offset += len;
    }
    return res;
}

pub fn unpackU64(allocator: std.mem.Allocator, encoded: []const u8, count: usize, offset: *usize) ![]u64 {
    const buf = try unpackBytes(allocator, encoded, offset);
    defer allocator.free(buf);
    return unpackU64s(allocator, buf, count);
}

fn unpackU64s(allocator: std.mem.Allocator, data: []const u8, count: usize) ![]u64 {
    if (data.len < 1) {
        return UnpackError.InsufficientData;
    }
    const vType = data[0];
    var res = try allocator.alloc(u64, count);

    switch (vType) {
        Packer.uintBlockTypeCell8 => {
            if (data[1..].len != 1) {
                return UnpackError.InsufficientDataLen;
            }
            for (0..count) |i| {
                res[i] = @intCast(data[1]);
            }
        },
        Packer.uintBlockTypeCell16 => {
            if (data[1..].len != 2) {
                return UnpackError.InsufficientDataLen;
            }
            var decoder = Decoder.init(data[1..]);
            const v = try decoder.readInt(u16);
            for (0..count) |i| {
                res[i] = @intCast(v);
            }
        },
        Packer.uintBlockTypeCell32 => {
            if (data[1..].len != 4) {
                return UnpackError.InsufficientDataLen;
            }
            var decoder = Decoder.init(data[1..]);
            const v = try decoder.readInt(u32);
            for (0..count) |i| {
                res[i] = @intCast(v);
            }
        },
        Packer.uintBlockTypeCell64 => {
            if (data[1..].len != 8) {
                return UnpackError.InsufficientDataLen;
            }
            var decoder = Decoder.init(data[1..]);
            const v = try decoder.readInt(u64);
            for (0..count) |i| {
                res[i] = @intCast(v);
            }
        },
        Packer.uintBlockType8 => {
            if (data[1..].len != count) {
                return UnpackError.InsufficientDataLen;
            }
            for (0..count) |i| {
                const v = data[1 + i];
                res[i] = @intCast(v);
            }
        },
        Packer.uintBlockType16 => {
            if (data[1..].len != count * 2) {
                return UnpackError.InsufficientDataLen;
            }
            var decoder = Decoder.init(data[1..]);
            for (0..count) |i| {
                const v = try decoder.readInt(u16);
                res[i] = @intCast(v);
            }
        },
        Packer.uintBlockType32 => {
            if (data[1..].len != count * 4) {
                return UnpackError.InsufficientDataLen;
            }
            var decoder = Decoder.init(data[1..]);
            for (0..count) |i| {
                const v = try decoder.readInt(u32);
                res[i] = @intCast(v);
            }
        },
        Packer.uintBlockType64 => {
            if (data[1..].len != count * 8) {
                return UnpackError.InsufficientDataLen;
            }
            var decoder = Decoder.init(data[1..]);
            for (0..count) |i| {
                const v = try decoder.readInt(u64);
                res[i] = @intCast(v);
            }
        },
        else => return UnpackError.InvalidBlockType,
    }
    return res;
}

fn unpackBytes(allocator: std.mem.Allocator, data: []const u8, offset: *usize) ![]u8 {
    if (data.len < 1) {
        return UnpackError.InsufficientData;
    }

    const compressionKind = data[0];

    switch (compressionKind) {
        Packer.compressionKindPlain => {
            // plain format: [kind:u8][len:u8][data]
            const len = data[1];
            const bytes = data[2..];
            if (bytes.len < len) {
                return UnpackError.InsufficientDataLen;
            }
            offset.* += 2 + len;
            return allocator.dupe(u8, bytes[0..len]);
        },
        Packer.compressionKindZstd => {
            // compressed format: [kind:u8][len:leb128][compressed_data]
            const compressedLen = try readLeb128(data[1..]);
            offset.* += 1 + compressedLen.offset + compressedLen.value;
            var rest = data[1 + compressedLen.offset ..];
            if (rest.len < compressedLen.value) {
                return UnpackError.InsufficientDataLen;
            }
            const compressedData = rest[0..compressedLen.value];

            const decompressedSize = try encoding.getFrameContentSize(compressedData);

            const decompressed = try allocator.alloc(u8, decompressedSize);
            errdefer allocator.free(decompressed);

            const actualSize = try encoding.decompress(decompressed, compressedData);

            if (actualSize != decompressedSize) {
                allocator.free(decompressed);
                return UnpackError.DecompressionFailed;
            }

            return decompressed;
        },
        else => return UnpackError.InvalidCompressionKind,
    }
}

fn readLeb128(data: []const u8) !CompressedValue(usize) {
    var result: u64 = 0;
    var shift: u6 = 0;

    for (0..10) |i| {
        const byte = data[i];
        result |= @as(u64, byte & 0x7f) << shift;

        if ((byte & 0x80) == 0) {
            return .{
                .offset = i + 1,
                .value = @intCast(result),
            };
        }

        shift += 7;
    }

    return UnpackError.InvalidLeb128;
}
