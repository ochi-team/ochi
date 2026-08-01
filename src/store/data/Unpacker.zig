const std = @import("std");
const encoding = @import("encoding");
const Decoder = encoding.Decoder;

const Packer = @import("Packer.zig");
const areNumbersSame = Packer.areNumbersSame;
const DecompressionPool = @import("../compression/DecompressionPool.zig");
const Io = std.Io;

const UnpackError = error{
    InvalidCompressionKind,
    InvalidBlockType,
    InsufficientData,
    InsufficientDataLen,
    InvalidLeb128,
    DecompressionFailed,
};

const Self = @This();
// TODO: get rid of collecting garbage
garbage: std.ArrayList([]u8) = .empty,
compressionPool: *DecompressionPool,

pub fn init(allocator: std.mem.Allocator, compressionPool: *DecompressionPool) !*Self {
    const s = try allocator.create(Self);
    s.* = .{ .compressionPool = compressionPool };
    return s;
}
pub fn deinit(self: *Self, allocator: std.mem.Allocator) void {
    for (self.garbage.items) |buf| {
        allocator.free(buf);
    }
    self.garbage.deinit(allocator);
    allocator.destroy(self);
}

pub fn unpackValues(self: *Self, io: Io, allocator: std.mem.Allocator, encoded: []const u8, count: usize) ![][]const u8 {
    var offset: usize = 0;
    const lengths = try self.unpackU64(io, allocator, encoded, count, &offset);
    defer allocator.free(lengths);

    const tail = encoded[offset..];
    const buf = try self.unpackBytes(io, allocator, tail, &offset);
    try self.garbage.append(allocator, buf);
    std.debug.assert(offset == encoded.len);

    var res = try allocator.alloc([]const u8, lengths.len);
    // same values first
    if (lengths.len >= 2 and buf.len == lengths[0] and areNumbersSame(lengths)) {
        for (0..res.len) |i| {
            res[i] = buf;
        }
        return res;
    }

    offset = 0;
    for (0..res.len) |i| {
        const len = lengths[i];
        std.debug.assert(buf[offset..].len >= len);
        res[i] = buf[offset .. offset + len];
        offset += len;
    }
    return res;
}

pub fn unpackU64(self: *Self, io: Io, allocator: std.mem.Allocator, encoded: []const u8, count: usize, offset: *usize) ![]u64 {
    const buf = try self.unpackBytes(io, allocator, encoded, offset);
    defer allocator.free(buf);
    return unpackU64s(allocator, buf, count);
}

fn unpackU64s(allocator: std.mem.Allocator, data: []const u8, count: usize) ![]u64 {
    if (data.len < 1) {
        return UnpackError.InsufficientData;
    }
    const vType = data[0];
    var res = try allocator.alloc(u64, count);
    errdefer allocator.free(res);

    switch (vType) {
        Packer.uintBlockTypeInvariant8 => {
            if (data[1..].len != 1) {
                return UnpackError.InsufficientDataLen;
            }
            for (0..count) |i| {
                res[i] = @intCast(data[1]);
            }
        },
        Packer.uintBlockTypeInvariant16 => {
            if (data[1..].len != 2) {
                return UnpackError.InsufficientDataLen;
            }
            var decoder = Decoder.init(data[1..]);
            const v = decoder.readInt(u16);
            for (0..count) |i| {
                res[i] = @intCast(v);
            }
        },
        Packer.uintBlockTypeInvariant32 => {
            if (data[1..].len != 4) {
                return UnpackError.InsufficientDataLen;
            }
            var decoder = Decoder.init(data[1..]);
            const v = decoder.readInt(u32);
            for (0..count) |i| {
                res[i] = @intCast(v);
            }
        },
        Packer.uintBlockTypeInvariant64 => {
            if (data[1..].len != 8) {
                return UnpackError.InsufficientDataLen;
            }
            var decoder = Decoder.init(data[1..]);
            const v = decoder.readInt(u64);
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
                const v = decoder.readInt(u16);
                res[i] = @intCast(v);
            }
        },
        Packer.uintBlockType32 => {
            if (data[1..].len != count * 4) {
                return UnpackError.InsufficientDataLen;
            }
            var decoder = Decoder.init(data[1..]);
            for (0..count) |i| {
                const v = decoder.readInt(u32);
                res[i] = @intCast(v);
            }
        },
        Packer.uintBlockType64 => {
            if (data[1..].len != count * 8) {
                return UnpackError.InsufficientDataLen;
            }
            var decoder = Decoder.init(data[1..]);
            for (0..count) |i| {
                const v = decoder.readInt(u64);
                res[i] = @intCast(v);
            }
        },
        else => return UnpackError.InvalidBlockType,
    }
    return res;
}

fn unpackBytes(self: *Self, io: Io, allocator: std.mem.Allocator, data: []const u8, offset: *usize) ![]u8 {
    if (data.len == 0) {
        return UnpackError.InsufficientData;
    }

    const compressionKind = data[0];

    // TODO: memory copies are crap here
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
            const compressedLen = Decoder.readVarIntFromBuf(data[1..]);
            offset.* += 1 + compressedLen.offset + compressedLen.value;
            var rest = data[1 + compressedLen.offset ..];
            if (rest.len < compressedLen.value) {
                return UnpackError.InsufficientDataLen;
            }
            const compressedData = rest[0..compressedLen.value];

            const decompressedSize = try encoding.getFrameContentSize(compressedData);

            const decompressed = try allocator.alloc(u8, decompressedSize);
            errdefer allocator.free(decompressed);

            // TODO: it must be fixed, we must not rely on the expected size
            const actualSize = try self.compressionPool.decompress(io, decompressed, compressedData);
            if (actualSize != decompressedSize) {
                allocator.free(decompressed);
                return UnpackError.DecompressionFailed;
            }

            return decompressed;
        },
        else => return UnpackError.InvalidCompressionKind,
    }
}

const testing = std.testing;

test "Unpacker.unpackU64s" {
    const Case = struct {
        data: []const u8,
        count: usize,
        expected: []const u64 = &.{},
        expectedErr: ?anyerror = null,
    };

    const cases = [_]Case{
        .{
            // uintBlockTypeInvariant64: single big-endian u64 repeated for every element
            .data = &[_]u8{ Packer.uintBlockTypeInvariant64, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08 },
            .count = 3,
            .expected = &[_]u64{ 0x0102030405060708, 0x0102030405060708, 0x0102030405060708 },
        },
        .{
            // uintBlockTypeInvariant64: wrong payload length
            .data = &[_]u8{ Packer.uintBlockTypeInvariant64, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07 },
            .count = 3,
            .expectedErr = UnpackError.InsufficientDataLen,
        },
        .{
            // uintBlockType64: one big-endian u64 per element
            .data = &[_]u8{
                Packer.uintBlockType64,
                0x00,
                0x00,
                0x00,
                0x00,
                0x00,
                0x00,
                0x00,
                0x01,
                0xFF,
                0xFF,
                0xFF,
                0xFF,
                0xFF,
                0xFF,
                0xFF,
                0xFF,
            },
            .count = 2,
            .expected = &[_]u64{ 1, 0xFFFFFFFFFFFFFFFF },
        },
        .{
            // uintBlockType64: wrong payload length for count
            .data = &[_]u8{
                Packer.uintBlockType64,
                0x00,
                0x00,
                0x00,
                0x00,
                0x00,
                0x00,
                0x00,
                0x01,
            },
            .count = 2,
            .expectedErr = UnpackError.InsufficientDataLen,
        },
    };

    for (cases) |case| {
        if (case.expectedErr) |expectedErr| {
            try testing.expectError(expectedErr, unpackU64s(testing.allocator, case.data, case.count));
            continue;
        }
        const got = try unpackU64s(testing.allocator, case.data, case.count);
        defer testing.allocator.free(got);
        try testing.expectEqualSlices(u64, case.expected, got);
    }
}
