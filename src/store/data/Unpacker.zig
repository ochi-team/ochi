const std = @import("std");
const Allocator = std.mem.Allocator;
const encoding = @import("encoding");
const Decoder = encoding.Decoder;
const tracy = @import("tracy");

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

/// leaky supposed be to be used with arena in order not to collect garbage to free later
pub fn Unpacker(comptime leaky: bool) type {
    return struct {
        const Self = @This();

        garbage: if (!leaky) std.ArrayList([]u8) else void,
        compressionPool: *DecompressionPool,

        pub fn init(alloc: Allocator, compressionPool: *DecompressionPool) !*Self {
            const s = try alloc.create(Self);
            s.* = .{
                .garbage = if (!leaky) .empty else {},
                .compressionPool = compressionPool,
            };
            return s;
        }

        /// resetArena must be called whenever the arena backing allocator is reset,
        /// it doesn't use .clearRetainingCapacity in order not to retain dangling memory.
        /// no-op when leaky, since garbage isn't tracked and the arena reclaims it wholesale.
        pub fn resetArena(self: *Self) void {
            if (!leaky) self.garbage = .empty;
        }

        pub fn deinit(self: *Self, alloc: Allocator) void {
            if (!leaky) {
                for (self.garbage.items) |buf| {
                    alloc.free(buf);
                }
                self.garbage.deinit(alloc);
            }
            alloc.destroy(self);
        }

        pub fn unpackValues(self: *Self, io: Io, alloc: Allocator, encoded: []const u8, count: usize) ![][]const u8 {
            const z = tracy.Zone.begin(.{
                .name = "unpackValues",
                .src = @src(),
            });
            defer z.end();
            var offset: usize = 0;
            const lengths = try self.unpackU64(io, alloc, encoded, count, &offset);
            defer alloc.free(lengths);

            const tail = encoded[offset..];
            const buf = try self.unpackBytes(io, alloc, tail, &offset);
            if (!leaky) try self.garbage.append(alloc, buf);
            std.debug.assert(offset == encoded.len);

            var res = try alloc.alloc([]const u8, lengths.len);
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

        pub fn unpackU64(
            self: *Self,
            io: Io,
            alloc: Allocator,
            encoded: []const u8,
            count: usize,
            offset: *usize,
        ) ![]u64 {
            const buf = try self.unpackBytes(io, alloc, encoded, offset);
            defer alloc.free(buf);
            return unpackU64s(alloc, buf, count);
        }

        fn unpackBytes(self: *Self, io: Io, alloc: Allocator, data: []const u8, offset: *usize) ![]u8 {
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
                    return alloc.dupe(u8, bytes[0..len]);
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

                    const decompressed = try alloc.alloc(u8, decompressedSize);
                    errdefer alloc.free(decompressed);

                    // TODO: it must be fixed, we must not rely on the expected size
                    const actualSize = try self.compressionPool.decompress(io, decompressed, compressedData);
                    if (actualSize != decompressedSize) {
                        alloc.free(decompressed);
                        return UnpackError.DecompressionFailed;
                    }

                    return decompressed;
                },
                else => return UnpackError.InvalidCompressionKind,
            }
        }
    };
}

fn unpackU64s(alloc: Allocator, data: []const u8, count: usize) ![]u64 {
    if (data.len < 1) {
        return UnpackError.InsufficientData;
    }
    const vType = data[0];
    var res = try alloc.alloc(u64, count);
    errdefer alloc.free(res);

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
