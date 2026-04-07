const std = @import("std");
const Allocator = std.mem.Allocator;
const builtin = @import("builtin");

const encoding = @import("encoding");
const Encoder = encoding.Encoder;
const Decoder = encoding.Decoder;

pub const EncodingType = enum(u8) {
    plain = 0,
    zstd = 1,
};

pub const DecodedBlockHeader = struct {
    blockHeader: BlockHeader,
    offset: usize,
};

const BlockHeader = @This();

firstEntry: []const u8,
prefix: []const u8,
encodingType: EncodingType,
entriesCount: u32 = 0,
entriesBlockOffset: u64 = 0,
lensBlockOffset: u64 = 0,
entriesBlockSize: u32 = 0,
lensBlockSize: u32 = 0,

pub fn reset(self: *BlockHeader) void {
    self.* = .{ .firstEntry = "", .prefix = "", .encodingType = .plain };
}

// [len:n][firstItem:len][len:n][prefix:len][count:4][type:1][offset:8][size:4][offset:8][size:4] = bound + len + 29
pub fn bound(self: *const BlockHeader) usize {
    const firstEntryLenBound = Encoder.varIntBound(self.firstEntry.len);
    const prefixLenBound = Encoder.varIntBound(self.prefix.len);
    return firstEntryLenBound + prefixLenBound + self.firstEntry.len + self.prefix.len + 29;
}

pub fn encode(self: *const BlockHeader, buf: []u8) void {
    var enc = Encoder.init(buf);

    enc.writeString(self.firstEntry);
    enc.writeString(self.prefix);
    enc.writeInt(u8, @intFromEnum(self.encodingType));
    enc.writeInt(u32, self.entriesCount);
    enc.writeInt(u64, self.entriesBlockOffset);
    enc.writeInt(u64, self.lensBlockOffset);
    enc.writeInt(u32, self.entriesBlockSize);
    enc.writeInt(u32, self.lensBlockSize);
}

pub fn encodeAlloc(self: *const BlockHeader, alloc: Allocator) ![]u8 {
    const size = self.bound();
    const buf = try alloc.alloc(u8, size);
    self.encode(buf);

    return buf;
}

pub fn decode(buf: []const u8) DecodedBlockHeader {
    var dec = Decoder.init(buf);

    const firstItem = dec.readString();
    const prefix = dec.readString();
    const encodingType: EncodingType = @enumFromInt(dec.readInt(u8));
    const itemsCount = dec.readInt(u32);
    const itemsBlockOffset = dec.readInt(u64);
    const lensBlockOffset = dec.readInt(u64);
    const itemsBlockSize = dec.readInt(u32);
    const lensBlockSize = dec.readInt(u32);

    return .{
        .blockHeader = .{
            .firstEntry = firstItem,
            .prefix = prefix,
            .encodingType = encodingType,
            .entriesCount = itemsCount,
            .entriesBlockOffset = itemsBlockOffset,
            .lensBlockOffset = lensBlockOffset,
            .entriesBlockSize = itemsBlockSize,
            .lensBlockSize = lensBlockSize,
        },
        .offset = dec.offset,
    };
}

pub fn decodeMany(alloc: Allocator, buf: []const u8, count: usize) ![]BlockHeader {
    std.debug.assert(count > 0);
    const headers = try alloc.alloc(BlockHeader, count);

    var offset: usize = 0;
    for (headers) |*header| {
        const decoded = decode(buf[offset..]);
        offset += decoded.offset;
        header.* = decoded.blockHeader;
    }

    if (builtin.is_test) {
        const ok = std.sort.isSorted(BlockHeader, headers, {}, blockHeaderLessThan);
        std.debug.assert(ok);
    }

    return headers;
}

pub fn blockHeaderLessThan(_: void, a: BlockHeader, b: BlockHeader) bool {
    return std.mem.lessThan(u8, a.firstEntry, b.firstEntry);
}

pub fn compareToKey(key: []const u8, header: BlockHeader) std.math.Order {
    const order = std.mem.order(u8, key, header.firstEntry);
    return switch (order) {
        .eq => .eq,
        .lt => .eq,
        .gt => .gt,
    };
}

test "BlockHeader encode/decode" {
    const Case = struct {
        bh: BlockHeader,
    };

    const cases = [_]Case{
        // Minimal values
        .{
            .bh = .{
                .firstEntry = "",
                .prefix = "",
                .encodingType = .plain,
                .entriesCount = 0,
                .entriesBlockOffset = 0,
                .lensBlockOffset = 0,
                .entriesBlockSize = 0,
                .lensBlockSize = 0,
            },
        },
        // Small values with short strings
        .{
            .bh = .{
                .firstEntry = "a",
                .prefix = "b",
                .encodingType = .plain,
                .entriesCount = 1,
                .entriesBlockOffset = 100,
                .lensBlockOffset = 200,
                .entriesBlockSize = 50,
                .lensBlockSize = 25,
            },
        },
        // Large values with very long strings (testing varint encoding bounds)
        .{
            .bh = .{
                .firstEntry = "a" ** 127, // Single byte varint
                .prefix = "b" ** 128, // Two byte varint
                .encodingType = .plain,
                .entriesCount = std.math.maxInt(u32),
                .entriesBlockOffset = std.math.maxInt(u64),
                .lensBlockOffset = std.math.maxInt(u64),
                .entriesBlockSize = std.math.maxInt(u32),
                .lensBlockSize = std.math.maxInt(u32),
            },
        },
        // Testing varint string length encoding boundaries
        .{
            .bh = .{
                .firstEntry = "x" ** 16383, // Max two byte varint (0x3fff)
                .prefix = "y" ** 16384, // Three byte varint
                .encodingType = .zstd,
                .entriesCount = 999999,
                .entriesBlockOffset = 1 << 40, // Large offset
                .lensBlockOffset = 1 << 50, // Very large offset
                .entriesBlockSize = 1 << 20, // 1MB
                .lensBlockSize = 1 << 20, // 1MB
            },
        },
    };

    const allocator = std.testing.allocator;

    for (cases) |case| {
        const size = case.bh.bound();
        const buf = try allocator.alloc(u8, size);
        defer allocator.free(buf);

        case.bh.encode(buf);
        const decoded = BlockHeader.decode(buf);

        try std.testing.expectEqualDeep(case.bh, decoded.blockHeader);
    }
}

test "BlockHeader decodeMany" {
    const allocator = std.testing.allocator;

    const headers = [_]BlockHeader{
        .{
            .firstEntry = "aaa",
            .prefix = "a",
            .encodingType = .plain,
            .entriesCount = 10,
            .entriesBlockOffset = 0,
            .lensBlockOffset = 100,
            .entriesBlockSize = 50,
            .lensBlockSize = 25,
        },
        .{
            .firstEntry = "bbb",
            .prefix = "b",
            .encodingType = .zstd,
            .entriesCount = 20,
            .entriesBlockOffset = 200,
            .lensBlockOffset = 300,
            .entriesBlockSize = 75,
            .lensBlockSize = 40,
        },
        .{
            .firstEntry = "ccc",
            .prefix = "c",
            .encodingType = .plain,
            .entriesCount = 30,
            .entriesBlockOffset = 400,
            .lensBlockOffset = 500,
            .entriesBlockSize = 100,
            .lensBlockSize = 60,
        },
    };

    var totalSize: usize = 0;
    for (&headers) |*h| totalSize += h.bound();

    const buf = try allocator.alloc(u8, totalSize);
    defer allocator.free(buf);

    var offset: usize = 0;
    for (&headers) |*h| {
        h.encode(buf[offset..]);
        offset += h.bound();
    }

    const decoded = try decodeMany(allocator, buf, headers.len);
    defer allocator.free(decoded);

    try std.testing.expectEqual(headers.len, decoded.len);
    for (headers, decoded) |expected, actual| {
        try std.testing.expectEqualDeep(expected, actual);
    }
}
