const std = @import("std");
const Allocator = std.mem.Allocator;

const encoding = @import("encoding");
const Encoder = encoding.Encoder;
const Decoder = encoding.Decoder;

pub const EncodingType = enum(u8) {
    plain = 0,
    zstd = 1,
};

const BlockHeader = @This();

firstItem: []const u8,
prefix: []const u8,
encodingType: EncodingType,
itemsCount: u32 = 0,
itemsBlockOffset: u64 = 0,
lensBlockOffset: u64 = 0,
itemsBlockSize: u32 = 0,
lensBlockSize: u32 = 0,

pub fn reset(self: *BlockHeader) void {
    self.* = .{ .firstItem = undefined, .prefix = undefined, .encodingType = undefined };
}

// [len:n][firstItem:len][len:n][prefix:len][count:4][type:1][offset:8][size:4][offset:8][size:4] = bound + len + 29
pub fn bound(self: *const BlockHeader) usize {
    const firstItemLenBound = Encoder.varIntBound(self.firstItem.len);
    const prefixLenBound = Encoder.varIntBound(self.prefix.len);
    return firstItemLenBound + prefixLenBound + self.firstItem.len + self.prefix.len + 29;
}

pub fn encode(self: *const BlockHeader, buf: []u8) void {
    var enc = Encoder.init(buf);

    enc.writeString(self.firstItem);
    enc.writeString(self.prefix);
    enc.writeInt(u8, @intFromEnum(self.encodingType));
    enc.writeInt(u32, self.itemsCount);
    enc.writeInt(u64, self.itemsBlockOffset);
    enc.writeInt(u64, self.lensBlockOffset);
    enc.writeInt(u32, self.itemsBlockSize);
    enc.writeInt(u32, self.lensBlockSize);
}

pub fn encodeAlloc(self: *const BlockHeader, alloc: Allocator) ![]u8 {
    const size = self.bound();
    const buf = try alloc.alloc(u8, size);
    self.encode(buf);

    return buf;
}

pub fn decode(buf: []const u8) BlockHeader {
    var dec = Decoder.init(buf);

    const firstItem = dec.readString();
    const prefix = dec.readString();
    const encodingType: EncodingType = @enumFromInt(dec.readInt(u8));
    const itemsCount = dec.readInt(u32);
    const itemsBlockOffset = dec.readInt(u64);
    const lensBlockOffset = dec.readInt(u64);
    const itemsBlockSize = dec.readInt(u32);
    const lensBlockSize = dec.readInt(u32);

    return BlockHeader{
        .firstItem = firstItem,
        .prefix = prefix,
        .encodingType = encodingType,
        .itemsCount = itemsCount,
        .itemsBlockOffset = itemsBlockOffset,
        .lensBlockOffset = lensBlockOffset,
        .itemsBlockSize = itemsBlockSize,
        .lensBlockSize = lensBlockSize,
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
                .firstItem = "",
                .prefix = "",
                .encodingType = .plain,
                .itemsCount = 0,
                .itemsBlockOffset = 0,
                .lensBlockOffset = 0,
                .itemsBlockSize = 0,
                .lensBlockSize = 0,
            },
        },
        // Small values with short strings
        .{
            .bh = .{
                .firstItem = "a",
                .prefix = "b",
                .encodingType = .plain,
                .itemsCount = 1,
                .itemsBlockOffset = 100,
                .lensBlockOffset = 200,
                .itemsBlockSize = 50,
                .lensBlockSize = 25,
            },
        },
        // Large values with very long strings (testing varint encoding bounds)
        .{
            .bh = .{
                .firstItem = "a" ** 127, // Single byte varint
                .prefix = "b" ** 128, // Two byte varint
                .encodingType = .plain,
                .itemsCount = std.math.maxInt(u32),
                .itemsBlockOffset = std.math.maxInt(u64),
                .lensBlockOffset = std.math.maxInt(u64),
                .itemsBlockSize = std.math.maxInt(u32),
                .lensBlockSize = std.math.maxInt(u32),
            },
        },
        // Testing varint string length encoding boundaries
        .{
            .bh = .{
                .firstItem = "x" ** 16383, // Max two byte varint (0x3fff)
                .prefix = "y" ** 16384, // Three byte varint
                .encodingType = .zstd,
                .itemsCount = 999999,
                .itemsBlockOffset = 1 << 40, // Large offset
                .lensBlockOffset = 1 << 50, // Very large offset
                .itemsBlockSize = 1 << 20, // 1MB
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

        try std.testing.expectEqualDeep(case.bh, decoded);
    }
}
