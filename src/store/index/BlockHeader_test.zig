const std = @import("std");

const BlockHeader = @import("BlockHeader.zig");
const decodeMany = BlockHeader.decodeMany;

const testing = std.testing;

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

    const allocator = testing.allocator;

    for (cases) |case| {
        const size = case.bh.bound();
        const buf = try allocator.alloc(u8, size);
        defer allocator.free(buf);

        case.bh.encode(buf);
        const decoded = BlockHeader.decode(buf);

        try testing.expectEqualDeep(case.bh, decoded.blockHeader);
    }
}

test "BlockHeader decodeMany" {
    const allocator = testing.allocator;

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

    try testing.expectEqual(headers.len, decoded.len);
    for (headers, decoded) |expected, actual| {
        try testing.expectEqualDeep(expected, actual);
    }
}
