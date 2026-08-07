const std = @import("std");
const snappy = @import("snappy").raw;

const tracy = @import("tracy");

pub const Compression = enum(u8) {
    snappy,
    gzip,
    none,
    pub fn fromEncoding(encoding: []const u8) !Compression {
        if (std.mem.eql(u8, encoding, "snappy")) {
            return .snappy;
        }

        if (encoding.len == 0) {
            return .none;
        }

        return error.CompressingNotSupported;
    }
    pub fn uncompress(compression: Compression, allocator: std.mem.Allocator, compressed: []const u8) ![]const u8 {
        const z = tracy.Zone.begin(.{
            .src = @src(),
            .name = "uncompress",
        });
        defer z.end();

        return switch (compression) {
            .gzip => {
                z.text("gzip");
                // const bound = try bound(compressed);
                // const uncompressed = try allocator.alloc(u8, bound);
                // try zlib.uncompress(compressed, uncompressed);
                // return uncompressed;
                return error.CompressionNotSupported;
            },
            .snappy => {
                z.text("snappy");
                const bound = try snappy.uncompressedLength(compressed);
                const uncompressed = try allocator.alloc(u8, bound);
                _ = try snappy.uncompress(compressed, uncompressed);
                return uncompressed;
            },
            .none => {
                z.text("none");
                return compressed;
            },
        };
    }
};

const testing = std.testing;

test "Snappy Uncompress valid input" {
    const allocator = testing.allocator;

    const cases = [_][]const u8{
        "Hello, world!",
        "",
        "abc",
    };

    for (cases) |input| {
        const max_len = snappy.maxCompressedLength(input.len);
        const compressed = try allocator.alloc(u8, max_len);
        defer allocator.free(compressed);

        const compressed_len = try snappy.compress(input, compressed);
        const compressed_slice = compressed[0..compressed_len];

        const actual = try Compression.snappy.uncompress(allocator, compressed_slice);
        defer allocator.free(actual);

        try testing.expectEqualStrings(input, actual);
    }
}

test "Snappy Uncompress invalid input" {
    const allocator = testing.allocator;
    const bad = [_]u8{ 0xde, 0xad, 0xbe, 0xef };

    try testing.expectError(
        error.invalid_input,
        Compression.snappy.uncompress(allocator, &bad),
    );
}
