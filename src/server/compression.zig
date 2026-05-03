const std = @import("std");
const snappy = @import("snappy").raw;

// TODO: support gzip to cover loki fully
// TODO: suport the other compressions types here like zstd, datadog, etc.
// TODO: support streaming decompression together with unmarshalling, so we pass a reader
// to a json/proto marshaller and don't keep the full uncompressed buffer in memory
pub const Compression = enum(u8) {
    snappy,
    pub fn fromEncoding(encoding: []const u8) !Compression {
        if (std.mem.eql(u8, encoding, "snappy")) {
            return .snappy;
        }

        return error.CompressingNotSupported;
    }
    pub fn uncompress(compression: Compression, allocator: std.mem.Allocator, compressed: []const u8) ![]const u8 {
        return switch (compression) {
            .snappy => {
                const uncompressed = try allocator.alloc(u8, try snappy.uncompressedLength(compressed[0..]));
                _ = try snappy.uncompress(compressed[0..], uncompressed);
                return uncompressed;
            },
        };
    }
};

test "Snappy Uncompress valid input" {
    const testing = std.testing;
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
    const allocator = std.testing.allocator;
    const bad = [_]u8{ 0xde, 0xad, 0xbe, 0xef };

    try std.testing.expectError(
        error.invalid_input,
        Compression.snappy.uncompress(allocator, &bad),
    );
}
