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
    // TODO: rename all uncompress* to decompress*
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
