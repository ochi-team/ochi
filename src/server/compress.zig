const std = @import("std");
const string = []const u8;
const snappy = @import("snappy");

// TODO: support gzip to cover loki fully
// TODO: suport the other compressions types here like zstd, datadog, etc.
pub const Compress = enum(u2) {
    snappy,
    pub fn fromEncoding(encoding: string) !Compress {
        if (std.mem.eql(u8, encoding, "snappy")) {
            return Compress.snappy;
        }

        return error.CompressingNotSupported;
    }
    pub fn uncompress(compress: Compress, allocator: std.mem.Allocator, compressed: string) !string {
        return switch (compress) {
            .snappy => {
                  const uncompressed = try allocator.alloc(u8, try snappy.uncompressedLength(compressed[0..]));
                 // TODO: consider if arena requires errdefer allocator.free(uncompressed);
                 _ = try snappy.uncompress(compressed[0..], uncompressed);
                 return uncompressed;
            },
        };
    }

};

