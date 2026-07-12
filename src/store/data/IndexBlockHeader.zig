const std = @import("std");
const Allocator = std.mem.Allocator;
const Io = std.Io;

const SID = @import("../lines.zig").SID;
const TableWriter = @import("TableWriter.zig");
const encoding = @import("encoding");
const Encoder = encoding.Encoder;
const Decoder = encoding.Decoder;
const CompressionPool = @import("../CompressionPool.zig").CompressionPool;
const DecompressionPool = @import("../CompressionPool.zig").DecompressionPool;

const Self = @This();

// Maximum size for an index block (8MB)
pub const maxIndexBlockSize: u64 = 8 * 1024 * 1024;

sid: SID = .{ .tenantID = 0, .id = 0 },
minTs: u64 = 0,
maxTs: u64 = 0,

offset: u64 = 0,
size: u64 = 0,

pub fn writeIndexBlock(
    self: *Self,
    io: Io,
    allocator: Allocator,
    indexBlockBuf: *std.ArrayList(u8),
    sid: SID,
    minBlockTimestamp: u64,
    maxBlockTimestamp: u64,
    streamWriter: *TableWriter,
) !void {
    if (indexBlockBuf.items.len == 0) {
        return;
    }

    const compressBound = try encoding.compressBound(indexBlockBuf.items.len);
    const compressed = try streamWriter.indexDst.allocSlice(allocator, compressBound);

    const offset = try streamWriter.compressionPool.compressAuto(io, compressed, indexBlockBuf.items);
    const len = streamWriter.indexDst.len();
    try streamWriter.indexDst.appendAllocated(io, compressed, offset);

    self.offset = len;
    self.size = offset;
    self.sid = sid;
    self.minTs = minBlockTimestamp;
    self.maxTs = maxBlockTimestamp;
}

// sid 24 + self 32 = 56
pub const encodeExpectedSize = 56;
pub fn encode(self: Self, buf: []u8) usize {
    var enc = Encoder.init(buf);
    self.sid.encode(&enc);
    enc.writeInt(u64, self.minTs);
    enc.writeInt(u64, self.maxTs);
    enc.writeInt(u64, self.offset);
    enc.writeInt(u64, self.size);
    return enc.offset;
}

pub fn decode(buf: []const u8) Self {
    var decoder = Decoder.init(buf);
    const sid = SID.decode(buf);
    decoder.offset += SID.encodeBound;
    const minTs = decoder.readInt(u64);
    const maxTs = decoder.readInt(u64);
    const offset = decoder.readInt(u64);
    const size = decoder.readInt(u64);
    return .{
        .sid = sid,
        .minTs = minTs,
        .maxTs = maxTs,
        .offset = offset,
        .size = size,
    };
}

pub fn readIndexBlockHeaders(
    io: Io,
    allocator: Allocator,
    compressionPool: anytype,
    compressed: []const u8,
) ![]Self {
    const decompressedSize = try encoding.getFrameContentSize(compressed);

    var decompressedBuf = try allocator.alloc(u8, decompressedSize);
    defer allocator.free(decompressedBuf);

    const n = try compressionPool.decompress(io, decompressedBuf, compressed);
    const decompressed = decompressedBuf[0..n];

    std.debug.assert(decompressed.len % encodeExpectedSize == 0);

    const count = decompressed.len / encodeExpectedSize;

    var dst = try allocator.alloc(Self, count);
    var i: usize = 0;
    errdefer allocator.free(dst);

    var off: usize = 0;
    while (off < decompressed.len) : ({
        off += encodeExpectedSize;
        i += 1;
    }) {
        dst[i] = decode(decompressed[off .. off + encodeExpectedSize]);
    }

    validateIndexBlockHeaders(dst);

    return dst;
}

// TODO: consider to move it under builtin.is_test condition or have it in the testing.assert,
// have it in release safe also must be vaible
fn validateIndexBlockHeaders(headers: []const Self) void {
    for (1..headers.len) |i| {
        std.debug.assert(!headers[i].sid.lessThan(headers[i - 1].sid));
    }
}

test "IndexBlockHeaderEncode" {
    const Case = struct {
        header: Self,
        expectedLen: usize,
    };

    const cases = &[_]Case{
        .{
            .header = .{
                .sid = .{
                    .tenantID = 42,
                    .id = 42,
                },
                .minTs = 100,
                .maxTs = 200,
                .offset = 1,
                .size = 1234,
            },
            .expectedLen = encodeExpectedSize,
        },
        .{
            .header = std.mem.zeroInit(Self, .{}),
            .expectedLen = encodeExpectedSize,
        },
        .{
            .header = .{
                .sid = .{
                    .tenantID = 42,
                    .id = std.math.maxInt(u128),
                },
                .minTs = std.math.maxInt(u64),
                .maxTs = std.math.maxInt(u64),
                .offset = std.math.maxInt(u64),
                .size = std.math.maxInt(u64),
            },
            .expectedLen = encodeExpectedSize,
        },
    };

    for (cases) |case| {
        var encodeBuf: [encodeExpectedSize]u8 = undefined;
        const offset = case.header.encode(&encodeBuf);
        try std.testing.expectEqual(case.expectedLen, offset);

        const h = Self.decode(encodeBuf[0..offset]);
        try std.testing.expectEqualDeep(case.header, h);
    }
}
