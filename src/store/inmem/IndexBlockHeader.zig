const std = @import("std");

const SID = @import("../lines.zig").SID;
const StreamWriter = @import("StreamWriter.zig");
const encoding = @import("encoding");
const Encoder = encoding.Encoder;
const Decoder = encoding.Decoder;

const Self = @This();

// Maximum size for an index block (8MB)
pub const maxIndexBlockSize: u64 = 8 * 1024 * 1024;

sid: SID,
minTs: u64,
maxTs: u64,

offset: u64,
size: u64,

pub fn init(allocator: std.mem.Allocator) !*Self {
    // TODO: test if it can be used by value, doesn't seem it needs an allocator
    const bh = try allocator.create(Self);
    bh.* = .{
        .sid = .{ .tenantID = "", .id = 0 },
        .minTs = 0,
        .maxTs = 0,
        .offset = 0,
        .size = 0,
    };
    return bh;
}

pub fn deinit(self: *Self, allocator: std.mem.Allocator) void {
    self.deinitRead(allocator);
    allocator.destroy(self);
}

pub fn deinitRead(self: *Self, allocator: std.mem.Allocator) void {
    self.sid.deinit(allocator);
}

pub fn writeIndexBlock(
    self: *Self,
    allocator: std.mem.Allocator,
    indexBlockBuf: *std.ArrayList(u8),
    sid: SID,
    minTs: u64,
    maxTs: u64,
    streamWriter: *StreamWriter,
) !void {
    if (indexBlockBuf.items.len == 0) {
        return;
    }

    self.sid = sid;
    self.minTs = minTs;
    self.maxTs = maxTs;

    const compressBound = try encoding.compressBound(indexBlockBuf.items.len);
    const buf = try allocator.alloc(u8, compressBound);
    defer allocator.free(buf);
    const offset = try encoding.compressAuto(buf, indexBlockBuf.items);
    self.offset = streamWriter.indexBuf.items.len;
    self.size = offset;

    try streamWriter.indexBuf.appendSlice(allocator, buf[0..offset]);
}

// sid 32 + self 32 = 64
pub const encodeExpectedSize = 64;
pub fn encode(self: *const Self, buf: []u8) usize {
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
    decoder.offset += 32; // SID is 32 bytes
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

// TODO: unify the way how we use SID
pub fn decodeAlloc(allocator: std.mem.Allocator, buf: []const u8) !Self {
    var decoder = Decoder.init(buf);
    const sid = try SID.decodeAlloc(allocator, buf);
    decoder.offset += 32; // SID is 32 bytes
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
    allocator: std.mem.Allocator,
    compressed: []const u8,
) ![]Self {
    const decompressedSize = try encoding.getFrameContentSize(compressed);

    var decompressedBuf = try allocator.alloc(u8, decompressedSize);
    defer allocator.free(decompressedBuf);

    const n = try encoding.decompress(
        decompressedBuf,
        compressed,
    );
    const decompressed = decompressedBuf[0..n];

    std.debug.assert(decompressed.len % encodeExpectedSize == 0);

    const count = decompressed.len / encodeExpectedSize;

    var dst = try allocator.alloc(Self, count);
    var i: usize = 0;
    errdefer {
        for (dst[0..i]) |*reader| reader.deinitRead(allocator);
        allocator.free(dst);
    }

    var off: usize = 0;
    while (off < decompressed.len) : ({
        off += encodeExpectedSize;
        i += 1;
    }) {
        dst[i] = try decodeAlloc(allocator, decompressed[off .. off + encodeExpectedSize]);
    }

    validateIndexBlockHeaders(dst);

    return dst;
}

// TODO: consider to move it under builtin.is_test condition
fn validateIndexBlockHeaders(headers: []const Self) void {
    for (1..headers.len) |i| {
        std.debug.assert(!headers[i].sid.lessThan(&headers[i - 1].sid));
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
                    .tenantID = "tenant",
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
                    .tenantID = "tenant",
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
