const std = @import("std");

const SID = @import("../lines.zig").SID;
const StreamWriter = @import("stream_writer.zig").StreamWriter;
const encoding = @import("encoding");
const Encoder = encoding.Encoder;
const Decoder = encoding.Decoder;

const Self = @This();

sid: SID,
minTs: u64,
maxTs: u64,

offset: u64,
size: u64,

pub fn init(allocator: std.mem.Allocator) !*Self {
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
    allocator.destroy(self);
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
    decoder.offset = 32; // SID is 32 bytes
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
