const std = @import("std");
const builtin = @import("builtin");
const Allocator = std.mem.Allocator;
const Io = std.Io;

const encoding = @import("encoding");

const SID = @import("../lines.zig").SID;
const Block = @import("Block.zig");
const BlockData = @import("BlockData.zig").BlockData;
const Encoder = @import("encoding").Encoder;
const Decoder = @import("encoding").Decoder;
const IndexBlockHeader = @import("IndexBlockHeader.zig");
const CompressionPool = @import("../CompressionPool.zig").CompressionPool;
const DecompressionPool = @import("../CompressionPool.zig").DecompressionPool;
const EncodingType = @import("TimestampsEncoder.zig").EncodingType;

pub const TimestampsHeader = struct {
    offset: u64,
    size: u64,
    min: u64,
    max: u64,

    encodingType: EncodingType,

    pub fn encode(self: *const TimestampsHeader, enc: *Encoder) void {
        enc.writeInt(u64, self.offset);
        enc.writeInt(u64, self.size);
        enc.writeInt(u64, self.min);
        enc.writeInt(u64, self.max);
        enc.writeInt(u8, @intFromEnum(self.encodingType));
    }

    pub fn decode(decoder: *Decoder) TimestampsHeader {
        const offset = decoder.readInt(u64);
        const size = decoder.readInt(u64);
        const min = decoder.readInt(u64);
        const max = decoder.readInt(u64);
        const encodingType = decoder.readInt(u8);

        return .{
            .offset = offset,
            .size = size,
            .min = min,
            .max = max,
            .encodingType = @enumFromInt(encodingType),
        };
    }
};

pub const BlockHeader = @This();
sid: SID,
size: u32,
len: u32,
timestampsHeader: TimestampsHeader,

columnsHeaderOffset: usize,
columnsHeaderSize: usize,
columnsHeaderIndexOffset: usize,
columnsHeaderIndexSize: usize,

pub fn initFromBlock(block: *Block, sid: SID) BlockHeader {
    return .{
        .sid = sid,
        .size = block.size(),
        .len = @intCast(block.len()),
        .timestampsHeader = .{
            .offset = 0,
            .size = 0,
            .min = 0,
            .max = 0,
            .encodingType = EncodingType.Undefined,
        },
        .columnsHeaderOffset = 0,
        .columnsHeaderSize = 0,
        .columnsHeaderIndexOffset = 0,
        .columnsHeaderIndexSize = 0,
    };
}

pub fn initFromData(data: *const BlockData, sid: SID) BlockHeader {
    return .{
        .sid = sid,
        .size = @intCast(data.uncompressedSizeBytes),
        .len = data.len,
        .timestampsHeader = .{
            .offset = 0,
            .size = 0,
            .min = 0,
            .max = 0,
            .encodingType = EncodingType.Undefined,
        },
        .columnsHeaderOffset = 0,
        .columnsHeaderSize = 0,
        .columnsHeaderIndexOffset = 0,
        .columnsHeaderIndexSize = 0,
    };
}

// [24:sid][4:size][4:len][33:timestamps, 32 values and 1 encoding type][40:columns header]
pub const encodeExpectedSize = SID.encodeBound + 4 + 4 + 32 + 1 + 40;

pub fn encode(self: *const BlockHeader, buf: []u8) usize {
    var enc = Encoder.init(buf);

    self.sid.encode(&enc);

    enc.writeInt(u32, self.size);
    enc.writeInt(u32, self.len);

    self.timestampsHeader.encode(&enc);

    enc.writeVarInt(self.columnsHeaderIndexOffset);
    enc.writeVarInt(self.columnsHeaderIndexSize);
    enc.writeVarInt(self.columnsHeaderOffset);
    enc.writeVarInt(self.columnsHeaderSize);

    return enc.offset;
}

pub fn decode(buf: []const u8) struct { header: BlockHeader, offset: usize } {
    var decoder = Decoder.init(buf);

    const sid = SID.decode(decoder.readBytes(SID.encodeBound));

    const size = decoder.readInt(u32);
    const len = decoder.readInt(u32);

    const timestampsHeader = TimestampsHeader.decode(&decoder);

    const columnsHeaderIndexOffset = decoder.readVarInt();
    const columnsHeaderIndexSize = decoder.readVarInt();
    const columnsHeaderOffset = decoder.readVarInt();
    const columnsHeaderSize = decoder.readVarInt();

    return .{
        .header = .{
            .sid = sid,
            .size = size,
            .len = len,
            .timestampsHeader = timestampsHeader,
            .columnsHeaderOffset = columnsHeaderOffset,
            .columnsHeaderSize = columnsHeaderSize,
            .columnsHeaderIndexOffset = columnsHeaderIndexOffset,
            .columnsHeaderIndexSize = columnsHeaderIndexSize,
        },
        .offset = decoder.offset,
    };
}

pub fn decodeFew(
    allocator: Allocator,
    dst: *std.ArrayList(BlockHeader),
    src: []const u8,
) !void {
    const dstLen = dst.items.len;
    errdefer dst.shrinkRetainingCapacity(dstLen);
    var buf = src;

    while (buf.len > 0) {
        const res = BlockHeader.decode(buf);
        try dst.append(allocator, res.header);
        buf = buf[res.offset..];
    }

    if (builtin.mode == .Debug) validateBlockHeaders(dst.items[dstLen..]);
}

pub fn decodeIndexWindow(
    io: Io,
    alloc: Allocator,
    compressionPool: anytype,
    dst: *std.ArrayList(BlockHeader),
    src: []const u8,
    index: IndexBlockHeader,
) !void {
    std.debug.assert(index.size <= IndexBlockHeader.maxIndexBlockSize);

    // src is the compressed index window described by index.
    std.debug.assert(src.len == index.size);
    const decompressedSize = try encoding.getFrameContentSize(src);

    // TODO: we probably can put it on stack,
    // since index is passed the window must be narrow enough
    var decompressedBuf = try alloc.alloc(u8, decompressedSize);
    defer alloc.free(decompressedBuf);

    const n = try compressionPool.decompress(io, decompressedBuf, src);
    const decompressed = decompressedBuf[0..n];
    try BlockHeader.decodeFew(alloc, dst, decompressed);
}

pub fn validateBlockHeaders(bhs: []const BlockHeader) void {
    if (bhs.len < 2) return;

    for (1..bhs.len) |i| {
        const curr = &bhs[i];
        const prev = &bhs[i - 1];

        std.debug.assert(!curr.sid.lessThan(prev.sid));

        if (!curr.sid.eql(prev.sid)) {
            continue;
        }

        const th_curr = curr.timestampsHeader;
        const th_prev = prev.timestampsHeader;

        std.debug.assert(th_curr.min >= th_prev.min);
    }
}

test "BlockHeader encode/decode and decodeIndexWindow" {
    const alloc = std.testing.allocator;

    const Case = struct {
        header: BlockHeader,
        expectedLen: usize,
    };

    const cases = [_]Case{
        .{
            .header = .{
                .sid = .{
                    .tenantID = 1,
                    .id = 1,
                },
                .size = 0,
                .len = 0,
                .timestampsHeader = .{
                    .offset = 0,
                    .size = 0,
                    .min = 0,
                    .max = 0,
                    .encodingType = .Undefined,
                },
                .columnsHeaderOffset = 0,
                .columnsHeaderSize = 0,
                .columnsHeaderIndexOffset = 0,
                .columnsHeaderIndexSize = 0,
            },
            .expectedLen = 69,
        },
        .{
            .header = .{
                .sid = .{
                    .tenantID = 1,
                    .id = 42,
                },
                .size = 1234,
                .len = 123,
                .timestampsHeader = .{
                    .offset = 1,
                    .size = 2,
                    .min = 50,
                    .max = 100,
                    .encodingType = .ZDeltapack,
                },
                .columnsHeaderOffset = 10,
                .columnsHeaderSize = 20,
                .columnsHeaderIndexOffset = 30,
                .columnsHeaderIndexSize = 40,
            },
            .expectedLen = 69,
        },
        .{
            .header = .{
                .sid = .{
                    .tenantID = 1,
                    .id = std.math.maxInt(u128),
                },
                .size = std.math.maxInt(u32),
                .len = std.math.maxInt(u32),
                .timestampsHeader = .{
                    .offset = std.math.maxInt(u64),
                    .size = std.math.maxInt(u64),
                    .min = std.math.maxInt(u64),
                    .max = std.math.maxInt(u64),
                    .encodingType = EncodingType.ZDeltapack,
                },
                .columnsHeaderOffset = std.math.maxInt(usize),
                .columnsHeaderSize = std.math.maxInt(usize),
                .columnsHeaderIndexOffset = std.math.maxInt(usize),
                .columnsHeaderIndexSize = std.math.maxInt(usize),
            },
            .expectedLen = BlockHeader.encodeExpectedSize,
        },
    };

    var headers: [cases.len]BlockHeader = undefined;
    for (cases, 0..) |case, i| {
        var encodeBuf: [BlockHeader.encodeExpectedSize]u8 = undefined;
        const offset = case.header.encode(&encodeBuf);
        try std.testing.expectEqual(case.expectedLen, offset);

        const decoded = BlockHeader.decode(encodeBuf[0..offset]);
        try std.testing.expectEqual(offset, decoded.offset);
        try std.testing.expectEqualDeep(case.header, decoded.header);

        headers[i] = case.header;
    }

    var indexBuf = std.ArrayList(u8).empty;
    defer indexBuf.deinit(alloc);

    const WindowCase = struct {
        offset: u64,
        size: u64,
        expected: []const BlockHeader,
    };

    var windowCases = [_]WindowCase{
        .{ .offset = 0, .size = 0, .expected = headers[0..2] },
        .{ .offset = 0, .size = 0, .expected = headers[1..3] },
        .{ .offset = 0, .size = 0, .expected = headers[1..2] },
        .{ .offset = 0, .size = 0, .expected = headers[2..3] },
    };
    const compressionPool = try CompressionPool.init(alloc, 1);
    defer compressionPool.deinit(alloc);

    for (&windowCases) |*windowCase| {
        const rawLen = windowCase.expected.len * BlockHeader.encodeExpectedSize;
        const raw = try alloc.alloc(u8, rawLen);
        defer alloc.free(raw);

        var off: usize = 0;
        for (windowCase.expected) |header| {
            const n = header.encode(raw[off .. off + BlockHeader.encodeExpectedSize]);
            off += n;
        }

        const bound = try encoding.compressBound(off);
        const compressed = try alloc.alloc(u8, bound);
        defer alloc.free(compressed);

        windowCase.offset = @intCast(indexBuf.items.len);
        const n = try compressionPool.compressAuto(std.testing.io, compressed, raw[0..off]);
        windowCase.size = @intCast(n);
        try indexBuf.appendSlice(alloc, compressed[0..n]);
    }

    for (windowCases) |windowCase| {
        var decoded = try std.ArrayList(BlockHeader).initCapacity(alloc, 4);
        defer decoded.deinit(alloc);

        const window = IndexBlockHeader{
            .sid = windowCase.expected[0].sid,
            .minTs = windowCase.expected[0].timestampsHeader.min,
            .maxTs = windowCase.expected[windowCase.expected.len - 1].timestampsHeader.max,
            .offset = windowCase.offset,
            .size = windowCase.size,
        };

        const start: usize = @intCast(windowCase.offset);
        const end = start + windowCase.size;
        try BlockHeader.decodeIndexWindow(std.testing.io, alloc, compressionPool, &decoded, indexBuf.items[start..end], window);
        try std.testing.expectEqualDeep(windowCase.expected, decoded.items);
    }
}
