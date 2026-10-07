const std = @import("std");
const Allocator = std.mem.Allocator;
const Io = std.Io;

const tracy = @import("tracy");

const encoding = @import("encoding");

const SID = @import("../lines.zig").SID;
const Block = @import("Block.zig");
const BlockData = @import("BlockData.zig").BlockData;
const Encoder = @import("encoding").Encoder;
const Decoder = @import("encoding").Decoder;
const IndexBlockHeader = @import("IndexBlockHeader.zig");
const DecompressionPool = @import("../compression/DecompressionPool.zig");
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

pub fn initFromBlock(block: *const Block, sid: SID) BlockHeader {
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
    const z = tracy.Zone.begin(.{
        .src = @src(),
        .name = "BlockHeader.encode",
    });
    defer z.end();

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

    validateBlockHeaders(dst.items[dstLen..]);
}

pub fn decodeIndexWindow(
    io: Io,
    alloc: Allocator,
    decompressionPool: *DecompressionPool,
    dst: *std.ArrayList(BlockHeader),
    src: []const u8,
    index: IndexBlockHeader,
) !void {
    std.debug.assert(index.size <= IndexBlockHeader.maxIndexBlockSize);

    // src is the compressed index window described by index.
    std.debug.assert(src.len == index.size);
    const decompressedSize = try encoding.getFrameContentSize(src);

    var decompressedBuf = try alloc.alloc(u8, decompressedSize);
    defer alloc.free(decompressedBuf);

    const n = try decompressionPool.decompress(io, decompressedBuf, src);
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
