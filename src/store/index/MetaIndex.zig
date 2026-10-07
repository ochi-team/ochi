const std = @import("std");
const Allocator = std.mem.Allocator;
const Io = std.Io;
const Dir = Io.Dir;

const Logger = @import("logging");
const encoding = @import("encoding");
const Decoder = encoding.Decoder;
const Encoder = encoding.Encoder;

const filenames = @import("../../filenames.zig");
const DecompressionPool = @import("../compression/DecompressionPool.zig");

const MetaIndex = @This();

pub const DecodedMetaIndex = struct {
    records: []MetaIndex,
    compressedSize: u64,
};

// TODO: make the json fields tiny bit shorter to consume less space on reading,
// after that knowing the entry value limit we can know in advance how much buffer we need to decode it
firstEntry: []const u8 = "",
blockHeadersCount: u32 = 0,
indexBlockOffset: u64 = 0,
indexBlockSize: u32 = 0,

pub fn deinit(self: *const MetaIndex, alloc: Allocator) void {
    alloc.free(self.firstEntry);
}

pub fn reset(self: *MetaIndex) void {
    self.* = .{};
}

// [firstItem.len:firstItem][4:count][8:offset][4:size] = firstItem.len + lenBound + 16
// TODO: we know the max entry size, so we must implement a bound as a const
// to give a static buffer size
pub fn bound(self: *const MetaIndex) usize {
    const firstItemBound = Encoder.varIntBound(self.firstEntry.len);
    return firstItemBound + self.firstEntry.len + 16;
}

pub fn encode(self: *const MetaIndex, buf: []u8) void {
    var enc = Encoder.init(buf);

    enc.writeString(self.firstEntry);
    enc.writeInt(u32, self.blockHeadersCount);
    enc.writeInt(u64, self.indexBlockOffset);
    enc.writeInt(u32, self.indexBlockSize);
}

pub fn encodeAlloc(self: *const MetaIndex, alloc: Allocator) ![]u8 {
    const buf = try alloc.alloc(u8, self.bound());
    errdefer alloc.free(buf);

    self.encode(buf);

    return buf;
}

pub fn decode(self: *MetaIndex, alloc: Allocator, buf: []u8) !usize {
    var dec = Decoder.init(buf);
    self.firstEntry = try alloc.dupe(u8, dec.readString());
    self.blockHeadersCount = dec.readInt(u32);
    self.indexBlockOffset = dec.readInt(u64);
    self.indexBlockSize = dec.readInt(u32);
    std.debug.assert(self.blockHeadersCount > 0);
    return dec.offset;
}

pub fn readFile(
    io: Io,
    alloc: Allocator,
    decompressionPool: *DecompressionPool,
    path: []const u8,
    blocksCount: u64,
) !DecodedMetaIndex {
    var fba = std.heap.stackFallback(256, alloc);
    const fbaAlloc = fba.get();

    var metaindexPathBuf: [std.fs.max_name_bytes]u8 = undefined;
    var pathWriter = std.Io.Writer.fixed(&metaindexPathBuf);
    try std.fs.path.fmtJoin(&.{ path, filenames.metaindex }).format(&pathWriter);
    const metaindexPath = pathWriter.buffer[0..pathWriter.end];
    var metaindexFile = try Dir.openFileAbsolute(io, metaindexPath, .{});
    defer metaindexFile.close(io);
    const metaindexStat = try metaindexFile.stat(io);

    var metaindexFileReader = metaindexFile.reader(io, &.{});
    const metaindexCompressed = try metaindexFileReader.interface.allocRemaining(fbaAlloc, .unlimited);
    defer fbaAlloc.free(metaindexCompressed);
    // TODO: why does .limited(metaindexStat.size) not work above?
    std.debug.assert(metaindexStat.size == metaindexCompressed.len);

    const decodedMetaindex = try MetaIndex.decodeDecompress(
        io,
        alloc,
        decompressionPool,
        metaindexCompressed,
        blocksCount,
    );
    std.debug.assert(decodedMetaindex.compressedSize == metaindexStat.size);
    return decodedMetaindex;
}

pub fn decodeDecompress(
    io: Io,
    alloc: Allocator,
    decompressionPool: *DecompressionPool,
    compressed: []const u8,
    blocksCount: u64,
) !DecodedMetaIndex {
    const metaindexBufSize = try encoding.getFrameContentSize(compressed);
    Logger.log(.debug, "decompressing index.metaindex buffer", .{ .size = metaindexBufSize });

    const metaindexBuf = try alloc.alloc(u8, metaindexBufSize);
    defer alloc.free(metaindexBuf);
    const metaindexLen = try decompressionPool.decompress(io, metaindexBuf, compressed);

    var records = try std.ArrayList(MetaIndex).initCapacity(alloc, @intCast(blocksCount));
    errdefer {
        for (records.items) |*rec| {
            rec.deinit(alloc);
        }
        records.deinit(alloc);
    }

    var slice = metaindexBuf[0..metaindexLen];
    var totalBlockHeaders: u64 = 0;
    while (slice.len > 0) {
        var rec: MetaIndex = undefined;
        const n = try rec.decode(alloc, slice);
        slice = slice[n..];
        totalBlockHeaders += rec.blockHeadersCount;
        try records.append(alloc, rec);
    }
    std.debug.assert(totalBlockHeaders == blocksCount);

    // TODO: find a way to get rid of all toOwnedSlice calls,
    // it does remap inside which might fail, if so - it copies the data,
    // if the incoming allocator is arena, very likely, it's double wasted memory
    const recordsOwned = try records.toOwnedSlice(alloc);
    return .{
        .records = recordsOwned,
        .compressedSize = compressed.len,
    };
}

pub fn lessThan(_: void, one: MetaIndex, another: MetaIndex) bool {
    return std.mem.lessThan(u8, one.firstEntry, another.firstEntry);
}

pub fn compareToKey(key: []const u8, record: MetaIndex) std.math.Order {
    const order = std.mem.order(u8, key, record.firstEntry);
    return switch (order) {
        .eq => .eq,
        .lt => .eq,
        .gt => .gt,
    };
}
