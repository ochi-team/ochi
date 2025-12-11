const std = @import("std");

const SID = @import("../lines.zig").SID;
const StreamWriter = @import("stream_writer.zig").StreamWriter;
const Encoder = @import("encode.zig").Encoder;
const Decoder = @import("encode.zig").Decoder;

const Self = @This();

sid: ?SID,
minTs: u64,
maxTs: u64,

offset: u64,
size: u64,

pub fn init(allocator: std.mem.Allocator) !*Self {
    const bh = try allocator.create(Self);
    bh.* = .{
        .sid = null,
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

pub fn writeIndexBlock(self: *Self, allocator: std.mem.Allocator, indexBlockBuf: *std.ArrayList(u8), sid: SID, minTs: u64, maxTs: u64, streamWriter: *StreamWriter) !void {
    if (indexBlockBuf.items.len == 0) {
        return;
    }

    self.sid = sid;
    self.minTs = minTs;
    self.maxTs = maxTs;

    // TODO: compress zstd or openzl
    self.offset = streamWriter.indexBuf.items.len;
    self.size = indexBlockBuf.items.len;

    try streamWriter.indexBuf.appendSlice(allocator, indexBlockBuf.items);
}

// sid 32 + self 32 = 64
pub const encodeExpectedSize = 64;
pub fn encode(self: *Self, buf: []u8) !usize {
    if (buf.len < encodeExpectedSize) {
        return std.mem.Allocator.Error.OutOfMemory;
    }

    var enc = Encoder.init(buf);
    if (self.sid) |*sid| {
        sid.encode(&enc);
    } else {
        enc.writePadded("", 32);
    }
    enc.writeInt(u64, self.minTs);
    enc.writeInt(u64, self.maxTs);
    enc.writeInt(u64, self.offset);
    enc.writeInt(u64, self.size);
    return enc.offset;
}

pub fn decode(buf: []const u8) !Self {
    if (buf.len < 64) {
        return error.InsufficientBuffer;
    }
    var decoder = Decoder.init(buf);
    const sid = try SID.decode(buf);
    decoder.offset = 32; // SID is 32 bytes
    const minTs = try decoder.readInt(u64);
    const maxTs = try decoder.readInt(u64);
    const offset = try decoder.readInt(u64);
    const size = try decoder.readInt(u64);
    return .{
        .sid = sid,
        .minTs = minTs,
        .maxTs = maxTs,
        .offset = offset,
        .size = size,
    };
}
