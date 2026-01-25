const std = @import("std");
const Allocator = std.mem.Allocator;

const encoding = @import("encoding");
const Encoder = encoding.Encoder;

const MetaIndex = @This();

firstItem: []const u8 = "",
blockHeadersCount: u32 = 0,
indexBlockOffset: u64 = 0,
indexBlockSize: u32 = 0,

pub fn reset(self: *MetaIndex) void {
    self.* = .{};
}

// [firstItem.len:firstItem][4:count][8:offset][4:size] = firstItem.len + lenBound + 16
pub fn bound(self: *const MetaIndex) usize {
    const firstItemBound = Encoder.varIntBound(self.firstItem.len);
    return firstItemBound + self.firstItem.len + 16;
}

pub fn encode(self: *const MetaIndex, buf: []u8) void {
    var enc = Encoder.init(buf);

    enc.writeString(self.firstItem);
    enc.writeInt(u32, self.blockHeadersCount);
    enc.writeInt(u64, self.indexBlockOffset);
    enc.writeInt(u32, self.indexBlockSize);
}

pub fn encodeAlloc(self: *const MetaIndex, alloc: Allocator) ![]u8 {
    const buf = try alloc.alloc(u8, self.bound());

    self.encode(buf);

    return buf;
}
