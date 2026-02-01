const std = @import("std");
const Allocator = std.mem.Allocator;

const encoding = @import("encoding");
const Decoder = encoding.Decoder;

const MetaIndexRecord = @This();

firstItem: []const u8,
blockHeadersCount: u32,
indexBlockOffset: u64,
indexBlockSize: u32,

pub fn decode(self: *MetaIndexRecord, buf: []u8) usize {
    var dec = Decoder.init(buf);
    self.firstItem = dec.readString();
    self.blockHeadersCount = dec.readInt(u32);
    self.indexBlockOffset = dec.readInt(u64);
    self.indexBlockSize = dec.readInt(u32);
    std.debug.assert(self.blockHeadersCount > 0);
    return dec.offset;
}

pub fn lessThan(_: void, one: MetaIndexRecord, another: MetaIndexRecord) bool {
    return std.mem.lessThan(u8, one.firstItem, another.firstItem);
}
