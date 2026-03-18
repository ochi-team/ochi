const std = @import("std");

const encoding = @import("encoding");

const Self = @This();

keyIDs: std.StringArrayHashMap(u16),
keysBuf: ?[]u8,

pub fn init(allocator: std.mem.Allocator) !*Self {
    const nameIDs = std.StringArrayHashMap(u16).init(allocator);
    const s = try allocator.create(Self);
    s.* = Self{
        .keyIDs = nameIDs,
        .keysBuf = null,
    };
    return s;
}

pub fn deinit(self: *Self, allocator: std.mem.Allocator) void {
    if (self.keysBuf != null) {
        allocator.free(self.keysBuf.?);
    }
    self.keyIDs.deinit();
    allocator.destroy(self);
}

pub fn genIDAssumeCapacity(self: *Self, key: []const u8) u16 {
    const maybeID = self.keyIDs.get(key);
    if (maybeID) |id| {
        return id;
    }

    const id: u16 = @intCast(self.keyIDs.count());
    self.keyIDs.putAssumeCapacity(key, id);
    return id;
}

pub fn bound(self: *Self) !usize {
    var res: usize = 10;
    for (self.keyIDs.keys()) |key| {
        res += key.len;
    }
    return encoding.compressBound(res);
}

// [10:len][keys]
pub fn encode(self: *Self, alloc: std.mem.Allocator, dst: []u8) !usize {
    // TODO: consider interning strings to a list instead of collecting them from the map keys

    var uncompressedSize: usize = 10;
    for (self.keyIDs.keys()) |key| {
        uncompressedSize += key.len;
    }
    var stackFba = std.heap.stackFallback(512, alloc);
    const fba = stackFba.get();
    const tmpBuf = try fba.alloc(u8, uncompressedSize);
    defer fba.free(tmpBuf);

    var enc = encoding.Encoder.init(tmpBuf);
    enc.writeVarInt(@intCast(self.keyIDs.count()));
    for (self.keyIDs.keys()) |key| {
        enc.writeString(key);
    }

    return encoding.compressAuto(dst, tmpBuf[0..enc.offset]);
}

pub fn decode(alloc: std.mem.Allocator, src: []u8) !*Self {
    const size = try encoding.getFrameContentSize(src);

    const buf = try alloc.alloc(u8, size);
    errdefer alloc.free(buf);
    const offset = try encoding.decompress(buf, src);

    const genSize = encoding.Decoder.readVarIntFromBuf(buf);
    const keysBuf = buf[genSize.offset..offset];
    var dec = encoding.Decoder.init(keysBuf);

    const gen = try Self.init(alloc);
    gen.keysBuf = buf;
    errdefer gen.deinit(alloc);

    try gen.keyIDs.ensureUnusedCapacity(@intCast(genSize.value));
    for (0..genSize.value) |_| {
        const key = dec.readString();
        _ = gen.genIDAssumeCapacity(key);
    }

    return gen;
}

test "ColumnIDGen" {
    const alloc = std.testing.allocator;
    const gener = try Self.init(alloc);
    defer gener.deinit(alloc);

    const keys = &[_][]const u8{ "key1", "key2", "", "_--=" };
    try gener.keyIDs.ensureUnusedCapacity(keys.len);
    for (0..keys.len) |i| {
        const id = gener.genIDAssumeCapacity(keys[i]);
        try std.testing.expectEqual(i, id);
    }

    for (0..keys.len) |i| {
        const id = gener.keyIDs.get(keys[i]).?;
        try std.testing.expectEqual(i, id);
    }

    const encodeBound = try gener.bound();
    const encoded = try alloc.alloc(u8, encodeBound);
    defer alloc.free(encoded);
    const offset = try gener.encode(alloc, encoded);

    const generDecoded = try Self.decode(alloc, encoded[0..offset]);
    defer generDecoded.deinit(alloc);

    try std.testing.expectEqual(gener.keyIDs.count(), generDecoded.keyIDs.count());
    for (gener.keyIDs.keys()) |key| {
        const value = gener.keyIDs.get(key);
        const decodedValue = generDecoded.keyIDs.get(key);
        try std.testing.expectEqual(value, decodedValue);
    }
}
