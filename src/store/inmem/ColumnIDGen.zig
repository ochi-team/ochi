const std = @import("std");
const Allocator = std.mem.Allocator;

const encoding = @import("encoding");

const ColumnIDGen = @This();

// keyIDs must use StringArrayHashMap, it's important to store the keys ordered
keyIDs: std.array_hash_map.String(u16),
keysBuf: ?[]u8,

pub fn init(allocator: Allocator) !*ColumnIDGen {
    const s = try allocator.create(ColumnIDGen);
    errdefer s.deinit(allocator);

    s.* = ColumnIDGen{
        .keyIDs = .empty,
        .keysBuf = null,
    };
    return s;
}

pub fn deinit(self: *ColumnIDGen, allocator: Allocator) void {
    if (self.keysBuf != null) {
        allocator.free(self.keysBuf.?);
    }
    self.keyIDs.deinit(allocator);
    allocator.destroy(self);
}

pub fn genIDAssumeCapacity(self: *ColumnIDGen, key: []const u8) u16 {
    const maybeID = self.keyIDs.get(key);
    if (maybeID) |id| {
        return id;
    }

    const id: u16 = @intCast(self.keyIDs.count());
    self.keyIDs.putAssumeCapacity(key, id);
    return id;
}

pub fn bound(self: *ColumnIDGen) !usize {
    var res: usize = 10;
    for (self.keyIDs.keys()) |key| {
        res += key.len;
    }
    return encoding.compressBound(res);
}

// [10:len][keys]
pub fn encode(self: *ColumnIDGen, alloc: Allocator, dst: []u8) !usize {
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

pub fn decode(alloc: Allocator, src: []const u8) !*ColumnIDGen {
    const size = try encoding.getFrameContentSize(src);

    const buf = try alloc.alloc(u8, size);
    errdefer alloc.free(buf);
    const offset = try encoding.decompress(buf, src);

    const genSize = encoding.Decoder.readVarIntFromBuf(buf);
    const keysBuf = buf[genSize.offset..offset];
    var dec = encoding.Decoder.init(keysBuf);

    const gen = try ColumnIDGen.init(alloc);
    errdefer gen.deinit(alloc);

    try gen.keyIDs.ensureUnusedCapacity(alloc, @intCast(genSize.value));
    for (0..genSize.value) |_| {
        const key = dec.readString();
        _ = gen.genIDAssumeCapacity(key);
    }

    // assign the buf ownership only after all the potential errors
    gen.keysBuf = buf;
    return gen;
}

pub fn decodeColumnIdxs(columnIDGen: *ColumnIDGen, alloc: Allocator, src: []const u8) !std.StringHashMapUnmanaged(u16) {
    var columnIdxs = std.StringHashMapUnmanaged(u16){};
    errdefer columnIdxs.deinit(alloc);

    var dec = encoding.Decoder.init(src);

    const count = dec.readVarInt();
    try columnIdxs.ensureTotalCapacity(alloc, @intCast(count));

    const keys = columnIDGen.keyIDs.keys();
    for (0..count) |_| {
        const colID: u16 = @intCast(dec.readVarInt());
        const shardIdx: u16 = @intCast(dec.readVarInt());

        std.debug.assert(colID < keys.len);
        const colName = keys[colID];
        columnIdxs.putAssumeCapacity(colName, shardIdx);
    }

    std.debug.assert(dec.offset == src.len);
    return columnIdxs;
}

test "ColumnIDGen" {
    const alloc = std.testing.allocator;
    const gener = try ColumnIDGen.init(alloc);
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

    const generDecoded = try ColumnIDGen.decode(alloc, encoded[0..offset]);
    defer generDecoded.deinit(alloc);

    try std.testing.expectEqual(gener.keyIDs.count(), generDecoded.keyIDs.count());
    for (gener.keyIDs.keys()) |key| {
        const value = gener.keyIDs.get(key);
        const decodedValue = generDecoded.keyIDs.get(key);
        try std.testing.expectEqual(value, decodedValue);
    }
}
