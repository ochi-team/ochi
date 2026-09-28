const std = @import("std");

const CompressionPool = @import("../compression/CompressionPool.zig");
const DecompressionPool = @import("../compression/DecompressionPool.zig");

const ColumnIDGen = @import("ColumnIDGen.zig");

const testing = std.testing;

test "ColumnIDGen" {
    const alloc = testing.allocator;
    const gener = try ColumnIDGen.init(alloc);
    defer gener.deinit(alloc);

    const keys = &[_][]const u8{ "key1", "key2", "", "_--=" };
    try gener.keyIDs.ensureUnusedCapacity(alloc, keys.len);
    for (0..keys.len) |i| {
        const id = gener.genIDAssumeCapacity(keys[i]);
        try testing.expectEqual(i, id);
    }

    for (0..keys.len) |i| {
        const id = gener.keyIDs.get(keys[i]).?;
        try testing.expectEqual(i, id);
    }

    const encodeBound = try gener.bound();
    const encoded = try alloc.alloc(u8, encodeBound);
    defer alloc.free(encoded);
    const compressionPool = try CompressionPool.init(alloc, 1);
    defer compressionPool.deinit(alloc);
    const decompressionPool = try DecompressionPool.init(alloc, 1);
    defer decompressionPool.deinit(alloc);
    const offset = try gener.encode(testing.io, compressionPool, alloc, encoded);

    const generDecoded = try ColumnIDGen.decode(testing.io, alloc, decompressionPool, encoded[0..offset]);
    defer generDecoded.deinit(alloc);

    try testing.expectEqual(gener.keyIDs.count(), generDecoded.keyIDs.count());
    for (gener.keyIDs.keys()) |key| {
        const value = gener.keyIDs.get(key);
        const decodedValue = generDecoded.keyIDs.get(key);
        try testing.expectEqual(value, decodedValue);
    }
}
