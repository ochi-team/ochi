const std = @import("std");
const Io = std.Io;
const Dir = Io.Dir;

const encoding = @import("encoding");

const filenames = @import("../../filenames.zig");
const DecompressionPool = @import("../compression/DecompressionPool.zig");

const MetaIndex = @import("MetaIndex.zig");

const testing = std.testing;

test "MetaIndex decodeDecompress roundtrip" {
    const alloc = testing.allocator;

    const rec1 = MetaIndex{
        .firstEntry = "alpha",
        .blockHeadersCount = 2,
        .indexBlockOffset = 10,
        .indexBlockSize = 64,
    };
    const rec2 = MetaIndex{
        .firstEntry = "omega",
        .blockHeadersCount = 3,
        .indexBlockOffset = 74,
        .indexBlockSize = 128,
    };

    var uncompressed = std.ArrayList(u8).empty;
    defer uncompressed.deinit(alloc);

    var recordBound = rec1.bound();
    try uncompressed.ensureUnusedCapacity(alloc, recordBound);
    rec1.encode(uncompressed.unusedCapacitySlice());
    uncompressed.items.len += recordBound;

    recordBound = rec2.bound();
    try uncompressed.ensureUnusedCapacity(alloc, recordBound);
    rec2.encode(uncompressed.unusedCapacitySlice());
    uncompressed.items.len += recordBound;

    const compressedBound = try encoding.compressBound(uncompressed.items.len);
    const compressed = try alloc.alloc(u8, compressedBound);
    defer alloc.free(compressed);
    const cctx = try encoding.createCCtx();
    defer encoding.freeCCtx(cctx);
    const compressedLen = try encoding.compressAuto(cctx, compressed, uncompressed.items);

    const decompressionPool = try DecompressionPool.init(alloc, 1);
    defer decompressionPool.deinit(alloc);
    const decoded = try MetaIndex.decodeDecompress(
        testing.io,
        alloc,
        decompressionPool,
        compressed[0..compressedLen],
        rec1.blockHeadersCount + rec2.blockHeadersCount,
    );
    defer {
        for (decoded.records) |rec| {
            rec.deinit(alloc);
        }
        if (decoded.records.len > 0) alloc.free(decoded.records);
    }

    try testing.expectEqualDeep(&[_]MetaIndex{ rec1, rec2 }, decoded.records);
}

test "MetaIndex roundtrip file read/write" {
    const alloc = testing.allocator;
    const io = testing.io;
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    try tmp.dir.createDirPath(io, "table");
    const tablePath = try tmp.dir.realPathFileAlloc(io, "table", alloc);
    defer alloc.free(tablePath);

    const rec1 = MetaIndex{
        .firstEntry = "alpha",
        .blockHeadersCount = 2,
        .indexBlockOffset = 10,
        .indexBlockSize = 64,
    };
    const rec2 = MetaIndex{
        .firstEntry = "omega",
        .blockHeadersCount = 3,
        .indexBlockOffset = 74,
        .indexBlockSize = 128,
    };

    var uncompressed = std.ArrayList(u8).empty;
    defer uncompressed.deinit(alloc);

    var recordBound = rec1.bound();
    try uncompressed.ensureUnusedCapacity(alloc, recordBound);
    rec1.encode(uncompressed.unusedCapacitySlice());
    uncompressed.items.len += recordBound;

    recordBound = rec2.bound();
    try uncompressed.ensureUnusedCapacity(alloc, recordBound);
    rec2.encode(uncompressed.unusedCapacitySlice());
    uncompressed.items.len += recordBound;

    const compressedBound = try encoding.compressBound(uncompressed.items.len);
    const compressed = try alloc.alloc(u8, compressedBound);
    defer alloc.free(compressed);
    const cctx = try encoding.createCCtx();
    defer encoding.freeCCtx(cctx);
    const compressedLen = try encoding.compressAuto(cctx, compressed, uncompressed.items);

    var metaindexPathBuf: [std.fs.max_path_bytes]u8 = undefined;
    var metaindexPathWriter = std.Io.Writer.fixed(&metaindexPathBuf);
    try std.fs.path.fmtJoin(&.{ tablePath, filenames.metaindex }).format(&metaindexPathWriter);

    var file = try Dir.createFileAbsolute(io, metaindexPathWriter.buffered(), .{ .truncate = true });
    defer file.close(io);
    try file.writeStreamingAll(io, compressed[0..compressedLen]);
    try file.sync(io);

    const decompressionPool = try DecompressionPool.init(alloc, 1);
    defer decompressionPool.deinit(alloc);
    const decoded = try MetaIndex.readFile(
        io,
        alloc,
        decompressionPool,
        tablePath,
        rec1.blockHeadersCount + rec2.blockHeadersCount,
    );
    defer {
        for (decoded.records) |rec| {
            rec.deinit(alloc);
        }
        if (decoded.records.len > 0) alloc.free(decoded.records);
    }

    try testing.expectEqualDeep(&[_]MetaIndex{ rec1, rec2 }, decoded.records);
}
