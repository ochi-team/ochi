const std = @import("std");
const Allocator = std.mem.Allocator;
const Io = std.Io;

const CompressionPool = @import("../compression/CompressionPool.zig");
const DecompressionPool = @import("../compression/DecompressionPool.zig");

const fs = @import("../../fs.zig");
const filenames = @import("../../filenames.zig");

const MetaIndex = @import("MetaIndex.zig");
const MemTable = @import("MemTable.zig");
const MemBlock = @import("MemBlock.zig");

const BlockWriter = @import("BlockWriter.zig");

const testing = std.testing;

fn createTestMemBlock(alloc: Allocator, items: []const []const u8) !*MemBlock {
    var total: u32 = 0;
    for (items) |item| total += @intCast(item.len);

    var block = try MemBlock.init(alloc, .{
        .maxMemBlockSize = total + 16,
        .blocksCountHint = items.len,
    });
    errdefer block.deinit(alloc);
    for (items) |item| {
        const ok = block.add(item);
        try testing.expect(ok);
    }
    block.sortData();
    return block;
}

pub fn readTableFile(io: Io, alloc: Allocator, tablePath: []const u8, fileName: []const u8) ![]u8 {
    var pathBuf: [std.fs.max_path_bytes]u8 = undefined;
    var pathWriter = std.Io.Writer.fixed(&pathBuf);
    try std.fs.path.fmtJoin(&.{ tablePath, fileName }).format(&pathWriter);
    return fs.readAll(io, alloc, pathWriter.buffered());
}

test "BlockWriter disk output matches mem output" {
    const alloc = testing.allocator;
    const io = testing.io;
    const compressionPool = try CompressionPool.init(alloc, 1);
    defer compressionPool.deinit(alloc);

    const blockOneItems = [_][]const u8{
        "item-a3",
        "item-a1",
        "item-a2",
    };
    const blockTwoItems = [_][]const u8{
        "item-b1-" ++ ("x" ** 160),
        "item-b2-" ++ ("y" ** 160),
        "item-b3-" ++ ("z" ** 160),
    };

    var blockOne = try createTestMemBlock(alloc, &blockOneItems);
    defer blockOne.deinit(alloc);
    var blockTwo = try createTestMemBlock(alloc, &blockTwoItems);
    defer blockTwo.deinit(alloc);

    var memTable = try MemTable.empty(alloc);
    defer memTable.deinit(alloc);
    var memWriter = BlockWriter.initFromMemTable(memTable, compressionPool);
    defer memWriter.deinit(alloc);
    try memWriter.writeBlock(io, alloc, blockOne);
    try memWriter.writeBlock(io, alloc, blockTwo);
    try memWriter.close(io, alloc);

    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    const rootPath = try tmp.dir.realPathFileAlloc(io, ".", alloc);
    defer alloc.free(rootPath);
    var tablePathBuf: [std.fs.max_path_bytes]u8 = undefined;
    var tablePathWriter = std.Io.Writer.fixed(&tablePathBuf);
    try std.fs.path.fmtJoin(&.{ rootPath, "table" }).format(&tablePathWriter);
    const tablePath = tablePathWriter.buffered();

    var diskWriter = try BlockWriter.initFromDiskTable(io, tablePath, true, compressionPool);
    defer diskWriter.deinit(alloc);
    try diskWriter.writeBlock(io, alloc, blockOne);
    try diskWriter.writeBlock(io, alloc, blockTwo);
    try diskWriter.close(io, alloc);

    const entries = try readTableFile(io, alloc, tablePath, filenames.entries);
    defer alloc.free(entries);
    const lens = try readTableFile(io, alloc, tablePath, filenames.lens);
    defer alloc.free(lens);
    const index = try readTableFile(io, alloc, tablePath, filenames.index);
    defer alloc.free(index);
    const metaindex = try readTableFile(io, alloc, tablePath, filenames.metaindex);
    defer alloc.free(metaindex);

    try testing.expectEqualSlices(u8, memTable.entriesBuf.items, entries);
    try testing.expectEqualSlices(u8, memTable.lensBuf.items, lens);
    try testing.expectEqualSlices(u8, memTable.indexBuf.items, index);
    try testing.expectEqualSlices(u8, memTable.metaindexBuf.items, metaindex);
}

test "BlockWriter metaindexBuf may contain multiple records" {
    const alloc = testing.allocator;
    const io = testing.io;
    const blocksCount: usize = 1400;
    const compressionPool = try CompressionPool.init(alloc, 1);
    defer compressionPool.deinit(alloc);
    const decompressionPool = try DecompressionPool.init(alloc, 1);
    defer decompressionPool.deinit(alloc);

    var memTable = try MemTable.empty(alloc);
    defer memTable.deinit(alloc);

    var writer = BlockWriter.initFromMemTable(memTable, compressionPool);
    defer writer.deinit(alloc);

    var itemsOwned = try std.ArrayList([]u8).initCapacity(alloc, blocksCount);
    defer {
        for (itemsOwned.items) |item| alloc.free(item);
        itemsOwned.deinit(alloc);
    }

    var blocks = try std.ArrayList(*MemBlock).initCapacity(alloc, blocksCount);
    defer {
        for (blocks.items) |block| block.deinit(alloc);
        blocks.deinit(alloc);
    }

    // Simulate a compaction/merge path calling writeBlock() many times.
    for (0..blocksCount) |i| {
        const item = try std.fmt.allocPrint(alloc, "merge-item-{d:0>6}", .{i});
        errdefer alloc.free(item);
        try itemsOwned.append(alloc, item);

        const items = [_][]const u8{item};
        const block = try createTestMemBlock(alloc, &items);
        errdefer block.deinit(alloc);
        try blocks.append(alloc, block);

        try writer.writeBlock(io, alloc, block);
    }
    try writer.close(io, alloc);

    const decoded = try MetaIndex.decodeDecompress(
        io,
        alloc,
        decompressionPool,
        memTable.metaindexBuf.items,
        @intCast(blocksCount),
    );
    defer {
        for (decoded.records) |*rec| rec.deinit(alloc);
        if (decoded.records.len > 0) alloc.free(decoded.records);
    }

    try testing.expect(decoded.records.len > 1);

    var totalBlockHeaders: u64 = 0;
    for (decoded.records) |rec| {
        try testing.expect(rec.blockHeadersCount > 0);
        totalBlockHeaders += rec.blockHeadersCount;
    }
    try testing.expectEqual(@as(u64, @intCast(blocksCount)), totalBlockHeaders);
}

test "BlockWriter preserves first metaindex item when reusing source block" {
    const alloc = testing.allocator;
    const io = testing.io;
    const compressionPool = try CompressionPool.init(alloc, 1);
    defer compressionPool.deinit(alloc);
    const decompressionPool = try DecompressionPool.init(alloc, 1);
    defer decompressionPool.deinit(alloc);

    var table = try MemTable.empty(alloc);
    defer table.deinit(alloc);

    var writer = BlockWriter.initFromMemTable(table, compressionPool);
    defer writer.deinit(alloc);

    var block = try MemBlock.init(alloc, .{
        .maxMemBlockSize = 128,
        .blocksCountHint = 2,
    });
    defer block.deinit(alloc);

    try testing.expect(block.add("alpha-001"));
    try testing.expect(block.add("alpha-002"));
    block.sortData();
    try writer.writeBlock(io, alloc, block);

    block.reset();
    try testing.expect(block.add("omega-001"));
    try testing.expect(block.add("omega-002"));
    block.sortData();
    try writer.writeBlock(io, alloc, block);

    try writer.close(io, alloc);

    const decoded = try MetaIndex.decodeDecompress(io, alloc, decompressionPool, table.metaindexBuf.items, 2);
    defer {
        for (decoded.records) |*rec| rec.deinit(alloc);
        if (decoded.records.len > 0) alloc.free(decoded.records);
    }

    try testing.expectEqual(@as(usize, 1), decoded.records.len);
    try testing.expectEqualStrings("alpha-001", decoded.records[0].firstEntry);
    try testing.expectEqual(@as(u32, 2), decoded.records[0].blockHeadersCount);
}
