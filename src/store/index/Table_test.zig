const std = @import("std");
const Allocator = std.mem.Allocator;
const Io = std.Io;
const Dir = Io.Dir;

const filenames = @import("../../filenames.zig");
const fs = @import("../../fs.zig");
const TableHeader = @import("TableHeader.zig");
const MemTable = @import("MemTable.zig");
const MetaIndex = @import("MetaIndex.zig");
const BlockWriter = @import("BlockWriter.zig");
const MemBlock = @import("MemBlock.zig");
const CompressionPool = @import("../compression/CompressionPool.zig");
const DecompressionPool = @import("../compression/DecompressionPool.zig");

const Table = @import("Table.zig");

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

fn createTestTableDir(io: Io, alloc: Allocator, tablePath: []const u8) !void {
    const compressionPool = try CompressionPool.init(alloc, 1);
    defer compressionPool.deinit(alloc);

    const items = [_][]const u8{ "alpha", "beta", "omega" };
    var block = try createTestMemBlock(alloc, &items);
    defer block.deinit(alloc);

    var writer = try BlockWriter.initFromDiskTable(io, tablePath, true, compressionPool);
    defer writer.deinit(alloc);
    try writer.writeBlock(io, alloc, block);
    try writer.close(io, alloc);

    var header = TableHeader{
        .entriesCount = items.len,
        .blocksCount = 1,
        .firstEntry = items[0],
        .lastEntry = items[items.len - 1],
    };
    try header.writeFile(io, tablePath);
}

// TODO: bunch of tests/things is duplicated between data/index tables,
// there must be a way to make them more generic
test "release keeps table unless toRemove is set, then removes table dir" {
    const alloc = testing.allocator;
    const io = testing.io;

    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    const rootPath = try tmp.dir.realPathFileAlloc(io, ".", alloc);
    defer alloc.free(rootPath);
    var tablePathBuf: [std.fs.max_path_bytes]u8 = undefined;
    var tablePathWriter = std.Io.Writer.fixed(&tablePathBuf);
    try std.fs.path.fmtJoin(&.{ rootPath, "table-1" }).format(&tablePathWriter);
    const tablePath = tablePathWriter.buffered();

    const decompressionPool = try DecompressionPool.init(alloc, 1);
    defer decompressionPool.deinit(alloc);
    try createTestTableDir(io, alloc, tablePath);

    const table1Path = try alloc.dupe(u8, tablePath);
    const table1 = try Table.open(io, alloc, table1Path, decompressionPool);
    table1.release(io);
    try Dir.accessAbsolute(io, tablePath, .{});

    const table2Path = try alloc.dupe(u8, tablePath);
    const table2 = try Table.open(io, alloc, table2Path, decompressionPool);
    table2.toRemove.store(true, .release);
    table2.release(io);
    try testing.expectError(error.FileNotFound, Dir.accessAbsolute(io, tablePath, .{}));
}

test "release fromMem does not affect filesystem path" {
    const alloc = testing.allocator;
    const io = testing.io;

    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    const rootPath = try tmp.dir.realPathFileAlloc(io, ".", alloc);
    defer alloc.free(rootPath);
    var sentinelPathBuf: [std.fs.max_path_bytes]u8 = undefined;
    var sentinelPathWriter = std.Io.Writer.fixed(&sentinelPathBuf);
    try std.fs.path.fmtJoin(&.{ rootPath, "sentinel" }).format(&sentinelPathWriter);
    const sentinelPath = sentinelPathWriter.buffered();
    // create a real directory to verify it remains
    try testing.expectError(error.FileNotFound, Dir.accessAbsolute(io, sentinelPath, .{}));
    try Dir.createDirAbsolute(io, sentinelPath, .default_dir);

    const decompressionPool = try DecompressionPool.init(alloc, 1);
    defer decompressionPool.deinit(alloc);
    const memTable = try MemTable.empty(alloc);

    const table = try Table.fromMem(io, alloc, memTable, decompressionPool);
    // eve if set we expect it to keep the created path when disk == null,
    // so close must not delete it
    table.toRemove.store(true, .release);
    // we expected only second release close cleans the table, otherwise it's a memory leak
    table.retain();

    try Dir.accessAbsolute(io, sentinelPath, .{});
    table.release(io);
    try Dir.accessAbsolute(io, sentinelPath, .{});
    table.release(io);
    try Dir.accessAbsolute(io, sentinelPath, .{});
}

test "fromMem creates proper table from mem table with populated data" {
    const alloc = testing.allocator;
    const io = testing.io;
    const compressionPool = try CompressionPool.init(alloc, 1);
    defer compressionPool.deinit(alloc);
    const decompressionPool = try DecompressionPool.init(alloc, 1);
    defer decompressionPool.deinit(alloc);

    const items = [_][]const u8{ "item-c", "item-a", "item-b" };
    const block = try createTestMemBlock(alloc, &items);

    var blocks = [_]*MemBlock{block};
    const memTable = try MemTable.init(io, alloc, blocks[0..], compressionPool, decompressionPool);

    const table = try Table.fromMem(io, alloc, memTable, decompressionPool);
    defer table.release(io);

    try testing.expect(table.inner == .mem);
    var buf: [64]u8 = undefined;
    var i: usize = 0;

    i = try table.readLens(io, &buf, 0);
    try testing.expect(i > 0);
    try testing.expectEqualSlices(u8, memTable.lensBuf.items, buf[0..i]);
    i = try table.readEntries(io, &buf, 0);
    try testing.expect(i > 0);
    try testing.expectEqualSlices(u8, memTable.entriesBuf.items, buf[0..i]);
    i = try table.readIndex(io, &buf, 0);
    try testing.expect(i > 0);
    try testing.expectEqualSlices(u8, memTable.indexBuf.items, buf[0..i]);

    const expectedMetaindex = try MetaIndex.decodeDecompress(
        testing.io,
        alloc,
        decompressionPool,
        memTable.metaindexBuf.items,
        memTable.tableHeader.blocksCount,
    );
    defer {
        for (expectedMetaindex.records) |*rec| rec.deinit(alloc);
        if (expectedMetaindex.records.len > 0) alloc.free(expectedMetaindex.records);
    }

    try testing.expectEqualDeep(expectedMetaindex.records, table.metaIndexRecords);
}

test "open reads table from disk" {
    const alloc = testing.allocator;
    const io = testing.io;

    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    const rootPath = try tmp.dir.realPathFileAlloc(io, ".", alloc);
    defer alloc.free(rootPath);
    var tablePathBuf: [std.fs.max_path_bytes]u8 = undefined;
    var tablePathWriter = std.Io.Writer.fixed(&tablePathBuf);
    try std.fs.path.fmtJoin(&.{ rootPath, "table-1" }).format(&tablePathWriter);
    const tablePath = tablePathWriter.buffered();

    const decompressionPool = try DecompressionPool.init(alloc, 1);
    defer decompressionPool.deinit(alloc);
    try createTestTableDir(io, alloc, tablePath);

    const tablePathOwned = try alloc.dupe(u8, tablePath);
    const table = try Table.open(io, alloc, tablePathOwned, decompressionPool);
    defer table.release(io);

    try testing.expect(table.inner == .disk);

    const expectedIndex = try BlockWriter.readTableFile(io, alloc, tablePath, filenames.index);
    defer alloc.free(expectedIndex);
    const expectedEntries = try BlockWriter.readTableFile(io, alloc, tablePath, filenames.entries);
    defer alloc.free(expectedEntries);
    const expectedLens = try BlockWriter.readTableFile(io, alloc, tablePath, filenames.lens);
    defer alloc.free(expectedLens);
    const expectedMetaindexCompressed = try BlockWriter.readTableFile(io, alloc, tablePath, filenames.metaindex);
    defer alloc.free(expectedMetaindexCompressed);

    var buf = try alloc.alloc(u8, @max(expectedIndex.len, @max(expectedEntries.len, expectedLens.len)));
    defer alloc.free(buf);

    var n = try table.readIndex(io, buf[0..expectedIndex.len], 0);
    try testing.expectEqual(expectedIndex.len, n);
    try testing.expectEqualSlices(u8, expectedIndex, buf[0..n]);
    n = try table.readEntries(io, buf[0..expectedEntries.len], 0);
    try testing.expectEqual(expectedEntries.len, n);
    try testing.expectEqualSlices(u8, expectedEntries, buf[0..n]);
    n = try table.readLens(io, buf[0..expectedLens.len], 0);
    try testing.expectEqual(expectedLens.len, n);
    try testing.expectEqualSlices(u8, expectedLens, buf[0..n]);

    const expectedMetaindex = try MetaIndex.decodeDecompress(
        io,
        alloc,
        decompressionPool,
        expectedMetaindexCompressed,
        table.tableHeader().blocksCount,
    );
    defer {
        for (expectedMetaindex.records) |*rec| rec.deinit(alloc);
        if (expectedMetaindex.records.len > 0) alloc.free(expectedMetaindex.records);
    }

    try testing.expectEqualDeep(expectedMetaindex.records, table.metaIndexRecords);

    const expectedSize: u64 = expectedMetaindexCompressed.len +
        expectedIndex.len + expectedEntries.len + expectedLens.len;
    try testing.expectEqual(expectedSize, table.size);
}

test "openAll frees catalog table names on error" {
    try testing.checkAllAllocationFailures(testing.allocator, testOpenAllFreesCatalogTableNamesOnError, .{testing.io});
}

fn testOpenAllFreesCatalogTableNamesOnError(alloc: Allocator, io: Io) !void {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    const rootPath = try tmp.dir.realPathFileAlloc(io, ".", alloc);
    defer alloc.free(rootPath);

    var tablesFilePathBuf: [std.fs.max_path_bytes]u8 = undefined;
    var tablesFilePathWriter = std.Io.Writer.fixed(&tablesFilePathBuf);
    try std.fs.path.fmtJoin(&.{ rootPath, filenames.tables }).format(&tablesFilePathWriter);
    const tablesFilePath = tablesFilePathWriter.buffered();
    try fs.writeBufferToFileAtomic(io, tablesFilePath, "[\"table-a\",\"table-b\"]", true);

    const decompressionPool = try DecompressionPool.init(alloc, 1);
    defer decompressionPool.deinit(alloc);
    const res = Table.openAll(io, alloc, rootPath, decompressionPool);
    try testing.expectError(error.TableDoesNotExist, res);
}

test "openAll frees spilled catalog table names on error" {
    const alloc = testing.allocator;
    const io = testing.io;

    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    const rootPath = try tmp.dir.realPathFileAlloc(io, ".", alloc);
    defer alloc.free(rootPath);

    var tablesFilePathBuf: [std.fs.max_path_bytes]u8 = undefined;
    var tablesFilePathWriter = std.Io.Writer.fixed(&tablesFilePathBuf);
    try std.fs.path.fmtJoin(&.{ rootPath, filenames.tables }).format(&tablesFilePathWriter);
    const tablesFilePath = tablesFilePathWriter.buffered();

    const longName = "table" ** 10;

    var content = try std.ArrayList(u8).initCapacity(alloc, 4096);
    defer content.deinit(alloc);
    try content.append(alloc, '[');
    for (0..5) |i| {
        if (i > 0) try content.append(alloc, ',');
        try content.append(alloc, '"');
        try content.appendSlice(alloc, longName);
        try content.append(alloc, '"');
    }
    try content.append(alloc, ']');

    try fs.writeBufferToFileAtomic(io, tablesFilePath, content.items, true);

    const decompressionPool = try DecompressionPool.init(alloc, 1);
    defer decompressionPool.deinit(alloc);
    try testing.expectError(error.TableDoesNotExist, Table.openAll(io, alloc, rootPath, decompressionPool));
}
