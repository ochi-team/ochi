const std = @import("std");
const Allocator = std.mem.Allocator;

const MemBlock = @import("MemBlock.zig");
const Table = @import("Table.zig");
const MemTable = @import("MemTable.zig");
const CompressionPool = @import("../compression/CompressionPool.zig");
const DecompressionPool = @import("../compression/DecompressionPool.zig");

const BlockReader = @import("BlockReader.zig");

const testing = std.testing;

fn itemsTotalSize(items: []const []const u8) u32 {
    var total: u32 = 0;
    for (items) |item| total += @intCast(item.len);
    return total;
}

fn createTestMemBlock(alloc: Allocator, items: []const []const u8) !*MemBlock {
    return createTestMemBlockWithMax(alloc, items, itemsTotalSize(items) + 16);
}

fn createTestMemBlockWithMax(alloc: Allocator, items: []const []const u8, maxMemBlockSize: u32) !*MemBlock {
    var block = try MemBlock.init(alloc, .{
        .maxMemBlockSize = maxMemBlockSize,
        .blocksCountHint = items.len,
    });
    errdefer block.deinit(alloc);

    for (items) |item| {
        const ok = block.add(item);
        try testing.expect(ok);
    }

    return block;
}

fn allocIndexedItem(alloc: Allocator, index: usize, totalLen: usize) ![]u8 {
    const buf = try alloc.alloc(u8, totalLen);
    errdefer alloc.free(buf);

    const head = try std.fmt.bufPrint(buf, "item-{d:0>4}", .{index});

    if (head.len < totalLen) {
        for (head.len..totalLen) |i| {
            buf[i] = @intCast((index + i) % 251);
        }
    }
    return buf;
}

test "BlockReader.blockReaderLessThan compares items correctly" {
    const alloc = testing.allocator;
    const decompressionPool = try DecompressionPool.init(alloc, 1);
    defer decompressionPool.deinit(alloc);

    const items1 = [_][]const u8{ "apple", "banana", "cherry" };
    const items2 = [_][]const u8{ "apricot", "blueberry", "date" };

    const block1 = try createTestMemBlock(alloc, &items1);
    const block2 = try createTestMemBlock(alloc, &items2);

    var reader1 = try BlockReader.initFromMovedMemBlock(alloc, block1, decompressionPool);
    defer reader1.deinit(alloc);

    var reader2 = try BlockReader.initFromMovedMemBlock(alloc, block2, decompressionPool);
    defer reader2.deinit(alloc);

    // After sorting, "apple" < "apricot"
    const less = BlockReader.blockReaderLessThan(reader1, reader2);
    try testing.expect(less);
    try testing.expect(reader1.currentI == 0);
    try testing.expect(reader2.currentI == 0);
}

test "BlockReader.current returns correct item at currentI" {
    const alloc = testing.allocator;
    const decompressionPool = try DecompressionPool.init(alloc, 1);
    defer decompressionPool.deinit(alloc);

    const items = [_][]const u8{ "first", "second", "third" };

    const block = try createTestMemBlock(alloc, &items);

    var reader = try BlockReader.initFromMovedMemBlock(alloc, block, decompressionPool);
    defer reader.deinit(alloc);

    // After sorting, test that current() returns the item at currentI
    // First item should be "first"
    const first = reader.current();
    try testing.expectEqualSlices(u8, "first", first);

    // Manually change currentI and verify current() updates
    reader.currentI = 1;
    const second = reader.current();
    try testing.expectEqualSlices(u8, "second", second);

    reader.currentI = 2;
    const third = reader.current();
    try testing.expectEqualSlices(u8, "third", third);
}

test "BlockReader.initFromMemTable reads items" {
    const alloc = testing.allocator;
    const io = testing.io;
    const compressionPool = try CompressionPool.init(alloc, 1);
    defer compressionPool.deinit(alloc);
    const decompressionPool = try DecompressionPool.init(alloc, 1);
    defer decompressionPool.deinit(alloc);

    const Case = struct {
        name: []const u8,
        items: []const []const u8,
        maxMemBlockSize: u32,
        expected: []const []const u8,
        useMultiBlock: bool = false,
    };

    // case 1
    const items_sorted = [_][]const u8{ "alpha", "beta", "delta" };
    // case 2
    const items_unsorted = [_][]const u8{ "delta", "alpha", "beta" };

    // case 3
    const long_len = 200;
    var long_items = try alloc.alloc([]const u8, 3);
    defer alloc.free(long_items);

    const long_a = try alloc.alloc(u8, long_len);
    defer alloc.free(long_a);

    const long_b = try alloc.alloc(u8, long_len);
    defer alloc.free(long_b);

    const long_c = try alloc.alloc(u8, long_len);
    defer alloc.free(long_c);

    long_a[0] = 'x';
    long_b[0] = 'y';
    long_c[0] = 'z';
    @memset(long_a[1..], 'a');
    @memset(long_b[1..], 'a');
    @memset(long_c[1..], 'a');

    long_items[0] = long_a;
    long_items[1] = long_b;
    long_items[2] = long_c;

    // case 4
    const item_count = 80;
    const item_len = 500;
    const full_items = try alloc.alloc([]const u8, item_count);
    defer alloc.free(full_items);

    const block_count = item_count / 2;

    const block1_items = try alloc.alloc([]const u8, block_count);
    defer alloc.free(block1_items);

    const block2_items = try alloc.alloc([]const u8, block_count);
    defer alloc.free(block2_items);

    var owned = try std.ArrayList([]u8).initCapacity(alloc, item_count);
    defer {
        for (owned.items) |buf| alloc.free(buf);
        owned.deinit(alloc);
    }

    var b1: usize = 0;
    var b2: usize = 0;
    for (0..item_count) |i| {
        const item = try allocIndexedItem(alloc, i, item_len);
        errdefer alloc.free(item);

        try owned.append(alloc, item);

        full_items[i] = item;
        if (i < block_count) {
            block1_items[b1] = item;
            b1 += 1;
        } else {
            block2_items[b2] = item;
            b2 += 1;
        }
    }

    const cases = [_]Case{
        .{
            .name = "plain sorted",
            .items = &items_sorted,
            .maxMemBlockSize = itemsTotalSize(&items_sorted) + 16,
            .expected = &items_sorted,
        },
        .{
            .name = "plain unsorted",
            .items = &items_unsorted,
            .maxMemBlockSize = itemsTotalSize(&items_unsorted) + 16,
            .expected = &items_sorted,
        },
        .{
            .name = "zstd long",
            .items = long_items,
            .maxMemBlockSize = @intCast(long_len * long_items.len + 16),
            .expected = long_items,
        },
        .{
            .name = "multi-block merge",
            .items = full_items,
            .maxMemBlockSize = itemsTotalSize(block1_items) + 16,
            .expected = full_items,
            .useMultiBlock = true,
        },
    };

    for (cases) |case| {
        const block1_items_for_case = if (case.useMultiBlock) block1_items else case.items;
        const block1 = try createTestMemBlockWithMax(alloc, block1_items_for_case, case.maxMemBlockSize);

        const memTable: *MemTable = blk: {
            var block2: ?*MemBlock = null;
            if (case.useMultiBlock) {
                block2 = try createTestMemBlockWithMax(alloc, block2_items, itemsTotalSize(block2_items) + 16);
                var blocks = [_]*MemBlock{ block1, block2.? };
                break :blk try MemTable.init(io, alloc, blocks[0..], compressionPool, decompressionPool);
            } else {
                var blocks = [_]*MemBlock{block1};
                break :blk try MemTable.init(io, alloc, blocks[0..], compressionPool, decompressionPool);
            }
        };
        const table = try Table.fromMem(io, alloc, memTable, decompressionPool);
        defer table.close(io);

        var reader = try BlockReader.initFromMemTable(io, alloc, table, decompressionPool);
        defer reader.deinit(alloc);

        if (case.useMultiBlock) {
            var expectedI: usize = 0;
            while (try reader.next(io, alloc)) {
                var iter = reader.block.iterator();
                while (iter.next()) |item| {
                    try testing.expectEqualStrings(case.expected[expectedI], item);
                    expectedI += 1;
                }
            }
            try testing.expectEqual(case.expected.len, expectedI);
        } else {
            try testing.expect(try reader.next(io, alloc));

            const decoded = reader.block.memEntries.items;
            try testing.expectEqual(case.expected.len, decoded.len);
            for (0..decoded.len) |i| {
                try testing.expectEqualStrings(case.expected[i], reader.block.get(i));
            }

            try testing.expect(!try reader.next(io, alloc));
        }
    }
}

test "BlockReader.next returns InvalidIndexBlockRange on empty index buffer with non-empty metaindex" {
    const alloc = testing.allocator;
    const io = testing.io;
    const compressionPool = try CompressionPool.init(alloc, 1);
    defer compressionPool.deinit(alloc);
    const decompressionPool = try DecompressionPool.init(alloc, 1);
    defer decompressionPool.deinit(alloc);

    const items = [_][]const u8{
        "item-0",
        "item-1",
        "item-2",
    };

    const block = try createTestMemBlock(alloc, &items);

    var blocks = [_]*MemBlock{block};
    const memTable = try MemTable.init(io, alloc, &blocks, compressionPool, decompressionPool);
    const table = try Table.fromMem(io, alloc, memTable, decompressionPool);
    defer table.close(io);

    var reader = try BlockReader.initFromMemTable(io, alloc, table, decompressionPool);
    defer reader.deinit(alloc);

    try testing.expect(reader.metaIndexRecords.len > 0);
    try testing.expect(reader.metaIndexRecords[0].indexBlockSize > 0);

    memTable.indexBuf.clearRetainingCapacity();

    try testing.expectError(error.InvalidIndexBlockRange, reader.next(io, alloc));
}

test "BlockReader.initFromDiskTable decodes blocks without null crash" {
    const alloc = testing.allocator;
    const io = testing.io;
    const compressionPool = try CompressionPool.init(alloc, 1);
    defer compressionPool.deinit(alloc);
    const decompressionPool = try DecompressionPool.init(alloc, 1);
    defer decompressionPool.deinit(alloc);

    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    const rootPath = try tmp.dir.realPathFileAlloc(io, ".", alloc);
    defer alloc.free(rootPath);

    const tablePath = try std.fs.path.join(alloc, &.{ rootPath, "table" });

    const items = [_][]const u8{ "delta", "alpha", "beta" };
    const expected = [_][]const u8{ "alpha", "beta", "delta" };

    const block = try createTestMemBlock(alloc, &items);

    var blocks = [_]*MemBlock{block};
    const memTable = try MemTable.init(io, alloc, &blocks, compressionPool, decompressionPool);
    defer memTable.deinit(alloc);

    try memTable.storeToDisk(io, tablePath);

    const diskTable = try Table.open(io, alloc, tablePath, decompressionPool);
    defer diskTable.close(io);

    var reader = try BlockReader.initFromDiskTable(io, alloc, diskTable, decompressionPool);
    defer reader.deinit(alloc);

    const hasNext = try reader.next(io, alloc);
    try testing.expect(hasNext);
    for (0..expected.len) |i| {
        try testing.expectEqualStrings(expected[i], reader.block.get(i));
    }
    try testing.expect(!try reader.next(io, alloc));
}
