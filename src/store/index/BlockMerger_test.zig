//! BlockMerger: Merges multiple sorted BlockReaders into a single sorted output.
//!
//! Use cases:
//! - SSTable compaction: merging multiple index blocks during LSM-tree compaction
//! - Flush operations: combining in-memory blocks with on-disk blocks
//!
//! Constraints:
//! - Input BlockReaders must contain sorted data
//! - Uses a min-heap for k-way merge, O(n log k) complexity
//! - Automatically merges consecutive tagToSids records with same prefix (tenant+tag)
//! - Limited to maxStreamsPerRecord (32) stream IDs per merged tag record
//! - Can be stopped mid-merge via Stop

const std = @import("std");
const Allocator = std.mem.Allocator;

const Stop = @import("../../stds/Stop.zig");
const BlockReader = @import("BlockReader.zig");
const MemBlock = @import("MemBlock.zig");
const BlockWriter = @import("BlockWriter.zig");
const TableHeader = @import("TableHeader.zig");
const IndexKind = @import("Index.zig").IndexKind;
const TagRecordsParser = @import("TagRecordsParser.zig");
const MemTable = @import("MemTable.zig");
const Table = @import("Table.zig");
const CompressionPool = @import("../compression/CompressionPool.zig");
const DecompressionPool = @import("../compression/DecompressionPool.zig");

const BlockMerger = @import("BlockMerger.zig");

const testing = std.testing;
const SID = @import("../lines.zig").SID;
const Field = @import("../lines.zig").Field;
const Encoder = @import("encoding").Encoder;

pub fn createTagRecord(
    alloc: Allocator,
    tenantID: u64,
    tag: Field,
    streamIDs: []const u128,
) ![]u8 {
    const bufSize = TagRecordsParser.encodeRecordBound(tag, streamIDs.len);
    const buf = try alloc.alloc(u8, bufSize);
    const recordLen = TagRecordsParser.encodeRecord(buf, tenantID, tag, streamIDs);
    return buf[0..recordLen];
}

fn createTestMemBlock(alloc: Allocator, entries: []const []const u8, maxIndexBlockSize: u32) !*MemBlock {
    const block = try MemBlock.init(alloc, .{
        .maxMemBlockSize = maxIndexBlockSize,
        .blocksCountHint = entries.len,
    });
    errdefer block.deinit(alloc);
    for (entries) |entry| {
        _ = block.add(entry);
    }
    return block;
}

fn createTestReaders(
    alloc: Allocator,
    blocksData: []const []const []const u8,
    maxIndexBlockSize: u32,
    decompressionPool: *DecompressionPool,
) !std.ArrayList(*BlockReader) {
    var readers = try std.ArrayList(*BlockReader).initCapacity(alloc, blocksData.len);
    errdefer {
        for (readers.items) |reader| {
            reader.deinit(alloc);
        }
        readers.deinit(alloc);
    }
    for (blocksData) |blockData| {
        const block = try createTestMemBlock(alloc, blockData, maxIndexBlockSize);
        const reader = try BlockReader.initFromMovedMemBlock(alloc, block, decompressionPool);
        errdefer reader.deinit(alloc);
        try readers.append(alloc, reader);
    }
    return readers;
}

fn createTestMemTable(alloc: Allocator) !*MemTable {
    const memTable = try alloc.create(MemTable);
    memTable.* = .{
        .blockHeader = undefined,
        .tableHeader = .{},
        .flushAtUs = undefined,
    };
    return memTable;
}

fn cleanupReaders(alloc: Allocator, readers: *std.ArrayList(*BlockReader)) void {
    for (readers.items) |reader| {
        reader.deinit(alloc);
    }
    readers.deinit(alloc);
}

fn createTestEntries(alloc: Allocator, count: usize, size: usize) ![][]const u8 {
    var entries = try std.ArrayList([]const u8).initCapacity(alloc, count);
    errdefer {
        for (entries.items) |e| {
            alloc.free(e);
        }
        entries.deinit(alloc);
    }

    for (0..count) |i| {
        // Create entries of specified size with sorted data
        const entry = try std.fmt.allocPrint(alloc, "entry_{d:0>[1]}", .{ i, size - 7 });
        entries.appendAssumeCapacity(entry);
    }

    return entries.toOwnedSlice(alloc);
}

fn createSidEntry(alloc: Allocator, tenantID: u64, streamID: u128) ![]const u8 {
    const buf = try alloc.alloc(u8, 1 + SID.encodeBound);
    errdefer alloc.free(buf);
    var enc = Encoder.init(buf);
    const sid = SID{ .tenantID = tenantID, .id = streamID };
    sid.encodeTenantWithPrefix(&enc, @intFromEnum(IndexKind.sid));
    enc.writeInt(u128, sid.id);

    return buf;
}

test "BlockMerger.mergeBasicScenarios" {
    const alloc = testing.allocator;
    const io = testing.io;
    const maxIndexBlockSize = 1024;
    const compressionPool = try CompressionPool.init(alloc, 1);
    defer compressionPool.deinit(alloc);
    const decompressionPool = try DecompressionPool.init(alloc, 1);
    defer decompressionPool.deinit(alloc);

    const Case = struct {
        blocks: []const []const []const u8,
        expectedTableHeader: TableHeader,
    };

    const cases = [_]Case{
        .{
            .blocks = &.{},
            .expectedTableHeader = .{ .entriesCount = 0 },
        },
        .{
            .blocks = &.{&.{ "a", "b", "c" }},
            .expectedTableHeader = .{ .entriesCount = 3, .blocksCount = 1, .firstEntry = "a", .lastEntry = "c" },
        },
        .{
            .blocks = &.{ &.{ "a", "d", "g" }, &.{ "b", "e", "h" }, &.{ "c", "f", "i" } },
            .expectedTableHeader = .{ .entriesCount = 9, .blocksCount = 1, .firstEntry = "a", .lastEntry = "i" },
        },
        .{
            .blocks = &.{ &.{ "a", "b", "c" }, &.{ "x", "y", "z" } },
            .expectedTableHeader = .{ .entriesCount = 6, .blocksCount = 1, .firstEntry = "a", .lastEntry = "z" },
        },
        .{
            .blocks = &.{ &.{ "a", "b", "c" }, &.{ "b", "c", "d" } },
            .expectedTableHeader = .{ .entriesCount = 6, .blocksCount = 1, .firstEntry = "a", .lastEntry = "d" },
        },
    };

    for (cases) |case| {
        var readers = try createTestReaders(alloc, case.blocks, maxIndexBlockSize, decompressionPool);
        defer cleanupReaders(alloc, &readers);

        var memTable = try createTestMemTable(alloc);
        defer memTable.deinit(alloc);

        var writer = BlockWriter.initFromMemTable(memTable, compressionPool);
        defer writer.deinit(alloc);

        var merger = try BlockMerger.init(io, alloc, &readers);
        defer merger.deinit(alloc);

        const tableHeader = try merger.merge(io, alloc, &writer, null);
        defer tableHeader.deinit(alloc);

        try testing.expectEqualDeep(case.expectedTableHeader, tableHeader);
    }
}

test "BlockMerger.merge block overflow" {
    const alloc = testing.allocator;
    const io = testing.io;
    const maxIndexBlockSize = 1024;
    const compressionPool = try CompressionPool.init(alloc, 1);
    defer compressionPool.deinit(alloc);
    const decompressionPool = try DecompressionPool.init(alloc, 1);
    defer decompressionPool.deinit(alloc);

    const Case = struct {
        entryCount: usize,
        entrySize: usize,
        expectedEntriesCount: u64,
    };

    const cases = [_]Case{
        .{
            // Case 1: 6 entries of 200 bytes each = 1200 bytes total (exceeds 1024 bytes block size)
            // Split across two readers (3 entries each), all 6 entries fit
            .entryCount = 6,
            .entrySize = 200,
            .expectedEntriesCount = 6,
        },
        .{
            // Case 2: 20 entries of 200 bytes each
            // Split into 2 readers with 10 entries each, but each block can only hold 5 entries (1000 bytes)
            // Result: 5 entries from first reader + 5 from second = 10 total
            .entryCount = 20,
            .entrySize = 200,
            .expectedEntriesCount = 10,
        },
    };

    for (cases) |case| {
        const largeEntries = try createTestEntries(alloc, case.entryCount, case.entrySize);
        defer {
            for (largeEntries) |entry| alloc.free(entry);
            alloc.free(largeEntries);
        }

        // Split entries across two readers
        const mid = largeEntries.len / 2;
        const blocks = [_][]const []const u8{
            largeEntries[0..mid],
            largeEntries[mid..],
        };

        var readers = try createTestReaders(alloc, &blocks, maxIndexBlockSize, decompressionPool);
        defer cleanupReaders(alloc, &readers);

        var memTable = try createTestMemTable(alloc);
        defer memTable.deinit(alloc);

        var writer = BlockWriter.initFromMemTable(memTable, compressionPool);
        defer writer.deinit(alloc);

        var merger = try BlockMerger.init(io, alloc, &readers);
        defer merger.deinit(alloc);

        const tableHeader = try merger.merge(io, alloc, &writer, null);
        defer tableHeader.deinit(alloc);

        try testing.expectEqual(case.expectedEntriesCount, tableHeader.entriesCount);
    }
}

test "BlockMerger.merge oversized entries" {
    const alloc = testing.allocator;
    const io = testing.io;
    const maxIndexBlockSize = 1024;
    const compressionPool = try CompressionPool.init(alloc, 1);
    defer compressionPool.deinit(alloc);
    const decompressionPool = try DecompressionPool.init(alloc, 1);
    defer decompressionPool.deinit(alloc);

    const OversizedCase = struct {
        index: usize,
        size: usize,
    };
    const entriesSpec = [_]OversizedCase{
        .{ .index = 0, .size = 200 },
        .{ .index = 1, .size = 2000 },
        .{ .index = 2, .size = 200 },
        .{ .index = 3, .size = 2000 },
    };
    var mixedEntries = try std.ArrayList([]const u8).initCapacity(alloc, entriesSpec.len);
    defer {
        for (mixedEntries.items) |entry| alloc.free(entry);
        mixedEntries.deinit(alloc);
    }
    for (entriesSpec) |spec| {
        const entry = try std.fmt.allocPrint(alloc, "entry_{d:0>[1]}", .{ spec.index, spec.size - 7 });
        mixedEntries.appendAssumeCapacity(entry);
    }

    const mid = mixedEntries.items.len / 2;
    const mixedBlocks = [_][]const []const u8{
        mixedEntries.items[0..mid],
        mixedEntries.items[mid..],
    };

    var readers = try createTestReaders(alloc, &mixedBlocks, maxIndexBlockSize, decompressionPool);
    defer cleanupReaders(alloc, &readers);

    var memTable = try createTestMemTable(alloc);
    defer memTable.deinit(alloc);

    var writer = BlockWriter.initFromMemTable(memTable, compressionPool);
    defer writer.deinit(alloc);

    var merger = try BlockMerger.init(io, alloc, &readers);
    defer merger.deinit(alloc);

    const tableHeader = try merger.merge(io, alloc, &writer, null);
    defer tableHeader.deinit(alloc);
    try testing.expectEqual(2, tableHeader.entriesCount);
}

test "BlockMerger.merge tag records" {
    const alloc = testing.allocator;
    const io = testing.io;
    const compressionPool = try CompressionPool.init(alloc, 1);
    defer compressionPool.deinit(alloc);
    const decompressionPool = try DecompressionPool.init(alloc, 1);
    defer decompressionPool.deinit(alloc);

    const tag = Field{ .key = "env", .value = "prod" };

    const Case = struct {
        name: []const u8,
        createEntries: *const fn (Allocator, Field) anyerror![][]const u8,
        expectedItemsCount: u64,
        maxIndexBlockSize: u32 = 1024,
    };

    const cases = [_]Case{
        .{
            .name = "single tag record",
            // Case 1: Single tag record (no merging)
            .createEntries = &struct {
                fn f(a: Allocator, t: Field) ![][]const u8 {
                    var entries = try std.ArrayList([]const u8).initCapacity(a, 1);
                    errdefer {
                        for (entries.items) |entry| a.free(entry);
                        entries.deinit(a);
                    }
                    const entry = try createTagRecord(a, 1, t, &[_]u128{ 100, 200 });
                    entries.appendAssumeCapacity(entry);
                    return entries.toOwnedSlice(a);
                }
            }.f,
            .expectedItemsCount = 1,
        },
        .{
            .name = "merge same prefix",
            // Case 2: Two consecutive tag records, same prefix (should merge)
            .createEntries = &struct {
                fn f(a: Allocator, t: Field) ![][]const u8 {
                    var entries = try std.ArrayList([]const u8).initCapacity(a, 5);
                    errdefer {
                        for (entries.items) |entry| a.free(entry);
                        entries.deinit(a);
                    }
                    entries.appendAssumeCapacity(try createSidEntry(a, 0, 50));
                    entries.appendAssumeCapacity(try createSidEntry(a, 1, 60));
                    entries.appendAssumeCapacity(try createTagRecord(a, 2, t, &[_]u128{100}));
                    entries.appendAssumeCapacity(try createTagRecord(a, 2, t, &[_]u128{200}));
                    entries.appendAssumeCapacity(try createTagRecord(a, 3, t, &[_]u128{300}));
                    return entries.toOwnedSlice(a);
                }
            }.f,
            .expectedItemsCount = 4,
        },
        .{
            .name = "different tenants stay separate",
            // Case 3: Two consecutive tag records, different tenant (should NOT merge)
            .createEntries = &struct {
                fn f(a: Allocator, t: Field) ![][]const u8 {
                    var entries = try std.ArrayList([]const u8).initCapacity(a, 5);
                    errdefer {
                        for (entries.items) |entry| a.free(entry);
                        entries.deinit(a);
                    }
                    entries.appendAssumeCapacity(try createSidEntry(a, 0, 50));
                    entries.appendAssumeCapacity(try createSidEntry(a, 1, 60));
                    entries.appendAssumeCapacity(try createTagRecord(a, 2, t, &[_]u128{100}));
                    entries.appendAssumeCapacity(try createTagRecord(a, 3, t, &[_]u128{200}));
                    entries.appendAssumeCapacity(try createTagRecord(a, 4, t, &[_]u128{300}));
                    return entries.toOwnedSlice(a);
                }
            }.f,
            .expectedItemsCount = 5,
        },
        .{
            .name = "mixed kinds",
            // Case 4: Mixed IndexKind entries
            .createEntries = &struct {
                fn f(a: Allocator, t: Field) ![][]const u8 {
                    var entries = try std.ArrayList([]const u8).initCapacity(a, 2);
                    errdefer {
                        for (entries.items) |entry| a.free(entry);
                        entries.deinit(a);
                    }
                    entries.appendAssumeCapacity(try createSidEntry(a, 1, 100));
                    entries.appendAssumeCapacity(try createTagRecord(a, 1, t, &[_]u128{100}));
                    return entries.toOwnedSlice(a);
                }
            }.f,
            .expectedItemsCount = 2,
        },
        .{
            .name = "unsorted merge falls back",
            // Case 5: Duplicate streamIDs causing unsorted output after merge (fallback to original)
            // This tests the scenario where merging would create unsorted data:
            // - item1 has duplicates: [100, 100, ..., 500]
            // - item2 has: [100, 400]
            // After dedup, item1 becomes [100, 500], item2 stays [100, 400]
            // This makes item1 > item2, so we fallback to original unmerged data
            .createEntries = &struct {
                fn f(a: Allocator, t: Field) ![][]const u8 {
                    var entries = try std.ArrayList([]const u8).initCapacity(a, 4);
                    errdefer {
                        for (entries.items) |entry| a.free(entry);
                        entries.deinit(a);
                    }
                    entries.appendAssumeCapacity(try createSidEntry(a, 0, 5));
                    // Create streamIDs with many duplicates: [100, 100, ..., 100, 500]
                    // After dedup in merged output: [100, 500]
                    var streamIDs1 = try std.ArrayList(u128).initCapacity(a, 5);
                    defer streamIDs1.deinit(a);
                    for (0..4) |_| {
                        streamIDs1.appendAssumeCapacity(10);
                    }
                    streamIDs1.appendAssumeCapacity(50);
                    entries.appendAssumeCapacity(
                        try createTagRecord(a, 1, t, streamIDs1.items),
                    );
                    // Second record with streamIDs: [100, 400]
                    // Would come after deduplicated first record [100, 500]
                    // but 500 > 400, making merged output unsorted
                    entries.appendAssumeCapacity(
                        try createTagRecord(a, 1, t, &[_]u128{ 10, 40 }),
                    );
                    entries.appendAssumeCapacity(try createSidEntry(a, 2, 60));
                    return entries.toOwnedSlice(a);
                }
            }.f,
            .expectedItemsCount = 4, // Should keep original 4 items due to unsorted merge result
        },
        .{
            .name = "large merge buffer growth",
            // Case 6: many tag records force merge buffer growth without invalidating prior slices
            .createEntries = &struct {
                fn f(a: Allocator, t: Field) ![][]const u8 {
                    var entries = try std.ArrayList([]const u8).initCapacity(a, 34);
                    errdefer {
                        for (entries.items) |entry| a.free(entry);
                        entries.deinit(a);
                    }
                    entries.appendAssumeCapacity(try createSidEntry(a, 0, 5));
                    var i: usize = 0;
                    while (i < 32) : (i += 1) {
                        const tenantID: u64 = @intCast(i + 1);
                        var streamIDs = try std.ArrayList(u128).initCapacity(a, 16);
                        defer streamIDs.deinit(a);
                        for (0..16) |j| {
                            streamIDs.appendAssumeCapacity(i * 100 + j);
                        }
                        entries.appendAssumeCapacity(
                            try createTagRecord(a, tenantID, t, streamIDs.items),
                        );
                    }
                    entries.appendAssumeCapacity(try createSidEntry(a, 100, 999));
                    return entries.toOwnedSlice(a);
                }
            }.f,
            .expectedItemsCount = 34,
            .maxIndexBlockSize = 16384,
        },
    };

    for (cases) |case| {
        const entries = try case.createEntries(alloc, tag);
        defer {
            for (entries) |entry| alloc.free(entry);
            alloc.free(entries);
        }

        var readers = try createTestReaders(alloc, &.{entries}, case.maxIndexBlockSize, decompressionPool);
        defer cleanupReaders(alloc, &readers);

        var memTable = try createTestMemTable(alloc);
        defer memTable.deinit(alloc);

        var writer = BlockWriter.initFromMemTable(memTable, compressionPool);
        defer writer.deinit(alloc);

        var merger = try BlockMerger.init(io, alloc, &readers);
        defer merger.deinit(alloc);

        const tableHeader = try merger.merge(io, alloc, &writer, null);
        defer tableHeader.deinit(alloc);

        try testing.expectEqual(case.expectedItemsCount, tableHeader.entriesCount);
    }
}

test "BlockMerger.merge stopped flag" {
    const alloc = testing.allocator;
    const io = testing.io;
    const maxIndexBlockSize = 1024;
    const compressionPool = try CompressionPool.init(alloc, 1);
    defer compressionPool.deinit(alloc);
    const decompressionPool = try DecompressionPool.init(alloc, 1);
    defer decompressionPool.deinit(alloc);

    var stopped = Stop{};
    stopped.stop(io);
    var readers = try createTestReaders(alloc, &.{&.{ "a", "b", "c" }}, maxIndexBlockSize, decompressionPool);
    defer cleanupReaders(alloc, &readers);

    var memTable = try createTestMemTable(alloc);
    defer memTable.deinit(alloc);

    var writer = BlockWriter.initFromMemTable(memTable, compressionPool);
    defer writer.deinit(alloc);

    var merger = try BlockMerger.init(io, alloc, &readers);
    defer merger.deinit(alloc);

    const res = merger.merge(io, alloc, &writer, &stopped);
    try testing.expectError(error.Stopped, res);
}

test "BlockMerger.merge keeps merged memtable buffers alive after merger deinit" {
    const alloc = testing.allocator;
    const io = testing.io;
    const maxIndexBlockSize = 1024;
    const compressionPool = try CompressionPool.init(alloc, 1);
    defer compressionPool.deinit(alloc);
    const decompressionPool = try DecompressionPool.init(alloc, 1);
    defer decompressionPool.deinit(alloc);

    const leftItems = [_][]const u8{ "a1", "c1", "e1" };
    const rightItems = [_][]const u8{ "b1", "d1", "f1" };
    const expected = [_][]const u8{ "a1", "b1", "c1", "d1", "e1", "f1" };

    const leftBlock = try createTestMemBlock(alloc, &leftItems, maxIndexBlockSize);

    const rightBlock = try createTestMemBlock(alloc, &rightItems, maxIndexBlockSize);

    // memTable is defined out of the block to ensure the source blocks are gone
    const memTable = blk: {
        var leftBlocks = [_]*MemBlock{leftBlock};
        const leftMemTable = try MemTable.init(io, alloc, &leftBlocks, compressionPool, decompressionPool);
        var leftTable = try Table.fromMem(io, alloc, leftMemTable, decompressionPool);
        defer leftTable.close(io);

        var rightBlocks = [_]*MemBlock{rightBlock};
        const rightMemTable = try MemTable.init(io, alloc, &rightBlocks, compressionPool, decompressionPool);
        var rightTable = try Table.fromMem(io, alloc, rightMemTable, decompressionPool);
        defer rightTable.close(io);

        var readers = try std.ArrayList(*BlockReader).initCapacity(alloc, 2);
        defer readers.deinit(alloc);
        try readers.append(alloc, try BlockReader.initFromMemTable(io, alloc, leftTable, decompressionPool));
        try readers.append(alloc, try BlockReader.initFromMemTable(io, alloc, rightTable, decompressionPool));

        var mergedMemTable = try MemTable.empty(alloc);
        errdefer mergedMemTable.deinit(alloc);

        var writer = BlockWriter.initFromMemTable(mergedMemTable, compressionPool);
        defer writer.deinit(alloc);

        var merger = try BlockMerger.init(io, alloc, &readers);
        defer merger.deinit(alloc);

        mergedMemTable.tableHeader = try merger.merge(io, alloc, &writer, null);
        try writer.close(io, alloc);

        readers.items.len = 0;

        try testing.expect(mergedMemTable.entriesBuf.items.len > 0);
        try testing.expect(mergedMemTable.lensBuf.items.len > 0);
        try testing.expect(mergedMemTable.indexBuf.items.len > 0);
        try testing.expect(mergedMemTable.metaindexBuf.items.len > 0);

        break :blk mergedMemTable;
    };
    var table = try Table.fromMem(io, alloc, memTable, decompressionPool);
    defer table.close(io);

    var mergedReader = try BlockReader.initFromMemTable(io, alloc, table, decompressionPool);
    defer mergedReader.deinit(alloc);

    var expectedI: usize = 0;
    var blocksRead: usize = 0;
    while (try mergedReader.next(io, alloc)) {
        blocksRead += 1;
        try testing.expect(blocksRead <= expected.len);
        var iter = mergedReader.block.iterator();
        while (iter.next()) |item| {
            try testing.expect(expectedI < expected.len);
            try testing.expectEqualStrings(expected[expectedI], item);
            expectedI += 1;
        }
    }

    try testing.expectEqual(expected.len, expectedI);
}
