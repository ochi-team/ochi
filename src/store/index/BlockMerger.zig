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
//! - Can be stopped mid-merge via atomic stopped flag

const std = @import("std");
const Allocator = std.mem.Allocator;
const Io = std.Io;

const Conf = @import("../../Conf.zig");
const BlockReader = @import("BlockReader.zig");
const MemBlock = @import("MemBlock.zig");
const MemEntry = MemBlock.MemEntry;
const BlockWriter = @import("BlockWriter.zig");
const TableHeader = @import("TableHeader.zig");
const IndexKind = @import("Index.zig").IndexKind;
const TagRecordsMerger = @import("TagRecordsMerger.zig");
const MemTable = @import("MemTable.zig");
const Table = @import("Table.zig");

const Heap = @import("../../stds/heap.zig").Heap;

const maxStreamsPerRecord = 32;

const BlockMerger = @This();

heap: Heap(*BlockReader, BlockReader.blockReaderLessThan),
exhaustedReaders: std.ArrayList(*BlockReader),
block: *MemBlock,

/// init creates a BlockMerger instance from the readers
/// be aware it mutates readers list inside
pub fn init(io: Io, alloc: Allocator, readers: *std.ArrayList(*BlockReader)) !BlockMerger {
    // TODO: collect metrics and experiment with flat array on 1-3 elements
    // TODO: experiment with Loser tree intead of heap:
    // https://grafana.com/blog/the-loser-tree-data-structure-how-to-optimize-merges-and-make-your-programs-run-faster/
    var exhaustedReaders = try std.ArrayList(*BlockReader).initCapacity(alloc, readers.items.len);
    errdefer exhaustedReaders.deinit(alloc);

    var i: usize = 0;
    while (i < readers.items.len) {
        const reader = readers.items[i];
        const hasNext = try reader.next(io, alloc);
        if (!hasNext) {
            reader.deinit(alloc);
            _ = readers.swapRemove(i);
            continue;
        }
        i += 1;
    }

    var heap = Heap(*BlockReader, BlockReader.blockReaderLessThan).init(alloc, readers);
    heap.heapify();

    return .{
        .heap = heap,
        .exhaustedReaders = exhaustedReaders,
        .block = try MemBlock.init(alloc, .{}),
    };
}

pub fn deinit(self: *BlockMerger, alloc: Allocator) void {
    for (self.exhaustedReaders.items) |reader| {
        reader.deinit(alloc);
    }
    self.exhaustedReaders.deinit(alloc);
    self.block.deinit(alloc);
}

pub fn merge(
    self: *BlockMerger,
    io: Io,
    alloc: Allocator,
    writer: *BlockWriter,
    stopped: ?*const std.atomic.Value(bool),
) !TableHeader {
    var tableHeader = TableHeader{};
    errdefer tableHeader.deinit(alloc);
    while (true) {
        if (self.heap.len() == 0) {
            // done, exit path
            try self.flush(io, alloc, writer, &tableHeader);
            return tableHeader;
        }

        if (stopped) |s| {
            if (s.load(.acquire)) return error.Stopped;
        }

        const reader = self.heap.array.items[0];
        var nextItem: []const u8 = "";
        var hasNextItem = false;

        if (self.heap.len() > 1) {
            const nReader = self.heap.peekNext().?;
            nextItem = nReader.current();
            hasNextItem = true;
        }

        const itemsLen = reader.block.memEntries.items.len;
        var compareEveryItem = true;
        if (reader.currentI < itemsLen) {
            const lastItem = reader.block.last();
            compareEveryItem = hasNextItem and (std.mem.order(u8, lastItem, nextItem) == .gt);
        }

        while (reader.currentI < itemsLen) {
            const item = reader.current();
            if (compareEveryItem and (std.mem.order(u8, item, nextItem) == .gt)) {
                break;
            }

            if (!self.block.add(item)) {
                try self.flush(io, alloc, writer, &tableHeader);
                continue;
            }
            reader.currentI += 1;
        }

        if (reader.currentI == itemsLen) {
            if (try reader.next(io, alloc)) {
                self.heap.fix(0);
                continue;
            }

            // Reader.next() rewrites its decoded block buffer. Keep currently
            // buffered items valid by owning keeping bytes before advancing,
            // clean them in the end on deinit
            const exhausted = self.heap.pop();
            self.exhaustedReaders.appendAssumeCapacity(exhausted);
            continue;
        }

        self.heap.fix(0);
    }
}

fn flush(
    self: *BlockMerger,
    io: Io,
    alloc: Allocator,
    writer: *BlockWriter,
    tableHeader: *TableHeader,
) !void {
    if (self.block.memEntries.items.len == 0) {
        return;
    }

    const originalFirst = try alloc.dupe(u8, self.block.get(0));
    defer alloc.free(originalFirst);
    const originalLast = try alloc.dupe(u8, self.block.last());
    defer alloc.free(originalLast);

    try self.mergeTagsRecords(alloc);

    if (self.block.memEntries.items.len == 0) {
        // nothing to flush
        return;
    }

    const blockLastEntry = self.block.last();

    // TODO: move this validation to tests and test the block is sorted
    std.debug.assert(std.mem.order(u8, self.block.get(0), originalFirst) != .lt);
    std.debug.assert(std.mem.order(u8, blockLastEntry, originalLast) != .gt);
    std.debug.assert(self.block.isSorted());

    tableHeader.entriesCount += self.block.memEntries.items.len;
    if (tableHeader.firstEntry.len == 0) {
        tableHeader.firstEntry = try alloc.dupe(u8, self.block.get(0));
    }

    const newLast = try alloc.dupe(u8, blockLastEntry);
    if (tableHeader.lastEntry.len > 0) {
        alloc.free(tableHeader.lastEntry);
    }
    tableHeader.lastEntry = newLast;

    try writer.writeBlock(io, alloc, self.block);
    tableHeader.blocksCount += 1;
    self.block.reset();
}

// TODO: the implementation is very error prone:
// 1. it copies block from the beginning and mutates original input,
// therefore on the fallback case it removes copy and returns the original array,
// but creating another destination array is memory consuming
// 2. writeState takes 2 buffers instead of a block, it manages the entire memory ownership,
// not just 2 buffers
fn mergeTagsRecords(self: *BlockMerger, alloc: Allocator) !void {
    const itemsLen = self.block.memEntries.items.len;
    if (itemsLen <= 2) {
        return;
    }

    const firstItem = self.block.get(0);
    if (firstItem.len > 0 and firstItem[0] > @intFromEnum(IndexKind.tagToSids)) {
        return;
    }

    const lastItem = self.block.last();
    if (lastItem.len > 0 and lastItem[0] < @intFromEnum(IndexKind.tagToSids)) {
        // nothing to merge, there are no tags -> stream records
        return;
    }

    var maxMergedBytes: usize = 0;
    var iter = self.block.iterator();
    while (iter.next()) |item| {
        maxMergedBytes += item.len;
    }

    // TODO: review concurrent writing model whether it's possible to optimize further
    // and avoid block copy;
    // Options:
    // 1. Instead of copying the entire items slice upfront,
    // detect the unsorted condition earlier by checking for duplicate streamIDs during the merge process:
    // if (block.hasDuplicateStreams()) {
    // return // Skip merging for this batch
    // }
    // 2. The current deduplication creates a new slice. it could optimize it by doing in-place deduplication when possible
    // benchmark both

    // TODO: don't copy the items since the beginning, but create a destination and build it,
    // then as a fallback returns src, it allows not to dupe firstItem/lastItem in flush above

    // Copy source bytes so merged output never aliases mutable destination memory.
    var sourceBuf = try std.ArrayList(u8).initCapacity(alloc, maxMergedBytes);
    defer sourceBuf.deinit(alloc);
    var sourceEntries = try std.ArrayList(MemEntry).initCapacity(alloc, itemsLen);
    defer sourceEntries.deinit(alloc);

    iter = self.block.iterator();
    while (iter.next()) |item| {
        const start: u16 = @intCast(sourceBuf.items.len);
        sourceBuf.appendSliceAssumeCapacity(item);
        sourceEntries.appendAssumeCapacity(.{ .start = start, .end = @intCast(sourceBuf.items.len) });
    }

    self.block.memEntries.clearRetainingCapacity();
    self.block.buf.clearRetainingCapacity();
    self.block.prefix = "";
    try self.block.buf.ensureUnusedCapacity(alloc, maxMergedBytes);

    var tagRecordsMerger: TagRecordsMerger = .{};
    defer tagRecordsMerger.deinit(alloc);

    for (0..sourceEntries.items.len) |i| {
        const itemRange = sourceEntries.items[i];
        const item = sourceBuf.items[itemRange.start..itemRange.end];
        if (item.len == 0 or item[0] != @intFromEnum(IndexKind.tagToSids) or i == 0 or i == sourceEntries.items.len - 1) {
            try tagRecordsMerger.writeState(alloc, self.block);

            const ok = self.block.add(item);
            std.debug.assert(ok);
            continue;
        }

        try tagRecordsMerger.state.setup(item);
        if (tagRecordsMerger.state.streamsLen() > maxStreamsPerRecord) {
            try tagRecordsMerger.writeState(alloc, self.block);

            const ok = self.block.add(item);
            std.debug.assert(ok);
            continue;
        }

        if (!tagRecordsMerger.statesPrefixEqual()) {
            try tagRecordsMerger.writeState(alloc, self.block);
        }

        try tagRecordsMerger.state.parseStreamIDs(alloc);
        try tagRecordsMerger.moveParsedState(alloc);

        if (tagRecordsMerger.streamIDs.items.len >= maxStreamsPerRecord) {
            try tagRecordsMerger.writeState(alloc, self.block);
        }
    }

    std.debug.assert(tagRecordsMerger.streamIDs.items.len == 0);
    if (!self.block.isSorted()) {
        // defend against parallel writing leaving the state unmerged,
        // fallback to the original data
        self.block.buf.clearRetainingCapacity();
        self.block.memEntries.clearRetainingCapacity();
        for (sourceEntries.items) |item| {
            const ok = self.block.add(sourceBuf.items[item.start..item.end]);
            std.debug.assert(ok);
        }
    }
}

const testing = std.testing;
const SID = @import("../lines.zig").SID;
const Field = @import("../lines.zig").Field;
const Encoder = @import("encoding").Encoder;

// Helper functions for tests

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
        const reader = try BlockReader.initFromMovedMemBlock(alloc, block);
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
        var readers = try createTestReaders(alloc, case.blocks, maxIndexBlockSize);
        defer cleanupReaders(alloc, &readers);

        var memTable = try createTestMemTable(alloc);
        defer memTable.deinit(alloc);

        var writer = BlockWriter.initFromMemTable(memTable);
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

        var readers = try createTestReaders(alloc, &blocks, maxIndexBlockSize);
        defer cleanupReaders(alloc, &readers);

        var memTable = try createTestMemTable(alloc);
        defer memTable.deinit(alloc);

        var writer = BlockWriter.initFromMemTable(memTable);
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

    var readers = try createTestReaders(alloc, &mixedBlocks, maxIndexBlockSize);
    defer cleanupReaders(alloc, &readers);

    var memTable = try createTestMemTable(alloc);
    defer memTable.deinit(alloc);

    var writer = BlockWriter.initFromMemTable(memTable);
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
                    const entry = try TagRecordsMerger.createTagRecord(a, 1, t, &[_]u128{ 100, 200 });
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
                    entries.appendAssumeCapacity(try TagRecordsMerger.createTagRecord(a, 2, t, &[_]u128{100}));
                    entries.appendAssumeCapacity(try TagRecordsMerger.createTagRecord(a, 2, t, &[_]u128{200}));
                    entries.appendAssumeCapacity(try TagRecordsMerger.createTagRecord(a, 3, t, &[_]u128{300}));
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
                    entries.appendAssumeCapacity(try TagRecordsMerger.createTagRecord(a, 2, t, &[_]u128{100}));
                    entries.appendAssumeCapacity(try TagRecordsMerger.createTagRecord(a, 3, t, &[_]u128{200}));
                    entries.appendAssumeCapacity(try TagRecordsMerger.createTagRecord(a, 4, t, &[_]u128{300}));
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
                    entries.appendAssumeCapacity(try TagRecordsMerger.createTagRecord(a, 1, t, &[_]u128{100}));
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
                        try TagRecordsMerger.createTagRecord(a, 1, t, streamIDs1.items),
                    );
                    // Second record with streamIDs: [100, 400]
                    // Would come after deduplicated first record [100, 500]
                    // but 500 > 400, making merged output unsorted
                    entries.appendAssumeCapacity(
                        try TagRecordsMerger.createTagRecord(a, 1, t, &[_]u128{ 10, 40 }),
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
                            try TagRecordsMerger.createTagRecord(a, tenantID, t, streamIDs.items),
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

        var readers = try createTestReaders(alloc, &.{entries}, case.maxIndexBlockSize);
        defer cleanupReaders(alloc, &readers);

        var memTable = try createTestMemTable(alloc);
        defer memTable.deinit(alloc);

        var writer = BlockWriter.initFromMemTable(memTable);
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

    var stopped = std.atomic.Value(bool).init(true);
    var readers = try createTestReaders(alloc, &.{&.{ "a", "b", "c" }}, maxIndexBlockSize);
    defer cleanupReaders(alloc, &readers);

    var memTable = try createTestMemTable(alloc);
    defer memTable.deinit(alloc);

    var writer = BlockWriter.initFromMemTable(memTable);
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

    const leftItems = [_][]const u8{ "a1", "c1", "e1" };
    const rightItems = [_][]const u8{ "b1", "d1", "f1" };
    const expected = [_][]const u8{ "a1", "b1", "c1", "d1", "e1", "f1" };

    const leftBlock = try createTestMemBlock(alloc, &leftItems, maxIndexBlockSize);

    const rightBlock = try createTestMemBlock(alloc, &rightItems, maxIndexBlockSize);

    // memTable is defined out of the block to ensure the source blocks are gone
    const memTable = blk: {
        var leftBlocks = [_]*MemBlock{leftBlock};
        const leftMemTable = try MemTable.init(io, alloc, &leftBlocks);
        var leftTable = try Table.fromMem(alloc, leftMemTable);
        defer leftTable.close(io);

        var rightBlocks = [_]*MemBlock{rightBlock};
        const rightMemTable = try MemTable.init(io, alloc, &rightBlocks);
        var rightTable = try Table.fromMem(alloc, rightMemTable);
        defer rightTable.close(io);

        var readers = try std.ArrayList(*BlockReader).initCapacity(alloc, 2);
        defer readers.deinit(alloc);
        try readers.append(alloc, try BlockReader.initFromMemTable(alloc, leftTable));
        try readers.append(alloc, try BlockReader.initFromMemTable(alloc, rightTable));

        var mergedMemTable = try MemTable.empty(alloc);
        errdefer mergedMemTable.deinit(alloc);

        var writer = BlockWriter.initFromMemTable(mergedMemTable);
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
    var table = try Table.fromMem(alloc, memTable);
    defer table.close(io);

    var mergedReader = try BlockReader.initFromMemTable(alloc, table);
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
