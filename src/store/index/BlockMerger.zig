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
const Io = std.Io;

const Stop = @import("../../stds/Stop.zig");
const BlockReader = @import("BlockReader.zig");
const MemBlock = @import("MemBlock.zig");
const MemEntry = MemBlock.MemEntry;
const BlockWriter = @import("BlockWriter.zig");
const TableHeader = @import("TableHeader.zig");
const IndexKind = @import("Index.zig").IndexKind;
const TagRecordsMerger = @import("TagRecordsMerger.zig");

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
    stopped: ?*const Stop,
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
            if (s.isStopped()) return error.Stopped;
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
