const std = @import("std");
const Allocator = std.mem.Allocator;
const encoding = @import("encoding");

const MemBlock = @import("../MemBlock.zig");
const Table = @import("../Table.zig");
const BlockHeader = @import("../BlockHeader.zig");
const MetaIndex = @import("../MetaIndex.zig");
const EntriesBlock = @import("../EntrieBlock.zig");
const strings = @import("../../../stds/strings.zig");

const LookupTable = @This();

table: *Table,
maxMemBlockSize: u32,
// blockHeadersOwned always keeps the base allocation we must free.
blockHeadersOwned: []BlockHeader,

// state

current: []const u8,
// blockHeaders may point into a tail subslice while scanning
blockHeaders: []BlockHeader,
indexBuf: std.ArrayList(u8) = .empty,
entriesBlock: EntriesBlock,

metaindexRecords: []MetaIndex,

isRead: bool,

memBlock: ?*MemBlock,
memBlockIdx: usize,

/// Creates a reusable lookup cursor for a single Table
pub fn init(table: *Table, maxMemBlockSize: u32) LookupTable {
    return .{
        .table = table,
        .maxMemBlockSize = maxMemBlockSize,

        .current = "",
        .isRead = false,
        .blockHeaders = &.{},
        .blockHeadersOwned = &.{},
        .entriesBlock = .{},
        .metaindexRecords = &.{},
        .memBlock = null,
        .memBlockIdx = 0,
    };
}

pub fn deinit(self: *LookupTable, alloc: Allocator) void {
    if (self.memBlock) |memBlock| memBlock.deinit(alloc);
    self.memBlock = null;
    if (self.blockHeadersOwned.len > 0) alloc.free(self.blockHeadersOwned);

    self.indexBuf.deinit(alloc);
    self.entriesBlock.deinit(alloc);

    self.* = undefined;
}

pub fn lessThan(one: LookupTable, another: LookupTable) bool {
    return std.mem.lessThan(u8, one.current, another.current);
}

/// Positions the cursor to the first item `>= key`.
pub fn seek(self: *LookupTable, alloc: Allocator, key: []const u8) !void {
    self.isRead = false;

    if (std.mem.lessThan(u8, self.table.tableHeader.lastEntry, key)) {
        self.isRead = true;
        return;
    }

    if (self.seekInMemBlock(key)) {
        return;
    }

    try self.seekFromStart(alloc, key);
}

// TODO: utilize understanding of the next block direction
fn seekInMemBlock(self: *LookupTable, key: []const u8) bool {
    const block = self.memBlock orelse return false;

    var items = block.memEntries.items;
    var idx = self.memBlockIdx;
    if (idx >= items.len) {
        // block is over
        return false;
    }

    const keyPrefixLen = strings.findPrefix(block.prefix, key).len;
    const keySuffix = key[keyPrefixLen..];

    // check upper bound,
    // If key is greater than the block upper bound, caller must load next block.
    if (std.mem.lessThan(u8, items[items.len - 1][keyPrefixLen..], keySuffix)) {
        // key is in the next block
        return false;
    }

    // If key may be below current position, step one item back and verify
    // the key still belongs to this block.
    if (idx > 0) idx -= 1;
    if (std.mem.lessThan(u8, keySuffix, items[idx][keyPrefixLen..])) {
        items = items[0..idx];
        if (items.len == 0) return false;

        if (std.mem.lessThan(u8, keySuffix, items[0][keyPrefixLen..])) {
            // key is in the previous block
            return false;
        }
        idx = 0;
    }

    self.memBlockIdx = idx + lowerBoundBySuffix(items[idx..], key, keyPrefixLen);
    return true;
}

fn seekFromStart(self: *LookupTable, alloc: Allocator, key: []const u8) !void {
    self.resetState(alloc);

    if (std.mem.eql(u8, key, self.table.tableHeader.firstEntry) or
        std.mem.lessThan(u8, key, self.table.tableHeader.firstEntry))
    {
        _ = try self.nextBlock(alloc);
        return;
    }
    std.debug.assert(self.metaindexRecords.len != 0);

    // Binary search can return the first row strictly above the key.
    // Step back by one row because the target may belong to the previous range.
    var i = std.sort.binarySearch(MetaIndex, self.metaindexRecords, key, MetaIndex.compareToKey) orelse 0;
    if (i > 0) i -= 1;
    self.metaindexRecords = self.metaindexRecords[i..];
    if (!try self.nextBlockHeaders(alloc)) {
        return;
    }

    // Same idea at block-header level: include the previous block as candidate.
    i = std.sort.binarySearch(BlockHeader, self.blockHeaders, key, BlockHeader.compareToKey) orelse 0;
    if (i > 0) i -= 1;
    self.blockHeaders = self.blockHeaders[i..];
    if (!try self.nextBlock(alloc)) {
        return;
    }

    const keyPrefixLen = strings.findPrefix(self.memBlock.?.prefix, key).len;
    self.memBlockIdx = lowerBoundBySuffix(self.memBlock.?.memEntries.items, key, keyPrefixLen);
    if (self.memBlockIdx < self.memBlock.?.memEntries.items.len) {
        return;
    }

    _ = try self.nextBlock(alloc);
}

fn resetState(self: *LookupTable, alloc: Allocator) void {
    self.current = "";
    self.blockHeaders = &.{};
    if (self.blockHeadersOwned.len > 0) alloc.free(self.blockHeadersOwned);
    self.blockHeadersOwned = &.{};
    self.indexBuf.clearRetainingCapacity();

    if (self.memBlock) |memBlock| memBlock.deinit(alloc);
    self.memBlock = null;
    self.memBlockIdx = 0;
    self.entriesBlock.reset();

    // Reset to full table scan window; seek narrows this slice later.
    self.metaindexRecords = self.table.metaindexRecords;
}

// returns the first index with item[prefixLen..] >= keySuffix.
// callers may receive items.len when keySuffix is greater than all suffixes.
// TODO: replace to std.order.binarySearch
// TODO: test alternative array layout
fn lowerBoundBySuffix(items: []const []const u8, key: []const u8, prefixLen: usize) usize {
    if (items.len == 0) return 0;

    const keySuffix = key[prefixLen..];

    var lo: usize = 0;
    var hi: usize = items.len;
    while (lo < hi) {
        const mid = lo + (hi - lo) / 2;
        if (std.mem.lessThan(u8, items[mid][prefixLen..], keySuffix)) {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    return lo;
}

/// Moves to the next item in table order.
pub fn next(self: *LookupTable, alloc: Allocator) !bool {
    if (self.isRead) return false;

    if (self.memBlockIdx < self.memBlock.?.memEntries.items.len) {
        self.current = self.memBlock.?.memEntries.items[self.memBlockIdx];
        self.memBlockIdx += 1;
        return true;
    }

    if (!try self.nextBlock(alloc)) {
        return false;
    }

    self.current = self.memBlock.?.memEntries.items[0];
    self.memBlockIdx += 1;
    return true;
}

fn nextBlock(self: *LookupTable, alloc: Allocator) !bool {
    if (self.blockHeaders.len == 0) {
        const hasNext = try self.nextBlockHeaders(alloc);
        if (!hasNext) return false;
    }

    const blockHeader = self.blockHeaders[0];
    const newMemBlock = try self.getMemBlock(blockHeader);

    // Each block decode allocates a new MemBlock; free previous one first.
    if (self.memBlock) |memBlock| memBlock.deinit(alloc);
    self.memBlock = newMemBlock;
    self.blockHeaders = self.blockHeaders[1..];
    self.memBlockIdx = 0;

    return true;
}

fn nextBlockHeaders(self: *LookupTable, alloc: Allocator) !bool {
    if (self.metaindexRecords.len == 0) {
        self.isRead = true;
        return false;
    }

    const metaIndex = self.metaindexRecords[0];
    self.metaindexRecords = self.metaindexRecords[1..];

    // TODO: cache block headers

    const blockHeadersOwned = try self.readBlockHeaders(alloc, metaIndex);

    // Always free the previous decode batch before loading the next one,
    // only after readBlockHeaders went successful
    if (self.blockHeadersOwned.len > 0) {
        alloc.free(self.blockHeadersOwned);
        self.blockHeadersOwned = &.{};
        self.blockHeaders = &.{};
    }
    self.blockHeadersOwned = blockHeadersOwned;
    self.blockHeaders = self.blockHeadersOwned;
    return true;
}

fn readBlockHeaders(self: *LookupTable, alloc: Allocator, metaIndex: MetaIndex) ![]BlockHeader {
    const end = metaIndex.indexBlockOffset + metaIndex.indexBlockSize;
    const compressedIndex = self.table.indexBuf[metaIndex.indexBlockOffset..end];

    const indexSize = try encoding.getFrameContentSize(compressedIndex);
    try self.indexBuf.ensureUnusedCapacity(alloc, indexSize);
    const n = try encoding.decompress(self.indexBuf.unusedCapacitySlice(), compressedIndex);
    std.debug.assert(n == indexSize);
    self.indexBuf.items.len = n;

    return BlockHeader.decodeMany(alloc, self.indexBuf.items, metaIndex.blockHeadersCount);
}

fn getMemBlock(self: *LookupTable, blockHeader: BlockHeader) !*MemBlock {
    // TODO: potentially we can cache a block
    return self.readMemBlock(blockHeader);
}

fn readMemBlock(self: *LookupTable, blockHeader: BlockHeader) !*MemBlock {
    const alloc = self.table.alloc;

    self.entriesBlock.reset();

    // Copy exact encoded slices for this block and decode them into a fresh MemBlock.
    try self.entriesBlock.entriesBuf.ensureUnusedCapacity(alloc, blockHeader.entriesBlockSize);
    const itemsStart: usize = @intCast(blockHeader.entriesBlockOffset);
    const itemsEnd = itemsStart + blockHeader.entriesBlockSize;
    const itemsSrc = self.table.entriesBuf[itemsStart..itemsEnd];
    const itemsDst = self.entriesBlock.entriesBuf.unusedCapacitySlice()[0..blockHeader.entriesBlockSize];
    @memcpy(itemsDst, itemsSrc);

    std.debug.assert(itemsSrc.len == blockHeader.entriesBlockSize);
    self.entriesBlock.entriesBuf.items.len = blockHeader.entriesBlockSize;

    try self.entriesBlock.lensBuf.ensureUnusedCapacity(alloc, blockHeader.lensBlockSize);
    const lensStart: usize = @intCast(blockHeader.lensBlockOffset);
    const lensEnd = lensStart + blockHeader.lensBlockSize;
    const lensSrc = self.table.lensBuf[lensStart..lensEnd];
    const lensDst = self.entriesBlock.lensBuf.unusedCapacitySlice()[0..blockHeader.lensBlockSize];
    @memcpy(lensDst, lensSrc);

    std.debug.assert(lensSrc.len == blockHeader.lensBlockSize);
    self.entriesBlock.lensBuf.items.len = blockHeader.lensBlockSize;

    var memBlock = try MemBlock.init(alloc, self.maxMemBlockSize);
    try memBlock.decode(
        alloc,
        &self.entriesBlock,
        blockHeader.firstEntry,
        blockHeader.prefix,
        blockHeader.entriesCount,
        blockHeader.encodingType,
    );

    return memBlock;
}
