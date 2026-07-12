const std = @import("std");
const Allocator = std.mem.Allocator;
const Io = std.Io;
const encoding = @import("encoding");

const Cache = @import("../../../stds/Cache.zig").Cache;
const MemBlock = @import("../MemBlock.zig");
const Table = @import("../Table.zig");
const BlockHeader = @import("../BlockHeader.zig");
const MetaIndex = @import("../MetaIndex.zig");
const EntriesBlock = @import("../EntriesBlock.zig");
const strings = @import("../../../stds/strings.zig");
const CompressionPool = @import("../../CompressionPool.zig");

const LookupTable = @This();

// TODO: the cache key is unreliable, if the table address is reused we are cooked,
// handling cache eviction on closing table works too
const memBlocksCacheKey = struct { tableAddr: usize, offset: u64 };
fn memBlocksCacheKeyBuf(buf: []u8, key: memBlocksCacheKey) void {
    var subKey: [8]u8 = undefined;
    subKey = @bitCast(key.tableAddr);
    @memcpy(buf[0..8], subKey[0..]);
    subKey = @bitCast(key.offset);
    @memcpy(buf[8..16], subKey[0..]);
}

table: *Table,
memBlocksCache: *Cache(*MemBlock),
compressionPool: *CompressionPool,
longAllocator: Allocator,
maxMemBlockSize: u32,
// blockHeadersOwned always keeps the base allocation we must free.
blockHeadersOwned: []BlockHeader,

// state

current: []const u8,
// blockHeaders may point into a tail subslice while scanning
blockHeaders: []BlockHeader,
indexBuf: std.ArrayList(u8) = .empty,
compressedIndexBuf: std.ArrayList(u8) = .empty,
entriesBlock: EntriesBlock,

metaIndexRecords: []MetaIndex,

isRead: bool,

// TODO: find out at what point it's null and document it or make non nullable
memBlockPin: ?Cache(*MemBlock).Pinned,
memBlockIdx: usize,

/// Creates a reusable lookup cursor for a single Table
pub fn init(longAlloc: Allocator, table: *Table, maxMemBlockSize: u32, cache: *Cache(*MemBlock), compressionPool: *CompressionPool) LookupTable {
    return .{
        .table = table,
        .memBlocksCache = cache,
        .compressionPool = compressionPool,
        .longAllocator = longAlloc,
        .maxMemBlockSize = maxMemBlockSize,

        .current = "",
        .isRead = false,
        .blockHeaders = &.{},
        .blockHeadersOwned = &.{},
        .entriesBlock = .{},
        .metaIndexRecords = &.{},
        .memBlockPin = null,
        .memBlockIdx = 0,
    };
}

pub fn deinit(self: *LookupTable, alloc: Allocator) void {
    if (self.memBlockPin) |*pinned| pinned.release();
    self.memBlockPin = null;
    if (self.blockHeadersOwned.len > 0) alloc.free(self.blockHeadersOwned);

    self.indexBuf.deinit(alloc);
    self.compressedIndexBuf.deinit(alloc);
    self.entriesBlock.deinit(alloc);

    self.* = undefined;
}

pub fn lessThanPtr(one: *const LookupTable, another: *const LookupTable) bool {
    return std.mem.lessThan(u8, one.current, another.current);
}

/// Positions the cursor to the first item `>= key`.
pub fn seek(self: *LookupTable, io: Io, alloc: Allocator, key: []const u8) !void {
    self.isRead = false;

    if (std.mem.lessThan(u8, self.table.tableHeader().lastEntry, key)) {
        self.isRead = true;
        return;
    }

    if (self.seekInMemBlock(key)) {
        return;
    }

    try self.seekFromStart(io, alloc, key);
}

// TODO: utilize understanding of the next block direction
fn seekInMemBlock(self: *LookupTable, key: []const u8) bool {
    const blockRef = self.memBlockPin orelse return false;
    const block = blockRef.value();

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
    if (std.mem.lessThan(u8, block.getEntry(items[items.len - 1])[keyPrefixLen..], keySuffix)) {
        // key is in the next block
        return false;
    }

    // If key may be below current position, step one item back and verify
    // the key still belongs to this block.
    if (idx > 0) idx -= 1;
    if (std.mem.lessThan(u8, keySuffix, block.getEntry(items[idx])[keyPrefixLen..])) {
        items = items[0..idx];
        if (items.len == 0) return false;

        if (std.mem.lessThan(u8, keySuffix, block.getEntry(items[0])[keyPrefixLen..])) {
            // key is in the previous block
            return false;
        }
        idx = 0;
    }

    self.memBlockIdx = idx + lowerBoundBySuffix(block, items[idx..], key, keyPrefixLen);
    return true;
}

fn seekFromStart(self: *LookupTable, io: Io, alloc: Allocator, key: []const u8) !void {
    self.resetState(alloc);

    const tableHeader = self.table.tableHeader();
    if (std.mem.eql(u8, key, tableHeader.firstEntry) or
        std.mem.lessThan(u8, key, tableHeader.firstEntry))
    {
        _ = try self.nextBlock(io, alloc);
        return;
    }
    std.debug.assert(self.metaIndexRecords.len != 0);

    // binary search can return the first row strictly above the key.
    // step back by one row because the target may belong to the previous range.
    var i = std.sort.binarySearch(MetaIndex, self.metaIndexRecords, key, MetaIndex.compareToKey) orelse 0;
    if (i > 0) i -= 1;
    self.metaIndexRecords = self.metaIndexRecords[i..];
    if (!try self.nextBlockHeaders(io, alloc)) {
        return;
    }

    // same idea at block-header level: include the previous block as candidate.
    i = std.sort.binarySearch(BlockHeader, self.blockHeaders, key, BlockHeader.compareToKey) orelse 0;
    if (i > 0) i -= 1;
    self.blockHeaders = self.blockHeaders[i..];
    if (!try self.nextBlock(io, alloc)) {
        return;
    }

    const memBlock = self.memBlockPin.?.value();
    const keyPrefixLen = strings.findPrefix(memBlock.prefix, key).len;
    self.memBlockIdx = lowerBoundBySuffix(memBlock, memBlock.memEntries.items, key, keyPrefixLen);
    if (self.memBlockIdx < memBlock.memEntries.items.len) {
        return;
    }

    _ = try self.nextBlock(io, alloc);
}

fn resetState(self: *LookupTable, alloc: Allocator) void {
    self.current = "";
    self.blockHeaders = &.{};
    if (self.blockHeadersOwned.len > 0) alloc.free(self.blockHeadersOwned);
    self.blockHeadersOwned = &.{};
    self.indexBuf.clearRetainingCapacity();

    if (self.memBlockPin) |*pinned| pinned.release();
    self.memBlockPin = null;
    self.memBlockIdx = 0;
    self.entriesBlock.reset();

    // Reset to full table scan window; seek narrows this slice later.
    self.metaIndexRecords = self.table.metaIndexRecords;
}

const LowerBoundBySuffixComparator = struct {
    block: *const MemBlock,
    keySuffix: []const u8,
    prefixLen: usize,
};
fn lowerBoundBySuffixComparatorFn(self: LowerBoundBySuffixComparator, item: MemBlock.MemEntry) std.math.Order {
    const itemSuffix = self.block.getEntry(item)[self.prefixLen..];
    const order = std.mem.order(u8, self.keySuffix, itemSuffix);
    return switch (order) {
        .gt => .gt,
        .eq, .lt => .eq,
    };
}

// returns the first index with item[prefixLen..] >= keySuffix.
// callers may receive items.len when keySuffix is greater than all suffixes.
// TODO: test alternative array layout
fn lowerBoundBySuffix(
    block: *const MemBlock,
    items: []const MemBlock.MemEntry,
    key: []const u8,
    prefixLen: usize,
) usize {
    if (items.len == 0) return 0;

    const keySuffix = key[prefixLen..];

    // binarySearch may return any index from the `.eq` range;
    // narrow the searched prefix until we reach its first index.
    const comp = LowerBoundBySuffixComparator{ .block = block, .keySuffix = keySuffix, .prefixLen = prefixLen };
    var result = items.len;
    var search = items;
    while (search.len > 0) {
        const i = std.sort.binarySearch(MemBlock.MemEntry, search, comp, lowerBoundBySuffixComparatorFn) orelse break;
        result = i;
        search = search[0..i];
    }

    return result;
}

/// Moves to the next item in table order.
pub fn next(self: *LookupTable, io: Io, alloc: Allocator) !bool {
    if (self.isRead) return false;

    const memBlock = self.memBlockPin.?.value();
    if (self.memBlockIdx < memBlock.memEntries.items.len) {
        self.current = memBlock.get(self.memBlockIdx);
        self.memBlockIdx += 1;
        return true;
    }

    if (!try self.nextBlock(io, alloc)) {
        return false;
    }

    self.current = self.memBlockPin.?.value().get(0);
    self.memBlockIdx += 1;
    return true;
}

fn nextBlock(self: *LookupTable, io: Io, alloc: Allocator) !bool {
    if (self.blockHeaders.len == 0) {
        const hasNext = try self.nextBlockHeaders(io, alloc);
        if (!hasNext) return false;
    }

    const blockHeader = self.blockHeaders[0];
    const pinned = try self.getMemBlock(io, alloc, blockHeader);
    if (self.memBlockPin) |*existing| existing.release();
    self.memBlockPin = pinned;

    self.blockHeaders = self.blockHeaders[1..];
    self.memBlockIdx = 0;

    return true;
}

fn nextBlockHeaders(self: *LookupTable, io: Io, alloc: Allocator) !bool {
    if (self.metaIndexRecords.len == 0) {
        self.isRead = true;
        return false;
    }

    const metaIndex = self.metaIndexRecords[0];
    self.metaIndexRecords = self.metaIndexRecords[1..];

    // TODO: cache block headers

    const blockHeadersOwned = try self.readBlockHeaders(io, alloc, metaIndex);

    // always free the previous decode batch before loading the next one,
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

fn readBlockHeaders(self: *LookupTable, io: Io, alloc: Allocator, metaIndex: MetaIndex) ![]BlockHeader {
    self.compressedIndexBuf.clearRetainingCapacity();
    try self.compressedIndexBuf.ensureUnusedCapacity(alloc, metaIndex.indexBlockSize);

    const compressedIndex = self.compressedIndexBuf.unusedCapacitySlice()[0..metaIndex.indexBlockSize];
    const compressedLen = try self.table.readIndex(io, compressedIndex, metaIndex.indexBlockOffset);
    std.debug.assert(compressedLen == metaIndex.indexBlockSize);
    self.compressedIndexBuf.items.len = compressedLen;

    self.indexBuf.clearRetainingCapacity();
    const indexSize = try encoding.getFrameContentSize(self.compressedIndexBuf.items);
    try self.indexBuf.ensureUnusedCapacity(alloc, indexSize);
    const n = try self.compressionPool.decompress(io, self.indexBuf.unusedCapacitySlice(), self.compressedIndexBuf.items);
    self.indexBuf.items.len = n;

    return BlockHeader.decodeMany(alloc, self.indexBuf.items, metaIndex.blockHeadersCount);
}

fn getMemBlock(self: *LookupTable, io: Io, alloc: Allocator, blockHeader: BlockHeader) !Cache(*MemBlock).Pinned {
    var keyBuf: [16]u8 = undefined;
    memBlocksCacheKeyBuf(&keyBuf, .{ .tableAddr = @intFromPtr(self.table), .offset = blockHeader.entriesBlockOffset });

    const LoadMemBlockCtx = struct {
        lookupTable: *LookupTable,
        io: Io,
        alloc: Allocator,
        blockHeader: BlockHeader,

        // it must get a block transactionally,
        // otherwise 2 threads create the same block and then one of them removes a duplicate
        // TODO: instrument locking and measure contention
        fn run(ctx: @This()) !*MemBlock {
            const memBlock = try MemBlock.init(ctx.lookupTable.longAllocator, .{
                .maxMemBlockSize = ctx.lookupTable.maxMemBlockSize,
                .blocksCountHint = @intCast(ctx.blockHeader.entriesCount),
            });
            errdefer memBlock.deinit(ctx.lookupTable.longAllocator);

            try ctx.lookupTable.readMemBlock(ctx.io, ctx.alloc, ctx.blockHeader, memBlock);
            return memBlock;
        }
    };

    const res = try self.memBlocksCache.getOrElsePinned(io, keyBuf[0..], LoadMemBlockCtx{
        .lookupTable = self,
        .io = io,
        .alloc = alloc,
        .blockHeader = blockHeader,
    }, LoadMemBlockCtx.run);
    return res.pinned;
}

fn readMemBlock(self: *LookupTable, io: Io, alloc: Allocator, blockHeader: BlockHeader, memBlock: *MemBlock) !void {
    self.entriesBlock.reset();

    // copy exact encoded slices for this block and decode them into a fresh MemBlock.
    try self.entriesBlock.entriesBuf.ensureUnusedCapacity(alloc, blockHeader.entriesBlockSize);
    const itemsDst = self.entriesBlock.entriesBuf.unusedCapacitySlice()[0..blockHeader.entriesBlockSize];
    const entriesLen = try self.table.readEntries(io, itemsDst, blockHeader.entriesBlockOffset);
    std.debug.assert(entriesLen == blockHeader.entriesBlockSize);
    self.entriesBlock.entriesBuf.items.len = blockHeader.entriesBlockSize;

    try self.entriesBlock.lensBuf.ensureUnusedCapacity(alloc, blockHeader.lensBlockSize);
    const lensDst = self.entriesBlock.lensBuf.unusedCapacitySlice()[0..blockHeader.lensBlockSize];
    const lensLen = try self.table.readLens(io, lensDst, blockHeader.lensBlockOffset);
    std.debug.assert(lensLen == blockHeader.lensBlockSize);

    self.entriesBlock.lensBuf.items.len = blockHeader.lensBlockSize;

    try memBlock.decode(
        io,
        alloc,
        self.compressionPool,
        &self.entriesBlock,
        blockHeader.firstEntry,
        blockHeader.prefix,
        blockHeader.entriesCount,
        blockHeader.encodingType,
    );
}

test "lowerBoundBySuffix" {
    const Case = struct {
        name: []const u8,
        items: []const []const u8,
        key: []const u8,
        prefixLen: usize,
        expected: usize,
    };

    const sharedItems = [_][]const u8{
        "stream-001",
        "stream-010",
        "stream-050",
    };

    const variedPrefixItems = [_][]const u8{
        "aa-x",
        "ab-b",
        "ac-c",
    };

    const cases = [_]Case{
        .{
            .name = "empty items returns zero",
            .items = &.{},
            .key = "stream-010",
            .prefixLen = 7,
            .expected = 0,
        },
        .{
            .name = "exact suffix match",
            .items = &sharedItems,
            .key = "stream-010",
            .prefixLen = 7,
            .expected = 1,
        },
        .{
            .name = "between two suffixes",
            .items = &sharedItems,
            .key = "stream-020",
            .prefixLen = 7,
            .expected = 2,
        },
        .{
            .name = "below all suffixes",
            .items = &sharedItems,
            .key = "stream-000",
            .prefixLen = 7,
            .expected = 0,
        },
        .{
            .name = "above all suffixes returns len",
            .items = &sharedItems,
            .key = "stream-999",
            .prefixLen = 7,
            .expected = sharedItems.len,
        },
        .{
            .name = "prefix length zero compares full value",
            .items = &variedPrefixItems,
            .key = "ab-a",
            .prefixLen = 0,
            .expected = 1,
        },
        .{
            .name = "short key suffix falls before first",
            .items = &variedPrefixItems,
            .key = "ab",
            .prefixLen = 2,
            .expected = 0,
        },
    };

    for (cases) |case| {
        var block = try MemBlock.init(std.testing.allocator, .{
            .maxMemBlockSize = 64,
            .blocksCountHint = case.items.len,
        });
        defer block.deinit(std.testing.allocator);
        for (case.items) |item| {
            try std.testing.expect(block.add(item));
        }

        const actual = lowerBoundBySuffix(block, block.memEntries.items, case.key, case.prefixLen);
        try std.testing.expectEqual(case.expected, actual);
    }
}

test "memBlocksCacheKeyBuf" {
    var buf: [16]u8 = undefined;
    memBlocksCacheKeyBuf(&buf, .{ .tableAddr = 42, .offset = 53 });

    const expected: [16]u8 = .{ 42, 0, 0, 0, 0, 0, 0, 0, 53, 0, 0, 0, 0, 0, 0, 0 };
    try std.testing.expectEqualStrings(&expected, &buf);
}
