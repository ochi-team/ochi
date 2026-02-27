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

// state
current: []const u8,
blockHeaders: []BlockHeader,
indexBuf: std.ArrayList(u8) = .empty,
compressedIndexBuf: std.ArrayList(u8) = .empty,
entriesBlock: EntriesBlock,

metaindexRecords: []MetaIndex,

isRead: bool,

memBlock: ?*MemBlock,
memBlockIdx: usize,

pub fn init(table: *Table, maxMemBlockSize: u32) LookupTable {
    return .{
        .table = table,
        .maxMemBlockSize = maxMemBlockSize,

        .current = "",
        .isRead = false,
        .blockHeaders = &.{},
        .entriesBlock = .{},
        .metaindexRecords = &.{},
        .memBlock = null,
        .memBlockIdx = 0,
    };
}

pub fn deinit(self: *LookupTable, alloc: Allocator) void {
    if (self.memBlock) |memBlock| memBlock.deinit(alloc);
    self.memBlock = null;

    self.indexBuf.deinit(alloc);
    self.compressedIndexBuf.deinit(alloc);
    self.entriesBlock.deinit(alloc);

    self.* = undefined;
}

pub fn lessThan(one: LookupTable, another: LookupTable) bool {
    return std.mem.lessThan(u8, one.current, another.current);
}

pub fn seek(self: *LookupTable, alloc: Allocator, key: []const u8) !void {
    self.isRead = false;

    if (std.mem.lessThan(u8, self.table.tableHeader.lastItem, key)) {
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

    var items = block.items.items;
    var idx = self.memBlockIdx;
    if (idx >= items.len) {
        // block is over
        return false;
    }

    const keyPrefixLen = strings.findPrefix(block.prefix, key).len;
    const keySuffix = key[keyPrefixLen..];

    // check upper blound,
    // if the key is out then it's in the next block
    if (std.mem.lessThan(u8, items[items.len - 1][keyPrefixLen..], keySuffix)) {
        // key is in the next block
        return false;
    }

    // check lower bound,
    // if the key is out then it's in the previous block
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
    self.resetState();

    if (std.mem.eql(u8, key, self.table.tableHeader.firstItem) or
        std.mem.lessThan(u8, key, self.table.tableHeader.firstItem))
    {
        _ = try self.nextBlock(alloc);
        return;
    }
    std.debug.assert(self.metaindexRecords.len != 0);

    var i = std.sort.binarySearch(MetaIndex, self.metaindexRecords, key, MetaIndex.compareToKey) orelse 0;
    if (i > 0) i -= 1;
    self.metaindexRecords = self.metaindexRecords[i..];
    if (!try self.nextBlockHeaders(alloc)) {
        return;
    }

    i = std.sort.binarySearch(BlockHeader, self.blockHeaders, key, BlockHeader.compareToKey) orelse 0;
    if (i > 0) i -= 1;
    self.blockHeaders = self.blockHeaders[i..];
    if (!try self.nextBlock(alloc)) {
        return;
    }

    const keyPrefixLen = strings.findPrefix(self.memBlock.?.prefix, key).len;
    self.memBlockIdx = lowerBoundBySuffix(self.memBlock.?.items.items, key, keyPrefixLen);
    if (self.memBlockIdx < self.memBlock.?.items.items.len) {
        return;
    }

    _ = try self.nextBlock(alloc);
}

fn resetState(self: *LookupTable) void {
    self.current = "";
    self.blockHeaders = &.{};
    self.indexBuf.clearRetainingCapacity();
    self.compressedIndexBuf.clearRetainingCapacity();

    self.memBlock = null;
    self.memBlockIdx = 0;
    self.entriesBlock.reset();

    self.metaindexRecords = self.table.metaindexRecords;
}

// returns the first index with item[prefixLen..] >= keySuffix.
// callers may receive items.len when keySuffix is greater than all suffixes.
// TODO: replace to std.order.binarySearch
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

pub fn next(self: *const LookupTable) bool {
    _ = self;
    unreachable;
}

fn nextBlock(self: *LookupTable, alloc: Allocator) !bool {
    if (self.blockHeaders.len == 0) {
        const hasNext = try self.nextBlockHeaders(alloc);
        if (!hasNext) return false;
    }

    const blockHeader = self.blockHeaders[0];
    self.memBlock = try self.getMemBlock(blockHeader);
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

    self.blockHeaders = try self.readBlockHeaders(alloc, metaIndex);
    return true;
}

fn readBlockHeaders(self: *LookupTable, alloc: Allocator, metaIndex: MetaIndex) ![]BlockHeader {
    try self.compressedIndexBuf.ensureUnusedCapacity(alloc, metaIndex.indexBlockSize);
    const end = metaIndex.indexBlockOffset + metaIndex.indexBlockSize;
    const compressedIndex = self.table.indexBuf[metaIndex.indexBlockOffset..end];

    const indexSize = try encoding.getFrameContentSize(compressedIndex);
    try self.indexBuf.ensureUnusedCapacity(alloc, indexSize);
    const n = try encoding.decompress(self.indexBuf.items, compressedIndex);
    std.debug.assert(n == indexSize);
    self.indexBuf.items.len += indexSize;

    return BlockHeader.decodeMany(alloc, self.indexBuf.items, metaIndex.blockHeadersCount);
}

fn getMemBlock(self: *LookupTable, blockHeader: BlockHeader) !*MemBlock {
    // TODO: potentially we can cache a block
    return self.readMemBlock(blockHeader);
}

fn readMemBlock(self: *LookupTable, blockHeader: BlockHeader) !*MemBlock {
    const alloc = self.table.alloc;

    self.entriesBlock.reset();

    try self.entriesBlock.entriesBuf.ensureUnusedCapacity(alloc, blockHeader.entriesBlockSize);
    const itemsStart: usize = @intCast(blockHeader.entriesBlockOffset);
    const itemsEnd = itemsStart + blockHeader.entriesBlockSize;
    const itemsSrc = self.table.entriesBuf[itemsStart..itemsEnd];
    @memmove(self.entriesBlock.entriesBuf.unusedCapacitySlice(), itemsSrc);
    std.debug.assert(itemsSrc.len == blockHeader.entriesBlockSize);
    self.entriesBlock.entriesBuf.items.len = blockHeader.entriesBlockSize;

    try self.entriesBlock.lensBuf.ensureUnusedCapacity(alloc, blockHeader.lensBlockSize);
    const lensStart: usize = @intCast(blockHeader.lensBlockOffset);
    const lensEnd = lensStart + blockHeader.lensBlockSize;
    const lensSrc = self.table.lensBuf[lensStart..lensEnd];
    @memmove(self.entriesBlock.lensBuf.unusedCapacitySlice(), lensSrc);
    std.debug.assert(lensSrc.len == blockHeader.lensBlockSize);
    self.entriesBlock.lensBuf.items.len = blockHeader.lensBlockSize;

    var memBlock = try MemBlock.init(alloc, self.maxMemBlockSize);
    try memBlock.decode(
        alloc,
        &self.entriesBlock,
        blockHeader.firstItem,
        blockHeader.prefix,
        blockHeader.entriesCount,
        blockHeader.encodingType,
    );

    return memBlock;
}
