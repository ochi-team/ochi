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

// state
current: []const u8,
blockHeaders: []BlockHeader,
indexBuf: std.ArrayList(u8) = .empty,
compressedIndexBuf: std.ArrayList(u8) = .empty,
entriesBlock: EntriesBlock,

metaindexRecords: []MetaIndex,

isRead: bool,

memBlock: ?*const MemBlock,
memBlockItemIdx: usize,

pub fn init(table: *Table) LookupTable {
    return .{
        .table = table,

        .current = "",
        .isRead = false,
        .blockHeaders = &.{},
        .entriesBlock = .{},
        .metaindexRecords = &.{},
        .memBlock = null,
        .memBlockItemIdx = 0,
    };
}

pub fn deinit(self: *LookupTable, alloc: Allocator) void {
    self.memBlock = null;

    self.indexBuf.deinit(alloc);
    self.compressedIndexBuf.deinit(alloc);

    self.* = undefined;
}

pub fn lessThan(one: LookupTable, another: LookupTable) bool {
    return std.mem.lessThan(u8, one.current, another.current);
}

pub fn seek(self: *LookupTable, alloc: Allocator, key: []const u8) void {
    self.isRead = false;

    if (std.mem.lessThan(u8, self.table.tableHeader.lastItem, key)) {
        self.isRead = true;
        return;
    }

    if (self.seekInMemBlock(key)) {
        return;
    }

    self.seekFromStart(alloc, key);
}

// TODO: utilize understanding of the next block direction
fn seekInMemBlock(self: *LookupTable, key: []const u8) bool {
    const block = self.memBlock orelse return false;

    var items = block.items.items;
    var idx = self.memBlockItemIdx;
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

    self.memBlockItemIdx = idx + lowerBoundBySuffix(items[idx..], key, keyPrefixLen);
    return true;
}

fn seekFromStart(self: *LookupTable, alloc: Allocator, key: []const u8) void {
    self.resetState();

    if (std.mem.eql(u8, key, self.table.tableHeader.firstItem) or
        std.mem.lessThan(u8, key, self.table.tableHeader.firstItem))
    {
        self.nextBlock(alloc) catch unreachable;
        return;
    }

    unreachable;
}

fn resetState(self: *LookupTable) void {
    self.current = "";
    self.blockHeaders = &.{};
    self.indexBuf.clearRetainingCapacity();
    self.compressedIndexBuf.clearRetainingCapacity();

    self.memBlock = null;
    self.memBlockItemIdx = 0;
    self.entriesBlock.reset();

    self.metaindexRecords = self.table.metaindexRecords;
}

// returns the first index with item[prefixLen..] >= keySuffix.
// callers may receive items.len when keySuffix is greater than all suffixes.
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

fn nextBlock(self: *LookupTable, alloc: Allocator) !void {
    if (self.blockHeaders.len == 0) {
        try self.nextBlockHeaders(alloc);
    }

    unreachable;
}

fn nextBlockHeaders(self: *LookupTable, alloc: Allocator) !void {
    if (self.metaindexRecords.len == 0) {
        self.isRead = true;
    }

    const metaIndex = self.metaindexRecords[0];
    self.metaindexRecords = self.metaindexRecords[1..];

    // TODO: cache block headers

    self.blockHeaders = try self.read(alloc, metaIndex);
}

fn read(self: *LookupTable, alloc: Allocator, metaIndex: MetaIndex) ![]BlockHeader {
    try self.compressedIndexBuf.ensureUnusedCapacity(alloc, metaIndex.indexBlockSize);
    // _ = try self.table.indexReader.discard(.limited(metaIndex.indexBlockOffset));
    // const n = try self.table.indexReader.readSliceAll(self.compressedIndexBuf.unusedCapacitySlice());
    // self.compressedIndexBuf.items.len += n;

    unreachable;
    // return BlockHeader.decodeMany(alloc, self.indexBuf.items, metaIndex.blockHeadersCount);
}
