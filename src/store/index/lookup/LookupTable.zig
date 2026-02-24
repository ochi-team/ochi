const std = @import("std");

const MemBlock = @import("../MemBlock.zig");
const Table = @import("../Table.zig");
const strings = @import("../../../stds/strings.zig");

const LookupTable = @This();

table: *Table,

// state
current: []const u8,
isRead: bool,
memBlock: ?*const MemBlock,
memBlockItemIdx: usize,

pub fn init(table: *Table) LookupTable {
    return .{
        .table = table,

        .current = "",
        .isRead = false,
        .memBlock = null,
        .memBlockItemIdx = 0,
    };
}

pub fn deinit(self: *LookupTable) void {
    self.memBlock = null;
    self.* = undefined;
}

pub fn lessThan(one: LookupTable, another: LookupTable) bool {
    return std.mem.lessThan(u8, one.current, another.current);
}

pub fn seek(self: *LookupTable, key: []const u8) void {
    self.isRead = false;

    if (std.mem.lessThan(u8, self.table.tableHeader.lastItem, key)) {
        self.isRead = true;
        return;
    }

    if (self.seekInMemBlock(key)) {
        return;
    }

    self.seekFromStart(key);
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

fn seekFromStart(self: *LookupTable, key: []const u8) void {
    _ = self;
    _ = key;
    unreachable;
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

pub fn next(self: *LookupTable) bool {
    _ = self;
    unreachable;
}
