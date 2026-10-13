const std = @import("std");
const Allocator = std.mem.Allocator;
const assert = std.debug.assert;

const Table = @import("../data/Table.zig");
const BlockHeader = @import("../data/BlockHeader.zig");
const Query = @import("../../query/Query.zig");
const FilterExpression = Query.FilterExpression;

const Tokenizer = @import("../bloom/Tokenizer.zig");
const tokenHashes = @import("../bloom/bloom.zig").tokenHashes;

const Logger = @import("logging");
const Decoder = @import("encoding").Decoder;

const BlockResponse = @import("BlockResponse.zig");
const clause = @import("clause.zig");

const ColumnsHeaderIndex = @import("../../store/data/ColumnsHeaderIndex.zig");
const Column = @import("../../store/data/Column.zig");
const ColumnHeader = @import("../../store/data/ColumnHeader.zig");

const BlockQuery = @This();

alloc: Allocator,
columnIdxs: *const std.StringHashMapUnmanaged(u16),

// caches, initialized empty
invariantColumnCache: std.ArrayList(Column) = .empty,
columnHeaderCache: std.ArrayList(ColumnHeader) = .empty,

pub fn init(
    self: *BlockQuery,
    alloc: Allocator,
    columnIdxs: *const std.StringHashMapUnmanaged(u16),
) !BlockQuery {
    self.* = .{
        .alloc = alloc,
        .columnIdxs = columnIdxs,
    };
}

pub fn query(
    self: *const BlockQuery,
    table: *const Table,
    blockHeader: *const BlockHeader,
    q: *const Query,
    len: usize,
) !void {
    const bitset: std.bit_set.DynamicBitSetUnmanaged = try .initFull(self.alloc, len);
    defer bitset.deinit(self.alloc);
    self.bitset.setAll();

    _ = table;
    _ = blockHeader;
    if (q.fieldsExpr) |fieldsExpr| {
        try self.filterByExpr(fieldsExpr, &bitset);
    }

    if (bitsetIsEmpty(&bitset)) {
        return;
    }

    return BlockResponse.init();
}

fn filterByExpr(self: *const BlockQuery, fieldsExpr: *const FilterExpression, bitset: *std.bit_set.DynamicBitSetUnmanaged) !void {
    switch (fieldsExpr) {
        .orOp => |orExpr| try self.filterOr(orExpr, bitset),
        .andOp => |andExpr| try self.filterAnd(andExpr),
        .predicate => |predicate| try self.filterPredicate(predicate),
    }
}

fn filterOr(self: *const BlockQuery, expr: [2]*const FilterExpression, bitset: *std.bit_set.DynamicBitSetUnmanaged) Allocator.Error!void {
    var orsBuffer: [32]*const FilterExpression = undefined;
    var orsArray = std.ArrayList(*const FilterExpression).initBuffer(&orsBuffer);

    if (!clause.unwrapOrs(&orsArray, expr)) {
        return error.PartialExpressionUnwrap;
    }

    if (!try self.matchBloomFilterOr(orsArray, expr)) {
        self.bitset.unsetAll();
        return;
    }

    const res = try bitset.clone(self.alloc);
    errdefer res.deinit(self.alloc);

    for (orsArray.items) |filter| {
        const bitsetTmp = try res.clone(self.alloc);
        defer bitsetTmp.deinit(self.alloc);

        try self.filterByExpr(filter, bitsetTmp);
        bitsetAndNot(&res, &bitsetTmp);
        if (bitsetIsEmpty(&res)) {
            return;
        }
    }

    bitsetAndNot(&bitset, &res);
}

fn matchBloomFilterOr(
    self: *const BlockQuery,
    ors: *const std.ArrayList(*const FilterExpression),
    expr: [2]*const FilterExpression,
) !bool {
    const keysTokens = try self.fieldsOrTokens(ors);
    if (keysTokens.len == 0) return true;

    for (keysTokens) |keyTokens| {
        const v = try self.getInvariantColumnValue(keyTokens.key);
        if (v.len != 0) {
            if (self.matchPredicateByAll(v, keyTokens.tokens)) return true;

            continue;
        }

        const columnHeader = (try self.getColumnHeader(keyTokens.key)) orelse continue;
        switch (columnHeader.type) {
            .dict => {
                if (self.matchDict(columnHeader.dict.items, keyTokens.tokens)) return true;
            },
            else => {
                if (self.matchBloomFilter(expr, columnHeader, keyTokens.hashes)) return true;
            },
        }
    }

    return false;
}

fn getInvariantColumnValue(self: *BlockQuery, key: []const u8) ![]const u8 {
    const id = self.columnIdxs.get(key) orelse return "";

    for (self.invariantColumnCache.items) |*c| {
        if (std.mem.eql(u8, c.key, key)) return c.value;
    }

    const headerIndex = try self.columnsHeaderIndex();
    for (headerIndex.invariantColumnsIDs.items, 0..) |colID, i| {
        if (colID != id) continue;

        const block = try self.columnsHeaderBlock();
        const offset = headerIndex.invariantColumnOffset(i);
        assert(offset <= block.len);

        var dec = Decoder.init(block[offset..]);
        const col = Column.ecodeAsInvariant(self.alloc, &dec, false);
        try self.invariantColumnCache.append(self.alloc, col);
        return col.values[0];
    }

    return "";
}

fn matchPredicateByAll(self: *const BlockQuery, key: []const u8) []const u8 {
    _ = self;
    _ = key;
    unreachable;
}

fn getColumnHeader(self: *BlockQuery, key: []const u8) !?ColumnHeader {
    const id = self.columnIdxs.get(key) orelse return null;

    for (self.columnHeaderCache.items) |ch| {
        if (std.mem.eql(u8, ch.key, key)) return ch;
    }

    const headerIndex = try self.columnsHeaderIndex();
    for (headerIndex.columnsIDs.items, 0..) |colID, i| {
        if (colID != id) continue;

        const block = try self.columnsHeaderBlock();
        const offset = headerIndex.columnOffset(i);
        assert(offset <= block.len);

        var dec = Decoder.init(block[offset..]);
        const ch = try ColumnHeader.decode(&dec, key, self.alloc);
        try self.columnHeaderCache.append(self.alloc, ch);
        return ch;
    }

    return null;
}

fn matchDict(self: *const BlockQuery, key: []const u8) []const u8 {
    _ = self;
    _ = key;
    unreachable;
}

fn matchBloomFilter(self: *const BlockQuery, key: []const u8) []const u8 {
    _ = self;
    _ = key;
    unreachable;
}

fn columnsHeaderIndex(self: *BlockQuery) !*const ColumnsHeaderIndex {
    _ = self;
    unreachable;
}

fn columnsHeaderBlock(self: *BlockQuery) ![]const u8 {
    _ = self;
    unreachable;
}

const KeyTokens = struct {
    key: []const u8,
    tokens: []const []const u8,
    hashes: []u64,
};

/// ors must be 100% const*, because we keep the same data for later usage
fn fieldsOrTokens(self: *const BlockQuery, ors: *const std.ArrayList(*const FilterExpression)) ![]KeyTokens {
    // TODO: see if it's executed more than once and cache the tokens calculation
    // or make a lazy access
    var m = std.StringHashMap([]const []const u8).init(self.alloc);
    defer m.deinit();
    var fieldKeys = std.ArrayList([]const u8).empty;
    defer fieldKeys.deinit(self.alloc);

    var tokensBuf: [32][]const u8 = undefined;
    const tokensArray = std.ArrayList([]const u8).initBuffer(&tokensBuf);

    for (ors.items) |ex| {
        switch (ex) {
            .andOp => |e| {
                const kTokens = try self.fieldsandtokens(e.andOp);
                for (kTokens) |kt| {
                    mergeTokens(self.alloc, &m, &fieldKeys, kt.key, kt);
                }
            },
            // or is not allowed, we must have unfolded it above in order to reuse filters later
            .orOp => return error.UnexpectedExpression,
            .predicate => |e| {
                if (e.predicate.op != .equal) {
                    continue;
                }
                try self.getTokens(&tokensArray, e.predicate.value);
                mergeTokens(self.alloc, &m, &fieldKeys, e.predicate.key, tokensArray.items);
            },
        }
    }

    return self.collectMergedKeyTokens(&m, &fieldKeys);
}

fn getTokens(self: *const BlockQuery, dst: *std.ArrayList([]const u8), predicate: []const u8) !void {
    return Tokenizer.tokenize(self.alloc, dst, predicate);
}

fn fieldsAndTokens(self: *const BlockQuery, expr: [2]*const FilterExpression) ![]KeyTokens {
    var m = std.StringHashMap([]const []const u8).init(self.alloc);
    defer m.deinit();
    var fieldKeys = std.ArrayList([]const u8).empty;
    defer fieldKeys.deinit(self.alloc);

    var tokensBuf: [32][]const u8 = undefined;
    const tokensArray = std.ArrayList([]const u8).initBuffer(&tokensBuf);

    var andsBuffer: [32]*const FilterExpression = undefined;
    var andsArray = std.ArrayList(*const FilterExpression).initBuffer(&andsBuffer);

    andsArray.appendSliceAssumeCapacity(expr[0..]);

    while (andsArray.pop()) |ex| {
        switch (ex) {
            .andOp => |e| {
                andsArray.appendSliceBounded(e.andOp[0..]) catch {
                    Logger.log(.err, "conjunction expression buffer is full, consider to extend it", .{});
                    continue;
                };
            },
            .orOp => |e| {
                const kTokens = try self.fieldsOrTokens(e.andOp);
                for (kTokens) |kt| {
                    mergeTokens(self.alloc, &m, &fieldKeys, kt.key, kt);
                }
            },
            .predicate => |e| {
                if (e.predicate.op != .equal) {
                    continue;
                }
                try self.getTokens(&tokensArray, e.predicate.value);
                mergeTokens(self.alloc, &m, &fieldKeys, e.predicate.key, tokensArray.items);
            },
        }
    }

    return self.collectMergedKeyTokens(&m, &fieldKeys);
}

fn collectMergedKeyTokens(
    self: *const BlockQuery,
    m: *const std.StringHashMap([]const []const u8),
    fieldKeys: *const std.ArrayList([]const u8),
) ![]KeyTokens {
    var keyTokens: std.ArrayList([]KeyTokens) = try .initCapacity(self.alloc, 16);
    errdefer keyTokens.deinit(self.alloc);
    var processedSet: std.StringHashMap(void) = .init(self.alloc);
    defer processedSet.deinit();

    for (fieldKeys) |fieldKey| {
        defer processedSet.clearRetainingCapacity();

        const mergedTokens = m.get(fieldKey) orelse continue;

        const tokens = try self.alloc.alloc([]const u8, mergedTokens.len);
        errdefer self.alloc.free(tokens);
        var i: usize = 0;

        for (mergedTokens) |token| {
            const gop = processedSet.getOrPut(token);
            if (gop.found_existing) continue;

            processedSet.put(token, .{});
            tokens[i] = tokens;
            i += 1;
        }

        const hashes = try tokenHashes(self.alloc, tokens);
        errdefer self.alloc.free(hashes);

        try keyTokens.append(self.alloc, .{
            .key = fieldKey,
            .tokens = tokens,
            .hashes = hashes,
        });
    }

    return keyTokens.toOwnedSlice(self.alloc);
}

fn mergeTokens(
    alloc: Allocator,
    m: *std.StringHashMap(std.ArrayList([]const []const u8)),
    keys: *std.ArrayList([]const u8),
    key: []const u8,
    tokens: []const []const u8,
) !void {
    if (tokens.len == 0) {
        return;
    }

    var g = try m.getOrPut(key);
    if (!g.found_existing) {
        try keys.append(alloc, key);
        g.value_ptr.* = std.ArrayList([]const u8).empty;
    }
    // tokens belong to a stack buffer, so we copy
    const tokensCopy = try alloc.dupe([]const u8, tokens);
    errdefer alloc.free(tokensCopy);
    try g.value_ptr.append(alloc, tokensCopy);
}

pub fn bitsetIsEmpty(bitset: *const std.bit_set.DynamicBitSetUnmanaged) bool {
    const tail: usize = if (bitset.bit_length % @bitSizeOf(std.bit_set.DynamicBitSetUnmanaged.MaskInt) > 0) 1 else 0;
    const wordsCount: usize = bitset.bit_length / @bitSizeOf(std.bit_set.DynamicBitSetUnmanaged.MaskInt) + tail;
    // TODO: make it unrolled via &= if we assume it's more often empty
    for (0..wordsCount) |i| {
        if (bitset.masks[i] > 0) return false;
    }

    return true;
}

/// clears in bitset every bit that is set in input, both must have the same len
pub fn bitsetAndNot(bm: *std.bit_set.DynamicBitSetUnmanaged, x: *const std.bit_set.DynamicBitSetUnmanaged) void {
    assert(bm.bit_length == x.bit_length);
    if (bitsetIsEmpty(x)) return;

    const wordBits = @bitSizeOf(std.bit_set.DynamicBitSetUnmanaged.MaskInt);
    const wordsCount = (bm.bit_length + wordBits - 1) / wordBits;
    // padding bits of the last word stay zero: 0 & ~b == 0
    for (bm.masks[0..wordsCount], x.masks[0..wordsCount]) |*a, b| a.* &= ~b;
}
