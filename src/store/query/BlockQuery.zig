const std = @import("std");
const Allocator = std.mem.Allocator;
const Io = std.Io;
const assert = std.debug.assert;

const Table = @import("../data/Table.zig");
const BlockHeader = @import("../data/BlockHeader.zig");
const Query = @import("../../query/Query.zig");
const FilterExpression = Query.FilterExpression;

const Tokenizer = @import("../bloom/Tokenizer.zig");
const tokenHashes = @import("../bloom/bloom.zig").tokenHashes;
const Unpacker = @import("../data/Unpacker.zig").Unpacker(false);
const DecompressionPool = @import("../compression/DecompressionPool.zig");

const encoding = @import("encoding");
const Decoder = encoding.Decoder;

const BlockResponse = @import("BlockResponse.zig");
const clause = @import("clause.zig");

const ColumnsHeaderIndex = @import("../../store/data/ColumnsHeaderIndex.zig");
const Column = @import("../../store/data/Column.zig");
const ColumnHeader = @import("../../store/data/ColumnHeader.zig");
const bloom = @import("../bloom/bloom.zig");
const BloomFilter = @import("../bloom/BloomFilter.zig");
const matchPredicateByAll = @import("match.zig").matchPredicateByAll;

const Block = @import("../data/Block.zig");
const BlockData = @import("../../store/data/BlockData.zig");
const maxBloomTokensBlockSize = BlockData.maxBloomTokensBlockSize;
const maxValuesBlockSize = BlockData.maxValuesBlockSize;
const maxColumnsHeaderIndexSize = BlockData.maxColumnsHeaderIndexSize;
const maxColumnsHeaderSize = BlockData.maxColumnsHeaderSize;
const UnpackError = @import("../data/Unpacker.zig").UnpackError;
const ColumnDict = @import("../data/ColumnDict.zig");
const ValuesEncoder = @import("../data/ValuesEncoder.zig");

pub const Error = Allocator.Error || Io.File.ReadPositionalError || UnpackError || encoding.Error || error{
    PartialExpressionUnwrap,
    UnexpectedExpression,
    PredicateOperationNotSupported,
    UnsupportedValueType,
    TokensBufTooSmall,
    ColumnShardNotFound,
};

const BlockQuery = @This();

io: Io,
alloc: Allocator,
table: *const Table,
blockHeader: *const BlockHeader,

// caches, initialized empty
invariantColumnCache: std.ArrayList(Column) = .empty,
columnHeaderCache: std.ArrayList(ColumnHeader) = .empty,
columnValuesCache: std.ArrayList(Column) = .empty,
cshIndex: ?ColumnsHeaderIndex = null,
cshIndexIDs: ?*[Block.maxColumns]u16 = null,
cshIndexOffsets: ?*[Block.maxColumns]u32 = null,
cshBlock: ?[]u8 = null,
unpacker: Unpacker,

pub fn init(
    io: Io,
    alloc: Allocator,
    table: *const Table,
    blockHeader: *const BlockHeader,
    decompressionPool: *DecompressionPool,
) BlockQuery {
    return .{
        .io = io,
        .alloc = alloc,
        .table = table,
        .blockHeader = blockHeader,
        .unpacker = .init(decompressionPool),
    };
}

pub fn deinit(self: *BlockQuery) void {
    for (self.invariantColumnCache.items) |c| self.alloc.free(c.values);
    self.invariantColumnCache.deinit(self.alloc);

    for (self.columnValuesCache.items) |c| self.alloc.free(c.values);
    self.columnValuesCache.deinit(self.alloc);

    for (self.columnHeaderCache.items) |*ch| ch.dict.deinit(self.alloc);
    self.columnHeaderCache.deinit(self.alloc);

    self.unpacker.deinit(self.alloc);
    if (self.cshIndexIDs) |ids| self.alloc.destroy(ids);
    if (self.cshIndexOffsets) |offsets| self.alloc.destroy(offsets);
    if (self.cshBlock) |b| self.alloc.free(b);
    self.* = undefined;
}

pub fn query(
    self: *BlockQuery,
    q: *const Query,
) Error!?BlockResponse {
    var bitset: std.bit_set.DynamicBitSetUnmanaged = try .initFull(self.alloc, self.blockHeader.len);
    defer bitset.deinit(self.alloc);

    if (q.fieldsExpr) |fieldsExpr| {
        try self.filterByExpr(fieldsExpr, &bitset);
    }

    if (bitsetIsEmpty(&bitset)) {
        return null;
    }
    return BlockResponse.init();
}

fn filterByExpr(self: *BlockQuery, fieldsExpr: *const FilterExpression, bitset: *std.bit_set.DynamicBitSetUnmanaged) Error!void {
    switch (fieldsExpr.*) {
        .orOp => |orExpr| try self.filterOr(orExpr, bitset),
        .andOp => |andExpr| try self.filterAnd(andExpr, bitset),
        .predicate => |predicate| try self.filterPredicate(predicate, bitset),
    }
}

fn filterPredicate(self: *BlockQuery, predicate: Query.FilterPredicate, bitset: *std.bit_set.DynamicBitSetUnmanaged) Error!void {
    switch (predicate.op) {
        .equal => try self.filterEqual(predicate.key, predicate.value, bitset),
        else => return error.PredicateOperationNotSupported,
    }
}

fn filterEqual(self: *BlockQuery, key: []const u8, value: []const u8, bitset: *std.bit_set.DynamicBitSetUnmanaged) Error!void {
    const v = try self.getInvariantColumnValue(key);
    if (v.len != 0) {
        if (!std.mem.eql(u8, value, v)) bitset.unsetAll();
        return;
    }

    const ch = (try self.getColumnHeader(key)) orelse {
        // missing column matches only empty value
        if (value.len != 0) bitset.unsetAll();
        return;
    };

    const hashes = try self.equalTokensHashes(value);
    defer self.alloc.free(hashes);

    // TODO: all the equation matching is build around decoding the column values,
    // it's very heavy compute wise,
    // what if we could encode a query once and compare it?
    // could work for equation/not, but wouldn't for regex/partial matching
    switch (ch.type) {
        .string => try self.matchStringEqual(&ch, bitset, value, hashes),
        .dict => try self.matchDictEqual(&ch, bitset, value),
        .uint8, .uint16, .uint32, .uint64 => try self.matchUintEqual(&ch, bitset, value, hashes),
        else => return error.UnsupportedValueType,
    }
}

fn matchStringEqual(
    self: *BlockQuery,
    ch: *const ColumnHeader,
    bitset: *std.bit_set.DynamicBitSetUnmanaged,
    value: []const u8,
    hashes: []u64,
) Error!void {
    if (!try self.matchBloomFilter(ch, hashes)) {
        bitset.unsetAll();
        return;
    }
    try self.setMatchedBits(ch, bitset, value);
}

fn matchUintEqual(
    self: *BlockQuery,
    ch: *const ColumnHeader,
    bitset: *std.bit_set.DynamicBitSetUnmanaged,
    value: []const u8,
    hashes: []u64,
) Error!void {
    const num = std.fmt.parseInt(u64, value, 10) catch {
        bitset.unsetAll();
        return;
    };
    if (num < ch.min or num > ch.max) {
        bitset.unsetAll();
        return;
    }

    var binBuf: [8]u8 = undefined;
    const n = ValuesEncoder.encodeUint(ch.type, num, &binBuf);
    const binValue = binBuf[0..n];

    if (!try self.matchBloomFilter(ch, hashes)) {
        bitset.unsetAll();
        return;
    }
    try self.setMatchedBits(ch, bitset, binValue);
}

fn matchDictEqual(
    self: *BlockQuery,
    ch: *const ColumnHeader,
    bitset: *std.bit_set.DynamicBitSetUnmanaged,
    value: []const u8,
) Error!void {
    var matchBuf: [ColumnDict.maxDictColumnValuesLen]u8 = undefined;
    const dictVals = ch.dict.values.items;
    assert(dictVals.len <= matchBuf.len);

    var anyMatch = false;
    for (dictVals, 0..) |dv, i| {
        matchBuf[i] = if (std.mem.eql(u8, dv, value)) 1 else 0;
        if (matchBuf[i] == 1) anyMatch = true;
    }
    if (!anyMatch) {
        bitset.unsetAll();
        return;
    }

    if (bitsetIsEmpty(bitset)) return;

    const values = try self.getColumnValues(ch);
    // TODO: a few repeatable patterns in match strings and dict
    // give a hint to add a bitset iterator ir something
    for (0..bitset.bit_length) |idx| {
        if (!bitset.isSet(idx)) continue;
        const encoded = values[idx];
        assert(encoded.len == ColumnDict.encodedValueSize);
        const dictIdx = encoded[0];
        if (matchBuf[dictIdx] != 1) bitset.unset(idx);
    }
}

fn setMatchedBits(
    self: *BlockQuery,
    ch: *const ColumnHeader,
    bitset: *std.bit_set.DynamicBitSetUnmanaged,
    value: []const u8,
) Error!void {
    if (bitsetIsEmpty(bitset)) return;
    const values = try self.getColumnValues(ch);
    for (0..bitset.bit_length) |idx| {
        if (!bitset.isSet(idx)) continue;
        if (!std.mem.eql(u8, values[idx], value)) bitset.unset(idx);
    }
}

fn equalTokensHashes(self: *BlockQuery, value: []const u8) Error![]u64 {
    var tokensBuf: [32][]const u8 = undefined;
    var tokensArray = std.ArrayList([]const u8).initBuffer(&tokensBuf);
    try self.getTokens(&tokensArray, value);
    return tokensToHashes(self.alloc, tokensArray.items);
}

fn tokensToHashes(alloc: Allocator, tokens: []const []const u8) Error![]u64 {
    var hashes: std.ArrayList(u64) = try .initCapacity(alloc, tokens.len * bloom.hashRounds);
    errdefer hashes.deinit(alloc);
    for (tokens) |token| {
        const h = try tokenHashes(alloc, token);
        defer alloc.free(h);
        hashes.appendSliceAssumeCapacity(h);
    }
    return hashes.toOwnedSlice(alloc);
}

fn getColumnValues(self: *BlockQuery, ch: *const ColumnHeader) Error![][]const u8 {
    for (self.columnValuesCache.items) |c| {
        if (std.mem.eql(u8, c.key, ch.key)) return c.values;
    }

    assert(ch.size <= maxValuesBlockSize);
    const buf = try self.alloc.alloc(u8, ch.size);
    defer self.alloc.free(buf);

    const n = try self.table.readBloomValues(self.io, buf, ch.key, ch.offset);
    assert(n == buf.len);

    const values = try self.unpacker.unpackValues(self.io, self.alloc, buf, self.blockHeader.len);
    errdefer self.alloc.free(values);
    try self.columnValuesCache.append(self.alloc, .{ .key = ch.key, .values = values });
    return values;
}

fn filterOr(self: *BlockQuery, expr: [2]*const FilterExpression, bitset: *std.bit_set.DynamicBitSetUnmanaged) Error!void {
    var orsBuffer: [32]*const FilterExpression = undefined;
    var orsArray = std.ArrayList(*const FilterExpression).initBuffer(&orsBuffer);

    if (!clause.unwrapOrs(&orsArray, expr)) {
        return error.PartialExpressionUnwrap;
    }

    if (!try self.matchBloomFilterOr(orsArray)) {
        bitset.unsetAll();
        return;
    }

    var res = try bitset.clone(self.alloc);
    defer res.deinit(self.alloc);

    for (orsArray.items) |filter| {
        var bitsetTmp = try res.clone(self.alloc);
        defer bitsetTmp.deinit(self.alloc);

        try self.filterByExpr(filter, &bitsetTmp);
        bitsetAndNot(&res, &bitsetTmp);
        if (bitsetIsEmpty(&res)) {
            return;
        }
    }

    bitsetAndNot(bitset, &res);
}

fn matchBloomFilterOr(
    self: *BlockQuery,
    ors: std.ArrayList(*const FilterExpression),
) Error!bool {
    const keysTokens = try self.fieldsOrTokens(ors);
    defer self.freeKeysTokens(keysTokens);
    if (keysTokens.len == 0) return true;

    for (keysTokens) |keyTokens| {
        const v = try self.getInvariantColumnValue(keyTokens.key);
        if (v.len != 0) {
            if (matchPredicateByAll(v, keyTokens.tokens)) {
                return true;
            }

            continue;
        }

        const columnHeader = (try self.getColumnHeader(keyTokens.key)) orelse continue;
        switch (columnHeader.type) {
            .dict => {
                if (try matchDict(self.alloc, columnHeader.dict.values.items, keyTokens.tokens)) return true;
            },
            else => {
                if (try self.matchBloomFilter(&columnHeader, keyTokens.hashes)) return true;
            },
        }
    }

    return false;
}

fn filterAnd(self: *BlockQuery, expr: [2]*const FilterExpression, bitset: *std.bit_set.DynamicBitSetUnmanaged) Error!void {
    var andsBuffer: [32]*const FilterExpression = undefined;
    var andsArray = std.ArrayList(*const FilterExpression).initBuffer(&andsBuffer);

    if (!clause.unwrapAnds(&andsArray, expr)) {
        return error.PartialExpressionUnwrap;
    }

    if (!try self.matchBloomFilterAnd(andsArray)) {
        bitset.unsetAll();
        return;
    }

    for (andsArray.items) |filter| {
        try self.filterByExpr(filter, bitset);

        if (bitsetIsEmpty(bitset)) return;
    }
}

fn matchBloomFilterAnd(
    self: *BlockQuery,
    ands: std.ArrayList(*const FilterExpression),
) Error!bool {
    const keysTokens = try self.fieldsAndTokens(ands);
    defer self.freeKeysTokens(keysTokens);

    if (keysTokens.len == 0) return true;

    for (keysTokens) |keyTokens| {
        const v = try self.getInvariantColumnValue(keyTokens.key);
        if (v.len != 0) {
            if (matchPredicateByAll(v, keyTokens.tokens)) continue;
            return false;
        }

        const columnHeader = (try self.getColumnHeader(keyTokens.key)) orelse return false;
        switch (columnHeader.type) {
            .dict => {
                if (try matchDict(self.alloc, columnHeader.dict.values.items, keyTokens.tokens)) continue;
                return false;
            },
            else => {
                if (!try self.matchBloomFilter(&columnHeader, keyTokens.hashes)) return false;
            },
        }
    }

    return true;
}

fn getInvariantColumnValue(self: *BlockQuery, key: []const u8) Error![]const u8 {
    const id = self.table.columnIDGen.keyIDs.get(key) orelse return "";

    for (self.invariantColumnCache.items) |c| {
        if (std.mem.eql(u8, c.key, key)) return c.values[0];
    }

    const headerIndex = try self.columnsHeaderIndex();
    for (headerIndex.invariantColumnsIDs.items, 0..) |colID, i| {
        if (colID != id) continue;

        const block = try self.columnsHeaderBlock();
        const offset = headerIndex.invariantColumnOffset(i);
        assert(offset <= block.len);

        var dec = Decoder.init(block[offset..]);
        var col = try Column.decodeAsInvariant(self.alloc, &dec, false);
        errdefer self.alloc.free(col.values);
        col.key = key;
        try self.invariantColumnCache.append(self.alloc, col);
        return col.values[0];
    }

    return "";
}

fn getColumnHeader(self: *BlockQuery, key: []const u8) Error!?ColumnHeader {
    const id = self.table.columnIDGen.keyIDs.get(key) orelse return null;

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
        var ch = try ColumnHeader.decode(&dec, key, self.alloc);
        errdefer ch.dict.deinit(self.alloc);
        try self.columnHeaderCache.append(self.alloc, ch);
        return ch;
    }

    return null;
}

fn matchDict(tmpAlloc: Allocator, values: []const []const u8, tokens: []const []const u8) !bool {
    const valuesJoined = try std.mem.join(tmpAlloc, ",", values);
    defer tmpAlloc.free(valuesJoined);
    return matchPredicateByAll(valuesJoined, tokens);
}

fn matchBloomFilter(self: *const BlockQuery, header: *const ColumnHeader, hashes: []const u64) Error!bool {
    assert(header.bloomFilterSize <= maxBloomTokensBlockSize);
    if (hashes.len == 0) return true;

    const buf = try self.alloc.alloc(u8, header.bloomFilterSize);
    defer self.alloc.free(buf);

    const n = try self.table.readBloomTokens(self.io, buf, header.key, header.bloomFilterOffset);
    assert(n == buf.len);

    return BloomFilter.containsAll(buf, hashes);
}

fn columnsHeaderIndex(self: *BlockQuery) Error!*const ColumnsHeaderIndex {
    if (self.cshIndex) |*idx| return idx;

    const bh = self.blockHeader;
    assert(bh.columnsHeaderIndexSize <= maxColumnsHeaderIndexSize);

    // the buffer is only needed for decoding, ids and offsets are copied out
    const buf = try self.alloc.alloc(u8, bh.columnsHeaderIndexSize);
    defer self.alloc.free(buf);
    const n = try self.table.readColumnsHeaderIndex(self.io, buf, bh.columnsHeaderIndexOffset);
    assert(n == buf.len);

    const ids = try self.alloc.create([Block.maxColumns]u16);
    errdefer self.alloc.destroy(ids);
    const offsets = try self.alloc.create([Block.maxColumns]u32);
    errdefer self.alloc.destroy(offsets);

    self.cshIndex = ColumnsHeaderIndex.initBufferUnknown(ids, offsets);
    self.cshIndex.?.decode(buf);
    self.cshIndexIDs = ids;
    self.cshIndexOffsets = offsets;
    return &self.cshIndex.?;
}

fn columnsHeaderBlock(self: *BlockQuery) Error![]const u8 {
    if (self.cshBlock) |b| return b;

    const bh = self.blockHeader;
    assert(bh.columnsHeaderSize <= maxColumnsHeaderSize);

    const buf = try self.alloc.alloc(u8, bh.columnsHeaderSize);
    errdefer self.alloc.free(buf);
    const n = try self.table.readColumnsHeader(self.io, buf, bh.columnsHeaderOffset);
    assert(n == buf.len);

    self.cshBlock = buf;
    return buf;
}

pub const KeyTokens = struct {
    key: []const u8,
    tokens: []const []const u8,
    hashes: []u64,
};

pub fn freeKeysTokens(self: *const BlockQuery, keysTokens: []KeyTokens) void {
    for (keysTokens) |kt| {
        self.alloc.free(kt.tokens);
        self.alloc.free(kt.hashes);
    }
    self.alloc.free(keysTokens);
}

// merged tokens per field key, tokens point to the query predicates values
const TokensMap = std.StringHashMap(std.ArrayList([]const u8));

fn deinitTokensMap(alloc: Allocator, m: *TokensMap) void {
    var it = m.valueIterator();
    while (it.next()) |v| v.deinit(alloc);
    m.deinit();
}

/// ors must be 100% const*, because we keep the same data for later usage
pub fn fieldsOrTokens(self: *const BlockQuery, ors: std.ArrayList(*const FilterExpression)) Error![]KeyTokens {
    // TODO: see if it's executed more than once and cache the tokens calculation
    // or make a lazy access
    var m = TokensMap.init(self.alloc);
    defer deinitTokensMap(self.alloc, &m);
    var fieldKeys = std.ArrayList([]const u8).empty;
    defer fieldKeys.deinit(self.alloc);

    for (ors.items, 0..) |ex, i| {
        var andsBuffer: [32]*const FilterExpression = undefined;
        var andsArray = std.ArrayList(*const FilterExpression).initBuffer(&andsBuffer);
        switch (ex.*) {
            .andOp => |e| if (!clause.unwrapAnds(&andsArray, e)) return error.PartialExpressionUnwrap,
            // or is not allowed, we must have unfolded it above in order to reuse filters later
            .orOp => return error.UnexpectedExpression,
            .predicate => andsArray.appendAssumeCapacity(ex),
        }

        const branch = try self.fieldsAndTokens(andsArray);
        defer self.freeKeysTokens(branch);

        if (i == 0) {
            for (branch) |kt| {
                try mergeTokens(self.alloc, &m, &fieldKeys, kt.key, kt.tokens);
            }
            continue;
        }

        // intersect in place, empty tokens are dropped in collectMergedKeyTokens
        for (fieldKeys.items) |key| {
            const tokens = m.getPtr(key).?;
            const other = for (branch) |kt| {
                if (std.mem.eql(u8, kt.key, key)) break kt.tokens;
            } else &.{};

            var n: usize = 0;
            for (tokens.items) |t| {
                for (other) |o| {
                    if (std.mem.eql(u8, o, t)) break;
                } else continue;
                tokens.items[n] = t;
                n += 1;
            }
            tokens.shrinkRetainingCapacity(n);
        }
    }

    return self.collectMergedKeyTokens(&m, &fieldKeys);
}

fn getTokens(self: *const BlockQuery, dst: *std.ArrayList([]const u8), predicate: []const u8) !void {
    const ok = try Tokenizer.tokenize(self.alloc, dst, predicate);
    if (!ok) {
        return error.TokensBufTooSmall;
    }
}

fn fieldsAndTokens(
    self: *const BlockQuery,
    ands: std.ArrayList(*const FilterExpression),
) Error![]KeyTokens {
    var m = TokensMap.init(self.alloc);
    defer deinitTokensMap(self.alloc, &m);
    var fieldKeys = std.ArrayList([]const u8).empty;
    defer fieldKeys.deinit(self.alloc);

    var tokensBuf: [32][]const u8 = undefined;
    var tokensArray = std.ArrayList([]const u8).initBuffer(&tokensBuf);

    for (ands.items) |ex| {
        switch (ex.*) {
            // and is not allowed, we must have unfolded it above in order to reuse filters later
            .andOp => return error.UnexpectedExpression,
            .orOp => |e| {
                var orsBuffer: [32]*const FilterExpression = undefined;
                var orsArray = std.ArrayList(*const FilterExpression).initBuffer(&orsBuffer);
                if (!clause.unwrapOrs(&orsArray, e)) return error.PartialExpressionUnwrap;

                const kTokens = try self.fieldsOrTokens(orsArray);
                defer self.freeKeysTokens(kTokens);
                for (kTokens) |kt| {
                    try mergeTokens(self.alloc, &m, &fieldKeys, kt.key, kt.tokens);
                }
            },
            .predicate => |p| {
                if (p.op != .equal) {
                    continue;
                }
                try self.getTokens(&tokensArray, p.value);
                try mergeTokens(self.alloc, &m, &fieldKeys, p.key, tokensArray.items);
            },
        }
    }

    return self.collectMergedKeyTokens(&m, &fieldKeys);
}

fn collectMergedKeyTokens(
    self: *const BlockQuery,
    m: *const TokensMap,
    fieldKeys: *const std.ArrayList([]const u8),
) Error![]KeyTokens {
    var keyTokens: std.ArrayList(KeyTokens) = try .initCapacity(self.alloc, fieldKeys.items.len);
    errdefer {
        for (keyTokens.items) |kt| {
            self.alloc.free(kt.tokens);
            self.alloc.free(kt.hashes);
        }
        keyTokens.deinit(self.alloc);
    }
    var processedSet: std.StringHashMap(void) = .init(self.alloc);
    defer processedSet.deinit();

    for (fieldKeys.items) |fieldKey| {
        defer processedSet.clearRetainingCapacity();

        const mergedTokens = m.get(fieldKey) orelse continue;
        if (mergedTokens.items.len == 0) continue;

        var tokens: std.ArrayList([]const u8) = try .initCapacity(self.alloc, mergedTokens.items.len);
        errdefer tokens.deinit(self.alloc);

        for (mergedTokens.items) |token| {
            const gop = try processedSet.getOrPut(token);
            if (gop.found_existing) continue;
            tokens.appendAssumeCapacity(token);
        }

        const tokensSlice = try tokens.toOwnedSlice(self.alloc);
        errdefer self.alloc.free(tokensSlice);
        const hashesSlice = try tokensToHashes(self.alloc, tokensSlice);
        keyTokens.appendAssumeCapacity(.{
            .key = fieldKey,
            .tokens = tokensSlice,
            .hashes = hashesSlice,
        });
    }

    return keyTokens.toOwnedSlice(self.alloc);
}

fn mergeTokens(
    alloc: Allocator,
    m: *TokensMap,
    keys: *std.ArrayList([]const u8),
    key: []const u8,
    tokens: []const []const u8,
) Allocator.Error!void {
    if (tokens.len == 0) {
        return;
    }

    const g = try m.getOrPut(key);
    if (!g.found_existing) {
        g.value_ptr.* = .empty;
        try keys.append(alloc, key);
    }
    // tokens may belong to a stack buffer, so we copy
    try g.value_ptr.appendSlice(alloc, tokens);
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
