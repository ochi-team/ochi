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

const Logger = @import("logging");
const Decoder = @import("encoding").Decoder;
const Encoder = @import("encoding").Encoder;
const parseIPv4 = @import("../data/ValuesEncoder.zig").parseIPv4;
const parseTimestampISO8601 = @import("../../stds/time.zig").parseTimestampISO8601;

const BlockResponse = @import("BlockResponse.zig");
const clause = @import("clause.zig");

const ColumnsHeaderIndex = @import("../../store/data/ColumnsHeaderIndex.zig");
const Column = @import("../../store/data/Column.zig");
const ColumnHeader = @import("../../store/data/ColumnHeader.zig");
const bloom = @import("../bloom/bloom.zig");
const BloomFilter = @import("../bloom/BloomFilter.zig");
const matchPredicateByAll = @import("match.zig").matchPredicateByAll;

const Block = @import("../data/Block.zig");
const Unpacker = @import("../data/Unpacker.zig").Unpacker(false);
const DecompressionPool = @import("../compression/DecompressionPool.zig");
const BlockData = @import("../../store/data/BlockData.zig");
const maxBloomTokensBlockSize = BlockData.maxBloomTokensBlockSize;
const maxColumnsHeaderIndexSize = BlockData.maxColumnsHeaderIndexSize;
const maxColumnsHeaderSize = BlockData.maxColumnsHeaderSize;

fn ErrorSetOf(comptime f: anytype) type {
    return @typeInfo(@typeInfo(@TypeOf(f)).@"fn".return_type.?).error_union.error_set;
}

pub const Error = Allocator.Error ||
    Io.File.ReadPositionalError ||
    ErrorSetOf(Unpacker.unpackValues) ||
    error{
        PartialExpressionUnwrap,
        UnexpectedExpression,
        UnsupportedMatchOperation,
        UnknownValueType,
        ColumnShardNotFound,
    };

const BlockQuery = @This();

io: Io,
alloc: Allocator,
table: *const Table,
blockHeader: *const BlockHeader,
unpacker: Unpacker,

// query caches, keyed by the query expressions, they survive reset
fieldTokensCache: std.AutoHashMapUnmanaged(*const FilterExpression, []KeyTokens) = .empty,
exactHashesCache: std.AutoHashMapUnmanaged(*const FilterExpression, []u64) = .empty,

// block caches, initialized empty
invariantColumnCache: std.ArrayList(Column) = .empty,
columnHeaderCache: std.ArrayList(ColumnHeader) = .empty,
cshIndex: ?ColumnsHeaderIndex = null,
cshIndexIDs: ?*[Block.maxColumns]u16 = null,
cshIndexOffsets: ?*[Block.maxColumns]u32 = null,
cshBlock: ?[]u8 = null,
// encoded column values, owned by unpacker
valuesCache: std.ArrayList(Column) = .empty,

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
    self.releaseBlockCaches();
    self.invariantColumnCache.deinit(self.alloc);
    self.columnHeaderCache.deinit(self.alloc);
    self.valuesCache.deinit(self.alloc);
    self.unpacker.deinit(self.alloc);
    if (self.cshIndexIDs) |ids| self.alloc.destroy(ids);
    if (self.cshIndexOffsets) |offsets| self.alloc.destroy(offsets);

    var tokensIt = self.fieldTokensCache.valueIterator();
    while (tokensIt.next()) |kt| self.freeKeysTokens(kt.*);
    self.fieldTokensCache.deinit(self.alloc);
    var hashesIt = self.exactHashesCache.valueIterator();
    while (hashesIt.next()) |h| self.alloc.free(h.*);
    self.exactHashesCache.deinit(self.alloc);

    self.* = undefined;
}

fn releaseBlockCaches(self: *BlockQuery) void {
    for (self.invariantColumnCache.items) |c| self.alloc.free(c.values);
    self.invariantColumnCache.clearRetainingCapacity();

    for (self.columnHeaderCache.items) |*ch| ch.dict.deinit(self.alloc);
    self.columnHeaderCache.clearRetainingCapacity();

    // ids and offsets buffers are reused by the next block
    self.cshIndex = null;
    if (self.cshBlock) |b| self.alloc.free(b);
    self.cshBlock = null;

    for (self.valuesCache.items) |c| self.alloc.free(c.values);
    self.valuesCache.clearRetainingCapacity();
    const pool = self.unpacker.compressionPool;
    self.unpacker.deinit(self.alloc);
    self.unpacker = .init(pool);
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
        .orOp => try self.filterOr(fieldsExpr, bitset),
        .andOp => try self.filterAnd(fieldsExpr, bitset),
        .predicate => try self.filterPredicate(fieldsExpr, bitset),
    }
}

fn filterPredicate(self: *BlockQuery, expr: *const FilterExpression, bitset: *std.bit_set.DynamicBitSetUnmanaged) Error!void {
    switch (expr.predicate.op) {
        .equal => try self.filterExact(expr, bitset),
        // applies the exact filter to a copy and excludes the matched rows
        .notEqual => {
            var matched = try bitset.clone(self.alloc);
            defer matched.deinit(self.alloc);
            try self.filterExact(expr, &matched);
            bitsetAndNot(bitset, &matched);
        },
        // TODO: support regex and range operations
        else => return error.UnsupportedMatchOperation,
    }
}

fn filterExact(self: *BlockQuery, expr: *const FilterExpression, bitset: *std.bit_set.DynamicBitSetUnmanaged) Error!void {
    const value = expr.predicate.value;

    const v = try self.getInvariantColumnValue(expr.predicate.key);
    if (v.len != 0) {
        if (!std.mem.eql(u8, v, value)) bitset.unsetAll();
        return;
    }

    const ch = (try self.getColumnHeader(expr.predicate.key)) orelse {
        // a missing column matches only to an empty value
        if (value.len != 0) bitset.unsetAll();
        return;
    };

    switch (ch.type) {
        .string => try self.matchBinaryValue(&ch, expr, bitset, value),
        .dict => try self.matchDictByExactValue(&ch, bitset, value),
        .uint8 => try self.matchUintByExactValue(u8, &ch, expr, bitset),
        .uint16 => try self.matchUintByExactValue(u16, &ch, expr, bitset),
        .uint32 => try self.matchUintByExactValue(u32, &ch, expr, bitset),
        .uint64 => try self.matchUintByExactValue(u64, &ch, expr, bitset),
        .int64 => {
            const n = std.fmt.parseInt(i64, value, 10) catch return bitset.unsetAll();
            const min: i64 = @bitCast(ch.min);
            const max: i64 = @bitCast(ch.max);
            if (n < min or n > max) return bitset.unsetAll();
            try self.matchBinaryValue(&ch, expr, bitset, &Encoder.toBytes(i64, n));
        },
        .float64 => {
            const f = std.fmt.parseFloat(f64, value) catch return bitset.unsetAll();
            const min: f64 = @bitCast(ch.min);
            const max: f64 = @bitCast(ch.max);
            if (f < min or f > max) return bitset.unsetAll();
            try self.matchBinaryValue(&ch, expr, bitset, &Encoder.toBytes(u64, @bitCast(f)));
        },
        .ipv4 => {
            const n = parseIPv4(value) catch return bitset.unsetAll();
            if (n < ch.min or n > ch.max) return bitset.unsetAll();
            try self.matchBinaryValue(&ch, expr, bitset, &Encoder.toBytes(u32, n));
        },
        .timestampIso8601 => {
            const n = parseTimestampISO8601(value) orelse return bitset.unsetAll();
            const min: i64 = @bitCast(ch.min);
            const max: i64 = @bitCast(ch.max);
            if (n < min or n > max) return bitset.unsetAll();
            try self.matchBinaryValue(&ch, expr, bitset, &Encoder.toBytes(i64, n));
        },
        .unknown => return error.UnknownValueType,
    }
}

fn matchUintByExactValue(self: *BlockQuery, comptime T: type, ch: *const ColumnHeader, expr: *const FilterExpression, bitset: *std.bit_set.DynamicBitSetUnmanaged) Error!void {
    const n = std.fmt.parseInt(u64, expr.predicate.value, 10) catch return bitset.unsetAll();
    if (n < ch.min or n > ch.max) return bitset.unsetAll();
    // fits T, since max does
    try self.matchBinaryValue(ch, expr, bitset, &Encoder.toBytes(T, @intCast(n)));
}

fn matchDictByExactValue(self: *BlockQuery, ch: *const ColumnHeader, bitset: *std.bit_set.DynamicBitSetUnmanaged, value: []const u8) Error!void {
    var matches: [256]bool = @splat(false);
    for (ch.dict.values.items, 0..) |dv, i| matches[i] = std.mem.eql(u8, dv, value);

    // dict values are encoded as 1 byte ids
    const ids = try self.getColumnValues(ch);
    var it = bitset.iterator(.{});
    while (it.next()) |i| {
        if (!matches[ids[i][0]]) bitset.unset(i);
    }
}

/// compares the encoded column values to the encoded predicate value
fn matchBinaryValue(self: *BlockQuery, ch: *const ColumnHeader, expr: *const FilterExpression, bitset: *std.bit_set.DynamicBitSetUnmanaged, binValue: []const u8) Error!void {
    if (!try self.matchBloomFilter(ch, try self.getExactHashes(expr))) return bitset.unsetAll();

    const values = try self.getColumnValues(ch);
    var it = bitset.iterator(.{});
    while (it.next()) |i| {
        if (!std.mem.eql(u8, values[i], binValue)) bitset.unset(i);
    }
}

/// bloom hashes of the predicate value tokens, cached for the query lifetime
fn getExactHashes(self: *BlockQuery, expr: *const FilterExpression) Error![]const u64 {
    if (self.exactHashesCache.get(expr)) |h| return h;

    var tokensBuf: [32][]const u8 = undefined;
    var tokens = std.ArrayList([]const u8).initBuffer(&tokensBuf);
    try self.getTokens(&tokens, expr.predicate.value);

    const hashes = try self.alloc.alloc(u64, tokens.items.len * bloom.hashRounds);
    errdefer self.alloc.free(hashes);
    for (tokens.items, 0..) |token, i| {
        const h = try tokenHashes(self.alloc, token);
        defer self.alloc.free(h);
        @memcpy(hashes[i * bloom.hashRounds ..][0..bloom.hashRounds], h);
    }

    try self.exactHashesCache.put(self.alloc, expr, hashes);
    return hashes;
}

/// returns the encoded column values: strings as is, dict ids, big endian numbers
fn getColumnValues(self: *BlockQuery, ch: *const ColumnHeader) Error![]const []const u8 {
    for (self.valuesCache.items) |c| {
        if (std.mem.eql(u8, c.key, ch.key)) return c.values;
    }

    assert(ch.size <= BlockData.maxValuesBlockSize);
    const buf = try self.alloc.alloc(u8, ch.size);
    defer self.alloc.free(buf);

    const n = if (ch.key.len == 0)
        try self.table.readMessageBloomValues(self.io, buf, ch.offset)
    else blk: {
        const shardIdx = self.table.columnIdxs.get(ch.key) orelse {
            Logger.log(.err, "column shard not found", .{ .key = ch.key, .table = self.table.path });
            return error.ColumnShardNotFound;
        };
        break :blk try self.table.readBloomValues(self.io, buf, ch.offset, shardIdx);
    };
    assert(n == buf.len);

    const values = try self.unpacker.unpackValues(self.io, self.alloc, buf, self.blockHeader.len);
    errdefer self.alloc.free(values);
    try self.valuesCache.append(self.alloc, .{ .key = ch.key, .values = values });
    return values;
}

fn filterOr(self: *BlockQuery, expr: *const FilterExpression, bitset: *std.bit_set.DynamicBitSetUnmanaged) Error!void {
    var orsBuffer: [32]*const FilterExpression = undefined;
    var orsArray = std.ArrayList(*const FilterExpression).initBuffer(&orsBuffer);

    if (!clause.unwrapOrs(&orsArray, expr.orOp)) {
        return error.PartialExpressionUnwrap;
    }

    if (!try self.matchBloomFilterOr(expr)) {
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

fn matchBloomFilterOr(self: *BlockQuery, expr: *const FilterExpression) Error!bool {
    const keysTokens = try self.getFieldTokens(expr);
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

fn filterAnd(self: *BlockQuery, expr: *const FilterExpression, bitset: *std.bit_set.DynamicBitSetUnmanaged) Error!void {
    var andsBuffer: [32]*const FilterExpression = undefined;
    var andsArray = std.ArrayList(*const FilterExpression).initBuffer(&andsBuffer);

    if (!clause.unwrapAnds(&andsArray, expr.andOp)) {
        return error.PartialExpressionUnwrap;
    }

    if (!try self.matchBloomFilterAnd(expr)) {
        bitset.unsetAll();
        return;
    }

    for (andsArray.items) |filter| {
        try self.filterByExpr(filter, bitset);

        if (bitsetIsEmpty(bitset)) return;
    }
}

fn matchBloomFilterAnd(self: *BlockQuery, expr: *const FilterExpression) Error!bool {
    const keysTokens = try self.getFieldTokens(expr);
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

    const n = if (header.key.len == 0)
        try self.table.readMessageBloomTokens(self.io, buf, header.bloomFilterOffset)
    else blk: {
        const shardIdx = self.table.columnIdxs.get(header.key) orelse {
            Logger.log(.err, "column shard not found", .{ .key = header.key, .table = self.table.path });
            return false;
        };
        break :blk try self.table.readBloomTokens(self.io, buf, header.bloomFilterOffset, shardIdx);
    };
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

    if (self.cshIndexIDs == null) self.cshIndexIDs = try self.alloc.create([Block.maxColumns]u16);
    if (self.cshIndexOffsets == null) self.cshIndexOffsets = try self.alloc.create([Block.maxColumns]u32);

    self.cshIndex = ColumnsHeaderIndex.initBufferUnknown(self.cshIndexIDs.?, self.cshIndexOffsets.?);
    self.cshIndex.?.decode(buf);
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

/// common tokens per field of the expression, cached for the query lifetime,
/// so the expression must be 100% const*
fn getFieldTokens(self: *BlockQuery, expr: *const FilterExpression) Error![]const KeyTokens {
    if (self.fieldTokensCache.get(expr)) |kt| return kt;

    var buffer: [32]*const FilterExpression = undefined;
    var list = std.ArrayList(*const FilterExpression).initBuffer(&buffer);
    const keysTokens = switch (expr.*) {
        .predicate => blk: {
            list.appendAssumeCapacity(expr);
            break :blk try self.fieldsAndTokens(list);
        },
        .andOp => |e| blk: {
            if (!clause.unwrapAnds(&list, e)) return error.PartialExpressionUnwrap;
            break :blk try self.fieldsAndTokens(list);
        },
        .orOp => |e| blk: {
            if (!clause.unwrapOrs(&list, e)) return error.PartialExpressionUnwrap;
            break :blk try self.fieldsOrTokens(list);
        },
    };
    errdefer self.freeKeysTokens(keysTokens);

    try self.fieldTokensCache.put(self.alloc, expr, keysTokens);
    return keysTokens;
}

fn fieldsOrTokens(self: *BlockQuery, ors: std.ArrayList(*const FilterExpression)) Error![]KeyTokens {
    var m = TokensMap.init(self.alloc);
    defer deinitTokensMap(self.alloc, &m);
    var fieldKeys = std.ArrayList([]const u8).empty;
    defer fieldKeys.deinit(self.alloc);

    for (ors.items, 0..) |ex, i| {
        // or is not allowed, we must have unfolded it above in order to reuse filters later
        if (ex.* == .orOp) return error.UnexpectedExpression;
        const branch = try self.getFieldTokens(ex);

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
    return Tokenizer.tokenize(self.alloc, dst, predicate);
}

fn fieldsAndTokens(
    self: *BlockQuery,
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
            .orOp => {
                for (try self.getFieldTokens(ex)) |kt| {
                    try mergeTokens(self.alloc, &m, &fieldKeys, kt.key, kt.tokens);
                }
            },
            .predicate => |p| {
                if (p.op != .equal) {
                    continue;
                }
                tokensArray.clearRetainingCapacity();
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
        var hashes: std.ArrayList(u64) = try .initCapacity(self.alloc, mergedTokens.items.len * bloom.hashRounds);
        errdefer hashes.deinit(self.alloc);

        for (mergedTokens.items) |token| {
            const gop = try processedSet.getOrPut(token);
            if (gop.found_existing) continue;

            tokens.appendAssumeCapacity(token);
            const h = try tokenHashes(self.alloc, token);
            defer self.alloc.free(h);
            hashes.appendSliceAssumeCapacity(h);
        }

        const tokensSlice = try tokens.toOwnedSlice(self.alloc);
        errdefer self.alloc.free(tokensSlice);
        const hashesSlice = try hashes.toOwnedSlice(self.alloc);
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
