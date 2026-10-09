const std = @import("std");
const Allocator = std.mem.Allocator;

const Table = @import("../data/Table.zig");
const BlockHeader = @import("../data/BlockHeader.zig");
const Query = @import("../../query/Query.zig");
const FilterExpression = Query.FilterExpression;

const Tokenizer = @import("../bloom/Tokenizer.zig");
const tokenHashes = @import("../bloom/bloom.zig").tokenHashes;

const Logger = @import("logging");

const BlockResponse = @import("BlockResponse.zig");
const clause = @import("clause.zig");

const BlockQuery = @This();

alloc: Allocator,
bitset: std.bit_set.DynamicBitSetUnmanaged,

pub fn init(
    self: *BlockQuery,
    alloc: Allocator,
    len: usize,
) !BlockQuery {
    const bitset: std.bit_set.DynamicBitSetUnmanaged = try .initFull(alloc, len);
    self.* = .{
        .alloc = alloc,
        .bitset = bitset,
    };
    self.bitset.setAll();
}

pub fn deinit(self: *BlockQuery) void {
    self.bitset.deinit(self.alloc);
}

pub fn query(
    self: *const BlockQuery,
    table: *const Table,
    blockHeader: *const BlockHeader,
    q: *const Query,
) !void {
    _ = table;
    _ = blockHeader;
    if (q.fieldsExpr) |fieldsExpr| {
        try self.filterByExpr(fieldsExpr);
    }

    if (bitsetIsEmpty(&self.bitset)) {
        return;
    }

    return BlockResponse.init();
}

fn filterByExpr(self: *const BlockQuery, fieldsExpr: *const FilterExpression) !void {
    switch (fieldsExpr) {
        .orOp => |orExpr| try self.filterOr(orExpr),
        .andOp => |andExpr| try self.filterAnd(andExpr),
        .predicate => |predicate| try self.filterPredicate(predicate),
    }
}

fn filterOr(self: *const BlockQuery, expr: [2]*const FilterExpression) Allocator.Error!void {
    var orsBuffer: [32]*const FilterExpression = undefined;
    var orsArray = std.ArrayList(*const FilterExpression).initBuffer(&orsBuffer);

    clause.unwrapOrs(&orsArray, expr);

    if (!try self.matchBloomFilterOr(orsArray, expr)) {
        self.bitset.unsetAll();
        return;
    }

    const bitset: std.bit_set.DynamicBitSetUnmanaged = try .initFull(self.alloc, self.bitset.bit_length);
    errdefer bitset.deinit(self.alloc);

    const bitsetTmp: std.bit_set.DynamicBitSetUnmanaged = try .initFull(self.alloc, self.bitset.bit_length);
    errdefer bitsetTmp.deinit(self.alloc);
}

fn matchBloomFilterOr(
    self: *const BlockQuery,
    ors: *const std.ArrayList(*const FilterExpression),
    expr: [2]*const FilterExpression,
) !bool {
    const keysTokens = try self.fieldsOrTokens(ors);
    if (keysTokens.len == 0) return true;

    for (keysTokens) |keyTokens| {
        const v = self.getInvariantColumnValue(keyTokens.key);
        if (v.len != 0) {
            if (self.matchPredicateByAll(v, keyTokens.tokens)) return true;

            continue;
        }

        const columnHeader = self.getColumnHeader(expr, keyTokens.key) orelse continue;
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

fn getInvariantColumnValue(self: *const BlockQuery, key: []const u8) []const u8 {
    const id = self.getColumnId(key);
    _ = id;
    unreachable;
}
fn matchPredicateByAll(self: *const BlockQuery, key: []const u8) []const u8 {
    _ = self;
    _ = key;
    unreachable;
}
fn getColumnHeader(self: *const BlockQuery, key: []const u8) []const u8 {
    _ = self;
    _ = key;
    unreachable;
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

fn getColumnId(self: *const BlockQuery, key: []const u8) u16 {
    _ = self;
    _ = key;
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
