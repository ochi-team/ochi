const std = @import("std");
const assert = std.debug.assert;
const Allocator = std.mem.Allocator;

const Io = std.Io;
const Query = @import("../../query/Query.zig");
const FilterExpression = Query.FilterExpression;
const Table = @import("../data/Table.zig");
const BlockHeader = @import("../data/BlockHeader.zig");
const MemTable = @import("../data/MemTable.zig");
const TimestampsEncoder = @import("../data/TimestampsEncoder.zig");
const CompressionPool = @import("../compression/CompressionPool.zig");
const DecompressionPool = @import("../compression/DecompressionPool.zig");
const lines = @import("../lines.zig");
const Field = lines.Field;
const Line = lines.Line;

const BlockQuery = @import("BlockQuery.zig");

const testing = std.testing;

pub fn reset(bq: *BlockQuery, blockHeader: *const BlockHeader) void {
    releaseBlockCaches(bq);
    bq.blockHeader = blockHeader;
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

    for (self.columnValuesCache.items) |c| self.alloc.free(c.values);
    self.columnValuesCache.clearRetainingCapacity();
    const pool = self.unpacker.compressionPool;
    self.unpacker.deinit(self.alloc);
    self.unpacker = .init(pool);
}

const TestColumn = struct {
    name: []const u8,
    values: []const []const u8,
};

/// a table with a single block, row i has timestamp i+1
const Fixture = struct {
    decompressionPool: *DecompressionPool,
    table: *Table,
    blockHeader: BlockHeader,

    fn init(io: Io, alloc: Allocator, columns: []const TestColumn) !Fixture {
        const rowsCount = columns[0].values.len;

        var arena = std.heap.ArenaAllocator.init(alloc);
        defer arena.deinit();
        const a = arena.allocator();

        const testLines = try a.alloc(Line, rowsCount);
        for (testLines, 0..) |*line, i| {
            var fields = std.ArrayList(Field).empty;
            for (columns) |c| {
                assert(c.values.len == rowsCount);
                // empty value is the same as a missing field
                if (c.values[i].len == 0) continue;
                try fields.append(a, .{ .key = c.name, .value = c.values[i] });
            }
            line.* = .{ .timestampNs = i + 1, .fields = .view(fields.items) };
        }

        const timestampsEncoders = try TimestampsEncoder.TimestampsEncoderPool.init(alloc, 1);
        defer timestampsEncoders.deinit(alloc);
        const compressionPool = try CompressionPool.init(alloc, 1);
        defer compressionPool.deinit(alloc);
        const decompressionPool = try DecompressionPool.init(alloc, 1);
        errdefer decompressionPool.deinit(alloc);

        const memTable = try MemTable.init(alloc);
        try memTable.addLinesForSid(io, alloc, timestampsEncoders, compressionPool, .{ .id = 1, .tenantID = 1 }, testLines);
        const table = try Table.fromMem(io, alloc, memTable, decompressionPool);
        errdefer table.release(io);

        try testing.expectEqual(1, table.indexBlockHeaders.len);
        const ibh = table.indexBlockHeaders[0];
        const indexBuf = try a.alloc(u8, ibh.size);
        const n = try table.readIndex(io, indexBuf, ibh.offset);
        try testing.expectEqual(indexBuf.len, n);

        var blockHeaders = std.ArrayList(BlockHeader).empty;
        try BlockHeader.decodeIndexWindow(io, a, decompressionPool, &blockHeaders, indexBuf, ibh);
        try testing.expectEqual(1, blockHeaders.items.len);
        try testing.expectEqual(rowsCount, blockHeaders.items[0].len);

        return .{
            .decompressionPool = decompressionPool,
            .table = table,
            .blockHeader = blockHeaders.items[0],
        };
    }

    fn deinit(self: *Fixture, io: Io, alloc: Allocator) void {
        self.table.release(io);
        self.decompressionPool.deinit(alloc);
    }

    /// null expr means no fields filter,
    /// same as in the real query
    fn expectRows(self: *const Fixture, io: Io, alloc: Allocator, expr: ?*const FilterExpression, expected: []const usize) !void {
        var bq = try BlockQuery.init(io, alloc, self.table, &self.blockHeader, self.decompressionPool);
        defer bq.deinit();

        const q: Query = .{ .start = 0, .end = std.math.maxInt(u64), .fieldsExpr = expr };
        const res = try bq.query(&q);
        // TODO: compare the matched row indexes once BlockResponse exposes them,
        // until then only an empty result is distinguishable
        try testing.expectEqual(expected.len == 0, res == null);
        // TODO: no real expected/res assertion in the test, at least a size,
        // or move it directly into block result hits
    }
};

const Expr = struct {
    a: Allocator,

    fn node(self: Expr, e: FilterExpression) *const FilterExpression {
        const n = self.a.create(FilterExpression) catch @panic("OOM");
        n.* = e;
        return n;
    }

    fn eqNode(self: Expr, key: []const u8, value: []const u8) *const FilterExpression {
        return self.node(.{ .predicate = .{ .key = key, .value = value, .op = .equal } });
    }

    fn neNode(self: Expr, key: []const u8, value: []const u8) *const FilterExpression {
        return self.node(.{ .predicate = .{ .key = key, .value = value, .op = .notEqual } });
    }

    fn andNode(self: Expr, l: *const FilterExpression, r: *const FilterExpression) *const FilterExpression {
        return self.node(.{ .andOp = .{ l, r } });
    }

    fn orNode(self: Expr, l: *const FilterExpression, r: *const FilterExpression) *const FilterExpression {
        return self.node(.{ .orOp = .{ l, r } });
    }
};

const all10 = [_]usize{ 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 };
const all11 = all10 ++ [_]usize{10};

test "bitsetIsEmpty" {
    const f = struct {
        fn f(alloc: Allocator, len: usize, set: []const usize, expectedEmpty: bool) !void {
            var bitset: std.bit_set.DynamicBitSetUnmanaged = try .initEmpty(alloc, len);
            defer bitset.deinit(alloc);

            for (set) |i| {
                bitset.set(i);
            }

            try testing.expectEqual(expectedEmpty, BlockQuery.bitsetIsEmpty(&bitset));
        }
    }.f;

    const alloc = testing.allocator;
    try f(alloc, 0, &.{}, true);
    try f(alloc, 1, &.{}, true);
    try f(alloc, 1, &.{0}, false);
    try f(alloc, 64, &.{42}, false);
    try f(alloc, 64, &.{}, true);
    try f(alloc, 129, &.{128}, false);
    try f(alloc, 128, &.{127}, false);
    try f(alloc, 127, &.{126}, false);
}

test "bitsetAndNot" {
    const alloc = std.testing.allocator;
    const Set = std.bit_set.DynamicBitSetUnmanaged;
    // 130 bits: spans three words with a partial tail
    var bm: Set = try .initFull(alloc, 130);
    defer bm.deinit(alloc);
    var x: Set = try .initEmpty(alloc, 130);
    defer x.deinit(alloc);

    BlockQuery.bitsetAndNot(&bm, &x);
    try std.testing.expectEqual(@as(usize, 130), bm.count());

    x.set(0);
    x.set(64);
    x.set(129);
    BlockQuery.bitsetAndNot(&bm, &x);
    try std.testing.expectEqual(@as(usize, 127), bm.count());
    try std.testing.expect(!bm.isSet(0) and !bm.isSet(64) and !bm.isSet(129));
    try std.testing.expect(bm.isSet(1) and bm.isSet(128));
}

test "query no fields filter" {
    const alloc = testing.allocator;
    const io = testing.io;
    var fx = try Fixture.init(io, alloc, &.{.{ .name = "key", .values = &.{ "n", "o" } }});
    defer fx.deinit(io, alloc);

    try fx.expectRows(io, alloc, null, &.{ 0, 1 });
}

test "query exact" {
    const alloc = testing.allocator;
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const e: Expr = .{ .a = arena.allocator() };

    const f = struct {
        const Case = struct { expr: *const FilterExpression, expected: []const usize };
        fn f(columns: []const TestColumn, cases: []const Case) !void {
            var fx = try Fixture.init(testing.io, testing.allocator, columns);
            defer fx.deinit(testing.io, testing.allocator);
            for (cases) |c| try fx.expectRows(testing.io, testing.allocator, c.expr, c.expected);
        }
    }.f;

    // single-row
    try f(&.{.{ .name = "key", .values = &.{"str val"} }}, &.{
        .{ .expr = e.eqNode("key", "str val"), .expected = &.{0} },
        .{ .expr = e.eqNode("no-columl", ""), .expected = &.{0} },
        .{ .expr = e.eqNode("key", "val"), .expected = &.{} },
        .{ .expr = e.eqNode("no column", "str"), .expected = &.{} },
    });

    // const-column: stored as an invariant column
    try f(&.{.{ .name = "key", .values = &.{ "str val", "str val", "str val" } }}, &.{
        .{ .expr = e.eqNode("key", "str val"), .expected = &.{ 0, 1, 2 } },
        .{ .expr = e.eqNode("another-key", ""), .expected = &.{ 0, 1, 2 } },
        .{ .expr = e.eqNode("key", "strval"), .expected = &.{} },
        .{ .expr = e.eqNode("key", ""), .expected = &.{} },
        .{ .expr = e.eqNode("another key", "k"), .expected = &.{} },
    });

    // dict: few distinct values
    try f(&.{.{ .name = "str", .values = &.{ "", "strval", "str", "long str val", "value", "another value", "strval", "0000" } }}, &.{
        .{ .expr = e.eqNode("str", "strval"), .expected = &.{ 1, 6 } },
        .{ .expr = e.eqNode("str", ""), .expected = &.{0} },
        .{ .expr = e.eqNode("key", "guu"), .expected = &.{} },
        .{ .expr = e.eqNode("not-found", "strval"), .expected = &.{} },
    });

    // strings: bloom filter backed column
    try f(&.{.{ .name = "key", .values = &.{
        "a key",
        "a word",
        "woop woop",
        "short thing,splited by coma",
        "an average word",
        "b longword",
        "a fail oom",
        "a qwerty long",
        "а ЮНИКОД Жиж",
        "a @#,42.(-!6]",
    } }}, &.{
        .{ .expr = e.eqNode("key", "woop woop"), .expected = &.{2} },
        .{ .expr = e.eqNode("key", "а ЮНИКОД Жиж"), .expected = &.{8} },
        .{ .expr = e.eqNode("key", "a @#,42.(-!6]"), .expected = &.{9} },
        .{ .expr = e.eqNode("non-existent", ""), .expected = &all10 },
        .{ .expr = e.eqNode("key", "aha"), .expected = &.{} },
        .{ .expr = e.eqNode("key", "woop"), .expected = &.{} },
        .{ .expr = e.eqNode("key", "longword"), .expected = &.{} },
        .{ .expr = e.eqNode("key", ""), .expected = &.{} },
    });

    // uint8, uint16, uint32, uint64: the max value changes the would-be numeric width
    inline for (.{ "7", "256", "65536", "96553632109" }) |maxValue| {
        try f(&.{.{ .name = "key", .values = &.{ "231", "21", "45", "8", "8", "21", maxValue, "6", "1", "9", "3" } }}, &.{
            .{ .expr = e.eqNode("key", "21"), .expected = &.{ 1, 5 } },
            .{ .expr = e.eqNode("key", maxValue), .expected = &.{6} },
            .{ .expr = e.eqNode("not-found", ""), .expected = &all11 },
            .{ .expr = e.eqNode("key", "one"), .expected = &.{} },
            .{ .expr = e.eqNode("key", ""), .expected = &.{} },
            .{ .expr = e.eqNode("key", "42"), .expected = &.{} },
            .{ .expr = e.eqNode("key", "+21"), .expected = &.{} },
        });
    }

    // int64
    try f(&.{.{ .name = "key", .values = &.{ "231", "21", "45", "8", "8", "-21", "96553632109", "6", "1", "9", "3" } }}, &.{
        .{ .expr = e.eqNode("key", "21"), .expected = &.{1} },
        .{ .expr = e.eqNode("key", "-21"), .expected = &.{5} },
        .{ .expr = e.eqNode("not-found", ""), .expected = &all11 },
        .{ .expr = e.eqNode("key", "one"), .expected = &.{} },
        .{ .expr = e.eqNode("key", ""), .expected = &.{} },
        .{ .expr = e.eqNode("key", "44"), .expected = &.{} },
    });

    // float64
    try f(&.{.{ .name = "key", .values = &.{ "4321", "8", "5676", "-65536", "4321.6553632", "7", "6", "1", "9" } }}, &.{
        .{ .expr = e.eqNode("key", "4321"), .expected = &.{0} },
        .{ .expr = e.eqNode("key", "4321.6553632"), .expected = &.{4} },
        .{ .expr = e.eqNode("key", "-65536"), .expected = &.{3} },
        .{ .expr = e.eqNode("not-found", ""), .expected = &.{ 0, 1, 2, 3, 4, 5, 6, 7, 8 } },
        .{ .expr = e.eqNode("key", "one"), .expected = &.{} },
        .{ .expr = e.eqNode("key", "65536"), .expected = &.{} },
        .{ .expr = e.eqNode("key", ""), .expected = &.{} },
        .{ .expr = e.eqNode("key", "231"), .expected = &.{} },
        .{ .expr = e.eqNode("key", "96553632109655363210"), .expected = &.{} },
    });

    // ipv4
    try f(&.{.{ .name = "key", .values = &.{
        "4.3.2.1",   "9.9.9.9",   "192.1.1.8",  "253.254.254.254", "192.1.1.8",   "192.1.1.8",
        "192.1.5.3", "192.1.1.8", "21.1.192.7", "44.44.44.44",     "77.77.77.77", "6.6.6.6",
    } }}, &.{
        .{ .expr = e.eqNode("key", "192.1.1.8"), .expected = &.{ 2, 4, 5, 7 } },
        .{ .expr = e.eqNode("not-found", ""), .expected = &(all11 ++ [_]usize{11}) },
        .{ .expr = e.eqNode("key", "one"), .expected = &.{} },
        .{ .expr = e.eqNode("key", ""), .expected = &.{} },
        .{ .expr = e.eqNode("key", "192.1"), .expected = &.{} },
        .{ .expr = e.eqNode("key", "254.254.254.254"), .expected = &.{} },
    });

    // timestamp-iso8601 in the message field, it has the empty key
    try f(&.{.{ .name = "", .values = &.{
        "2019-07-11T08:31:47.100Z", "2019-07-11T08:31:47.200Z", "2019-07-11T08:31:47.300Z",
        "2019-07-11T08:31:47.400Z", "2019-07-11T08:31:47.500Z", "2019-07-11T08:31:47.600Z",
        "2019-07-11T08:31:47.700Z", "2019-07-11T08:31:47.800Z", "2019-07-11T08:31:47.900Z",
    } }}, &.{
        .{ .expr = e.eqNode("", "2019-07-11T08:31:47.500Z"), .expected = &.{4} },
        .{ .expr = e.eqNode("not found", ""), .expected = &.{ 0, 1, 2, 3, 4, 5, 6, 7, 8 } },
        .{ .expr = e.eqNode("", "one"), .expected = &.{} },
        .{ .expr = e.eqNode("", ""), .expected = &.{} },
        .{ .expr = e.eqNode("", "2019-09-11T08:31:47.500Z"), .expected = &.{} },
    });
}

test "query and" {
    const alloc = testing.allocator;
    const io = testing.io;
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const e: Expr = .{ .a = arena.allocator() };

    var fx = try Fixture.init(io, alloc, &.{.{ .name = "key", .values = &.{
        "a key",
        "a word",
        "a longer word",
        "split,by coma",
        "an interesting one",
        "",
        "a very long string",
        "super cookie",
        "а ЮНИКОД Жиж",
        "n !!,78.(!6)",
    } }});
    defer fx.deinit(io, alloc);

    const f = struct {
        fn f(fixture: *const Fixture, expr: *const FilterExpression, expected: []const usize) !void {
            return fixture.expectRows(testing.io, testing.allocator, expr, expected);
        }
    }.f;

    const nonEmpty = [_]usize{ 0, 1, 2, 3, 4, 6, 7, 8, 9 };
    const fooOr = e.orNode(e.eqNode("key", "a key"), e.eqNode("key", "a word"));

    // non-empty intersection
    try f(&fx, e.andNode(e.eqNode("key", "a key"), e.neNode("key", "k")), &.{0});
    try f(&fx, e.andNode(e.neNode("key", "k"), e.eqNode("key", "a key")), &.{0});

    // the first filter mismatch
    try f(&fx, e.andNode(e.eqNode("key", "k"), e.neNode("key", "a key")), &.{});
    // the last filter mismatch
    try f(&fx, e.andNode(e.neNode("key", "a key"), e.eqNode("key", "k")), &.{});

    // empty intersection
    try f(&fx, e.andNode(e.eqNode("key", "a key"), e.eqNode("key", "a word")), &.{});
    try f(&fx, e.andNode(e.eqNode("key", "a word"), e.eqNode("key", "a key")), &.{});
    // all the tokens are present in the block, but in different rows
    try f(&fx, e.andNode(e.eqNode("key", "a key"), e.eqNode("key", "n xwyxws qsss")), &.{});

    // empty value
    try f(&fx, e.andNode(e.eqNode("key", ""), e.eqNode("one", "")), &.{5});

    // non-existing field with empty value
    try f(&fx, e.andNode(fooOr, e.eqNode("one", "")), &.{ 0, 1 });
    try f(&fx, e.andNode(e.eqNode("one", ""), fooOr), &.{ 0, 1 });
    try f(&fx, e.andNode(e.neNode("key", ""), e.eqNode("one", "")), &nonEmpty);

    // non-existing field with non-empty value
    try f(&fx, e.andNode(fooOr, e.neNode("one", "")), &.{});
    try f(&fx, e.andNode(e.neNode("one", ""), fooOr), &.{});
    try f(&fx, e.andNode(e.eqNode("key", "a key"), e.eqNode("one", "k")), &.{});
    try f(&fx, e.andNode(e.eqNode("one", "k"), e.eqNode("key", "a key")), &.{});

    // nested or, https://github.com/VictoriaMetrics/VictoriaMetrics/issues/6554
    try f(&fx, e.andNode(fooOr, e.orNode(e.eqNode("key", "a word"), e.eqNode("obb", "ooooooo"))), &.{1});
    try f(&fx, e.andNode(e.orNode(e.eqNode("key", "a key"), e.eqNode("one", "onm")), e.orNode(e.eqNode("one", "k"), e.neNode("key", ""))), &.{0});
    try f(&fx, e.andNode(e.orNode(e.eqNode("key", "a key"), e.eqNode("one", "onm")), e.orNode(e.eqNode("one", "k"), e.eqNode("key", "klm"))), &.{});
    try f(&fx, e.andNode(e.orNode(e.eqNode("key", "a key"), e.eqNode("one", "onm")), e.orNode(e.neNode("one", ""), e.eqNode("key", "klm"))), &.{});
    try f(&fx, e.andNode(e.orNode(e.eqNode("key", "a key"), e.eqNode("one", "onm")), e.orNode(e.eqNode("one", ""), e.eqNode("key", "klm"))), &.{0});

    // nested and is flattened
    try f(&fx, e.andNode(e.andNode(e.neNode("key", ""), e.neNode("key", "a key")), e.andNode(e.neNode("key", "a longer word"), e.neNode("key", "k"))), &.{ 1, 3, 4, 6, 7, 8, 9 });

    // negative filters
    try f(&fx, e.andNode(e.neNode("key", "a key"), e.neNode("key", "")), &.{ 1, 2, 3, 4, 6, 7, 8, 9 });

    // more ands than the unwrap buffer fits
    var many = e.neNode("key", "k5");
    for (0..32) |_| many = e.andNode(many, e.neNode("key", "k"));
    var bq = try BlockQuery.init(io, alloc, fx.table, &fx.blockHeader, fx.decompressionPool);
    defer bq.deinit();
    try testing.expectError(error.PartialExpressionUnwrap, bq.query(&.{ .start = 0, .end = std.math.maxInt(u64), .fieldsExpr = many }));

    // regex isn't supported
    const regex = e.node(.{ .predicate = .{ .key = "key", .value = "key", .op = .matchRegex } });
    try testing.expectError(error.UnsupportedMatchOperation, bq.query(&.{ .start = 0, .end = std.math.maxInt(u64), .fieldsExpr = e.andNode(regex, e.neNode("key", "k")) }));

    // reset drops the block caches and keeps the query caches
    const q: Query = .{ .start = 0, .end = std.math.maxInt(u64), .fieldsExpr = e.andNode(e.eqNode("key", "a key"), e.neNode("key", "k")) };
    try testing.expect(try bq.query(&q) != null);
    reset(&bq, &fx.blockHeader);
    try testing.expect(try bq.query(&q) != null);
}
