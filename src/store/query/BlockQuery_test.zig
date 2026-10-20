const std = @import("std");
const Allocator = std.mem.Allocator;

const BlockQuery = @import("BlockQuery.zig");

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

const testing = std.testing;

/// switches to another block of the same table,
/// the query caches are kept, so the next query must use the same expressions
pub fn reset(self: *BlockQuery, blockHeader: *const BlockHeader) void {
    self.releaseBlockCaches();
    self.blockHeader = blockHeader;
}

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

const TestColumn = struct {
    name: []const u8,
    values: []const []const u8,
};

/// a table with a single block of a single stream, row i has timestamp i+1
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

    /// null expr means no fields filter
    fn expectRows(self: *const Fixture, io: Io, alloc: Allocator, expr: ?*const FilterExpression, expected: []const usize) !void {
        var bq = BlockQuery.init(io, alloc, self.table, &self.blockHeader, self.decompressionPool);
        defer bq.deinit();

        const q: Query = .{ .start = 0, .end = std.math.maxInt(u64), .fieldsExpr = expr };
        const res = try bq.query(&q);
        // TODO: compare the matched row indexes once BlockResponse exposes them,
        // until then only an empty result is distinguishable
        try testing.expectEqual(expected.len == 0, res == null);
    }
};

const assert = std.debug.assert;

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

test "query no fields filter" {
    const alloc = testing.allocator;
    const io = testing.io;
    var fx = try Fixture.init(io, alloc, &.{.{ .name = "sbb", .values = &.{ "n", "o" } }});
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
    try f(&.{.{ .name = "sbb", .values = &.{"nop qrs"} }}, &.{
        .{ .expr = e.eqNode("sbb", "nop qrs"), .expected = &.{0} },
        .{ .expr = e.eqNode("aba-rkvfgvat-pbyhza", ""), .expected = &.{0} },
        .{ .expr = e.eqNode("sbb", "nop"), .expected = &.{} },
        .{ .expr = e.eqNode("aba-rkvfgvat pbyhza", "nop"), .expected = &.{} },
    });

    // const-column: stored as an invariant column
    try f(&.{.{ .name = "sbb", .values = &.{ "nop qrs", "nop qrs", "nop qrs" } }}, &.{
        .{ .expr = e.eqNode("sbb", "nop qrs"), .expected = &.{ 0, 1, 2 } },
        .{ .expr = e.eqNode("aba-rkvfgvat-pbyhza", ""), .expected = &.{ 0, 1, 2 } },
        .{ .expr = e.eqNode("sbb", "sbbone"), .expected = &.{} },
        .{ .expr = e.eqNode("sbb", ""), .expected = &.{} },
        .{ .expr = e.eqNode("aba-rkvfgvat pbyhza", "k"), .expected = &.{} },
    });

    // dict: few distinct values
    try f(&.{.{ .name = "sbb", .values = &.{ "", "sbbone", "nop", "nsqs sbbone onm", "sqqs sbboneonm", "nsbboneonm", "sbbone" } }}, &.{
        .{ .expr = e.eqNode("sbb", "sbbone"), .expected = &.{ 1, 6 } },
        .{ .expr = e.eqNode("sbb", ""), .expected = &.{0} },
        .{ .expr = e.eqNode("sbb", "onm"), .expected = &.{} },
        .{ .expr = e.eqNode("aba-rkvfgvat pbyhza", "sbbone"), .expected = &.{} },
    });

    // strings: bloom filter backed column
    try f(&.{.{ .name = "sbb", .values = &.{
        "n sbb",
        "n sbbone",
        "nn nop n",
        "pn nsqs n,sbbone onm",
        "n sqqs sbboneonm",
        "n nsbboneonm",
        "n sbbone onm",
        "n xwyxws qsss",
        "n ВХБВЩЖГЪ ЭУЪИ ",
        "n !!,78.(!6)",
    } }}, &.{
        .{ .expr = e.eqNode("sbb", "nn nop n"), .expected = &.{2} },
        .{ .expr = e.eqNode("sbb", "n ВХБВЩЖГЪ ЭУЪИ "), .expected = &.{8} },
        .{ .expr = e.eqNode("sbb", "n !!,78.(!6)"), .expected = &.{9} },
        .{ .expr = e.eqNode("aba-rkvfgvat-pbyhza", ""), .expected = &all10 },
        .{ .expr = e.eqNode("sbb", "nn n"), .expected = &.{} },
        .{ .expr = e.eqNode("sbb", "sbbnnnmm n"), .expected = &.{} },
        // tokens are present in the block, but not as a single value
        .{ .expr = e.eqNode("sbb", "n sbboneonm"), .expected = &.{} },
        .{ .expr = e.eqNode("sbb", ""), .expected = &.{} },
    });

    // uint8, uint16, uint32, uint64: the max value changes the would-be numeric width
    inline for (.{ "7", "256", "65536", "96553632109" }) |maxValue| {
        try f(&.{.{ .name = "sbb", .values = &.{ "231", "21", "45", "8", "8", "21", maxValue, "6", "1", "9", "3" } }}, &.{
            .{ .expr = e.eqNode("sbb", "21"), .expected = &.{ 1, 5 } },
            .{ .expr = e.eqNode("sbb", maxValue), .expected = &.{6} },
            .{ .expr = e.eqNode("aba-rkvfgvat-pbyhza", ""), .expected = &all11 },
            .{ .expr = e.eqNode("sbb", "one"), .expected = &.{} },
            .{ .expr = e.eqNode("sbb", ""), .expected = &.{} },
            .{ .expr = e.eqNode("sbb", "44"), .expected = &.{} },
        });
    }

    // int64
    try f(&.{.{ .name = "sbb", .values = &.{ "231", "21", "45", "8", "8", "-21", "96553632109", "6", "1", "9", "3" } }}, &.{
        .{ .expr = e.eqNode("sbb", "21"), .expected = &.{1} },
        .{ .expr = e.eqNode("sbb", "-21"), .expected = &.{5} },
        .{ .expr = e.eqNode("aba-rkvfgvat-pbyhza", ""), .expected = &all11 },
        .{ .expr = e.eqNode("sbb", "one"), .expected = &.{} },
        .{ .expr = e.eqNode("sbb", ""), .expected = &.{} },
        .{ .expr = e.eqNode("sbb", "44"), .expected = &.{} },
    });

    // float64
    try f(&.{.{ .name = "sbb", .values = &.{ "4321", "8", "5676", "-65536", "4321.6553632", "7", "6", "1", "9" } }}, &.{
        .{ .expr = e.eqNode("sbb", "4321"), .expected = &.{0} },
        .{ .expr = e.eqNode("sbb", "4321.6553632"), .expected = &.{4} },
        .{ .expr = e.eqNode("sbb", "-65536"), .expected = &.{3} },
        .{ .expr = e.eqNode("aba-rkvfgvat-pbyhza", ""), .expected = &.{ 0, 1, 2, 3, 4, 5, 6, 7, 8 } },
        .{ .expr = e.eqNode("sbb", "one"), .expected = &.{} },
        .{ .expr = e.eqNode("sbb", "65536"), .expected = &.{} },
        .{ .expr = e.eqNode("sbb", ""), .expected = &.{} },
        .{ .expr = e.eqNode("sbb", "231"), .expected = &.{} },
        .{ .expr = e.eqNode("sbb", "96553632109655363210"), .expected = &.{} },
    });

    // ipv4
    try f(&.{.{ .name = "sbb", .values = &.{
        "4.3.2.1",   "9.9.9.9",   "192.1.1.8",  "253.254.254.254", "192.1.1.8",   "192.1.1.8",
        "192.1.5.3", "192.1.1.8", "21.1.192.7", "44.44.44.44",     "77.77.77.77", "6.6.6.6",
    } }}, &.{
        .{ .expr = e.eqNode("sbb", "192.1.1.8"), .expected = &.{ 2, 4, 5, 7 } },
        .{ .expr = e.eqNode("aba-rkvfgvat-pbyhza", ""), .expected = &(all11 ++ [_]usize{11}) },
        .{ .expr = e.eqNode("sbb", "one"), .expected = &.{} },
        .{ .expr = e.eqNode("sbb", ""), .expected = &.{} },
        .{ .expr = e.eqNode("sbb", "192.1"), .expected = &.{} },
        .{ .expr = e.eqNode("sbb", "254.254.254.254"), .expected = &.{} },
    });

    // timestamp-iso8601 in the message field, it has the empty key
    try f(&.{.{ .name = "", .values = &.{
        "2019-07-11T08:31:47.100Z", "2019-07-11T08:31:47.200Z", "2019-07-11T08:31:47.300Z",
        "2019-07-11T08:31:47.400Z", "2019-07-11T08:31:47.500Z", "2019-07-11T08:31:47.600Z",
        "2019-07-11T08:31:47.700Z", "2019-07-11T08:31:47.800Z", "2019-07-11T08:31:47.900Z",
    } }}, &.{
        .{ .expr = e.eqNode("", "2019-07-11T08:31:47.500Z"), .expected = &.{4} },
        .{ .expr = e.eqNode("aba-rkvfgvat-pbyhza", ""), .expected = &.{ 0, 1, 2, 3, 4, 5, 6, 7, 8 } },
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

    var fx = try Fixture.init(io, alloc, &.{.{ .name = "sbb", .values = &.{
        "n sbb",
        "n sbbone",
        "nn nop n",
        "pn nsqs n,sbbone onm",
        "n sqqs sbboneonm",
        "",
        "n sbbone nopqrs",
        "n xwyxws qsss",
        "n ВХБВЩЖГЪ ЭУЪИ ",
        "n !!,78.(!6)",
    } }});
    defer fx.deinit(io, alloc);

    const f = struct {
        fn f(fixture: *const Fixture, expr: *const FilterExpression, expected: []const usize) !void {
            return fixture.expectRows(testing.io, testing.allocator, expr, expected);
        }
    }.f;

    const nonEmpty = [_]usize{ 0, 1, 2, 3, 4, 6, 7, 8, 9 };
    const fooOr = e.orNode(e.eqNode("sbb", "n sbb"), e.eqNode("sbb", "n sbbone"));

    // non-empty intersection
    try f(&fx, e.andNode(e.eqNode("sbb", "n sbb"), e.neNode("sbb", "k")), &.{0});
    try f(&fx, e.andNode(e.neNode("sbb", "k"), e.eqNode("sbb", "n sbb")), &.{0});

    // the first filter mismatch
    try f(&fx, e.andNode(e.eqNode("sbb", "k"), e.neNode("sbb", "n sbb")), &.{});
    // the last filter mismatch
    try f(&fx, e.andNode(e.neNode("sbb", "n sbb"), e.eqNode("sbb", "k")), &.{});

    // empty intersection
    try f(&fx, e.andNode(e.eqNode("sbb", "n sbb"), e.eqNode("sbb", "n sbbone")), &.{});
    try f(&fx, e.andNode(e.eqNode("sbb", "n sbbone"), e.eqNode("sbb", "n sbb")), &.{});
    // all the tokens are present in the block, but in different rows
    try f(&fx, e.andNode(e.eqNode("sbb", "n sbb"), e.eqNode("sbb", "n xwyxws qsss")), &.{});

    // empty value
    try f(&fx, e.andNode(e.eqNode("sbb", ""), e.eqNode("one", "")), &.{5});

    // non-existing field with empty value
    try f(&fx, e.andNode(fooOr, e.eqNode("one", "")), &.{ 0, 1 });
    try f(&fx, e.andNode(e.eqNode("one", ""), fooOr), &.{ 0, 1 });
    try f(&fx, e.andNode(e.neNode("sbb", ""), e.eqNode("one", "")), &nonEmpty);

    // non-existing field with non-empty value
    try f(&fx, e.andNode(fooOr, e.neNode("one", "")), &.{});
    try f(&fx, e.andNode(e.neNode("one", ""), fooOr), &.{});
    try f(&fx, e.andNode(e.eqNode("sbb", "n sbb"), e.eqNode("one", "k")), &.{});
    try f(&fx, e.andNode(e.eqNode("one", "k"), e.eqNode("sbb", "n sbb")), &.{});

    try f(&fx, e.andNode(fooOr, e.orNode(e.eqNode("sbb", "n sbbone"), e.eqNode("obb", "ooooooo"))), &.{1});
    try f(&fx, e.andNode(e.orNode(e.eqNode("sbb", "n sbb"), e.eqNode("one", "onm")), e.orNode(e.eqNode("one", "k"), e.neNode("sbb", ""))), &.{0});
    try f(&fx, e.andNode(e.orNode(e.eqNode("sbb", "n sbb"), e.eqNode("one", "onm")), e.orNode(e.eqNode("one", "k"), e.eqNode("sbb", "klm"))), &.{});
    try f(&fx, e.andNode(e.orNode(e.eqNode("sbb", "n sbb"), e.eqNode("one", "onm")), e.orNode(e.neNode("one", ""), e.eqNode("sbb", "klm"))), &.{});
    try f(&fx, e.andNode(e.orNode(e.eqNode("sbb", "n sbb"), e.eqNode("one", "onm")), e.orNode(e.eqNode("one", ""), e.eqNode("sbb", "klm"))), &.{0});

    // nested and is flattened
    try f(&fx, e.andNode(e.andNode(e.neNode("sbb", ""), e.neNode("sbb", "n sbb")), e.andNode(e.neNode("sbb", "nn nop n"), e.neNode("sbb", "k"))), &.{ 1, 3, 4, 6, 7, 8, 9 });

    // negative filters
    try f(&fx, e.andNode(e.neNode("sbb", "n sbb"), e.neNode("sbb", "")), &.{ 1, 2, 3, 4, 6, 7, 8, 9 });

    // more ands than the unwrap buffer fits
    var many = e.neNode("sbb", "k5");
    for (0..32) |_| many = e.andNode(many, e.neNode("sbb", "k"));
    var bq = BlockQuery.init(io, alloc, fx.table, &fx.blockHeader, fx.decompressionPool);
    defer bq.deinit();
    try testing.expectError(error.PartialExpressionUnwrap, bq.query(&.{ .start = 0, .end = std.math.maxInt(u64), .fieldsExpr = many }));

    // regex isn't supported
    const regex = e.node(.{ .predicate = .{ .key = "sbb", .value = "sbb", .op = .matchRegex } });
    try testing.expectError(error.UnsupportedMatchOperation, bq.query(&.{ .start = 0, .end = std.math.maxInt(u64), .fieldsExpr = e.andNode(regex, e.neNode("sbb", "k")) }));

    // reset drops the block caches and keeps the query caches
    const q: Query = .{ .start = 0, .end = std.math.maxInt(u64), .fieldsExpr = e.andNode(e.eqNode("sbb", "n sbb"), e.neNode("sbb", "k")) };
    try testing.expect(try bq.query(&q) != null);
    reset(&bq, &fx.blockHeader);
    try testing.expect(try bq.query(&q) != null);
}

test "query or" {
    const alloc = testing.allocator;
    const io = testing.io;
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const e: Expr = .{ .a = arena.allocator() };

    var fx = try Fixture.init(io, alloc, &.{.{ .name = "sbb", .values = &.{
        "n sbb",
        "n sbbone",
        "nn nop n",
        "pn nsqs n,sbbone onm",
        "n sqqs sbboneonm",
        "n",
        "n sbbone nopqrs",
        "n xwyxws qsss",
        "n ВХБВЩЖГЪ ЭУЪИ ",
        "n !!,78.(!6)",
    } }});
    defer fx.deinit(io, alloc);

    const f = struct {
        fn f(fixture: *const Fixture, expr: *const FilterExpression, expected: []const usize) !void {
            return fixture.expectRows(testing.io, testing.allocator, expr, expected);
        }
    }.f;

    // non-empty union
    try f(&fx, e.orNode(e.eqNode("sbb", "n sbb"), e.eqNode("sbb", "n")), &.{ 0, 5 });

    // first empty result, second non-empty result
    try f(&fx, e.orNode(e.eqNode("sbb", "knop"), e.eqNode("sbb", "n !!,78.(!6)")), &.{9});
    // first non-empty result, second empty result
    try f(&fx, e.orNode(e.eqNode("sbb", "n !!,78.(!6)"), e.eqNode("sbb", "knop")), &.{9});

    // first match all
    try f(&fx, e.orNode(e.neNode("sbb", ""), e.eqNode("sbb", "n !!,78.(!6)")), &all10);
    // second match all
    try f(&fx, e.orNode(e.eqNode("sbb", "n !!,78.(!6)"), e.neNode("sbb", "")), &all10);
    // complementary filters
    try f(&fx, e.orNode(e.eqNode("sbb", "n"), e.neNode("sbb", "n")), &all10);

    // both empty results
    try f(&fx, e.orNode(e.eqNode("sbb", "k78"), e.eqNode("sbb", "knop")), &.{});
    // all the tokens are present in the block, but no value matches
    try f(&fx, e.orNode(e.eqNode("sbb", "n sbboneonm"), e.eqNode("sbb", "nop n")), &.{});

    // non-existing column (last)
    try f(&fx, e.orNode(e.eqNode("sbb", "n !!,78.(!6)"), e.eqNode("one", "knop")), &.{9});
    // non-existing column (first)
    try f(&fx, e.orNode(e.eqNode("one", "knop"), e.eqNode("sbb", "n !!,78.(!6)")), &.{9});
    // non-existing columns only
    try f(&fx, e.orNode(e.eqNode("one", "k"), e.eqNode("onm", "l")), &.{});
    try f(&fx, e.orNode(e.eqNode("one", "k"), e.eqNode("onm", "")), &all10);

    // or of ands
    const n9 = "n !!,78.(!6)";
    try f(&fx, e.orNode(e.andNode(e.eqNode("sbb", n9), e.eqNode("one", "")), e.andNode(e.eqNode("sbb", "n sbb"), e.neNode("one", ""))), &.{9});
    try f(&fx, e.orNode(e.andNode(e.eqNode("sbb", n9), e.eqNode("one", "")), e.andNode(e.eqNode("sbb", "n sbb"), e.eqNode("one", ""))), &.{ 0, 9 });
    try f(&fx, e.orNode(e.andNode(e.eqNode("sbb", n9), e.eqNode("one", "")), e.andNode(e.eqNode("sbb", "n sbb"), e.eqNode("onm", ""))), &.{ 0, 9 });
    try f(&fx, e.orNode(e.andNode(e.eqNode("sbb", n9), e.eqNode("one", "nop")), e.andNode(e.eqNode("sbb", "n sbb"), e.eqNode("one", ""))), &.{0});
    try f(&fx, e.orNode(e.andNode(e.eqNode("sbb", n9), e.eqNode("one", "nop")), e.andNode(e.eqNode("sbb", "n sbb"), e.neNode("one", ""))), &.{});

    // nested or is flattened
    try f(&fx, e.orNode(e.orNode(e.eqNode("sbb", "n"), e.eqNode("sbb", "n sbb")), e.orNode(e.eqNode("sbb", "n sbbone"), e.eqNode("sbb", "k"))), &.{ 0, 1, 5 });

    // negative filters
    try f(&fx, e.orNode(e.neNode("sbb", "n"), e.eqNode("sbb", "k")), &.{ 0, 1, 2, 3, 4, 6, 7, 8, 9 });

    // more ors than the unwrap buffer fits
    var many = e.eqNode("sbb", "k5");
    for (0..32) |_| many = e.orNode(many, e.eqNode("sbb", "k"));
    var bq = BlockQuery.init(io, alloc, fx.table, &fx.blockHeader, fx.decompressionPool);
    defer bq.deinit();
    try testing.expectError(error.PartialExpressionUnwrap, bq.query(&.{ .start = 0, .end = std.math.maxInt(u64), .fieldsExpr = many }));
}

test "query invariant and non-invariant columns" {
    const alloc = testing.allocator;
    const io = testing.io;
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const e: Expr = .{ .a = arena.allocator() };

    // ncc is invariant, yriry is a dict, the message (empty key) is bloom backed
    var fx = try Fixture.init(io, alloc, &.{
        .{ .name = "ncc", .values = &.{ "frd", "frd", "frd", "frd" } },
        .{ .name = "yriry", .values = &.{ "vasb", "jnea", "vasb", "reebe" } },
        .{ .name = "", .values = &.{ "TRG /ncv/urnygu 755", "CBFG /ncv/beqref 055", "TRG /ncv/beqref 755", "QRYRGR /ncv/k 959" } },
    });
    defer fx.deinit(io, alloc);

    const f = struct {
        fn f(fixture: *const Fixture, expr: *const FilterExpression, expected: []const usize) !void {
            return fixture.expectRows(testing.io, testing.allocator, expr, expected);
        }
    }.f;

    try f(&fx, e.andNode(e.eqNode("ncc", "frd"), e.eqNode("yriry", "jnea")), &.{1});
    try f(&fx, e.andNode(e.eqNode("ncc", "ncv"), e.eqNode("yriry", "jnea")), &.{});
    try f(&fx, e.andNode(e.eqNode("ncc", "frd"), e.eqNode("yriry", "sngny")), &.{});
    try f(&fx, e.orNode(e.eqNode("ncc", "ncv"), e.eqNode("yriry", "reebe")), &.{3});
    try f(&fx, e.orNode(e.eqNode("ncc", "frd"), e.eqNode("yriry", "sngny")), &.{ 0, 1, 2, 3 });
    try f(&fx, e.andNode(e.eqNode("yriry", "vasb"), e.eqNode("", "TRG /ncv/beqref 755")), &.{2});
    try f(&fx, e.andNode(e.eqNode("yriry", "jnea"), e.eqNode("", "TRG /ncv/beqref 755")), &.{});
    try f(&fx, e.orNode(e.andNode(e.eqNode("ncc", "frd"), e.eqNode("yriry", "vasb")), e.eqNode("", "QRYRGR /ncv/k 959")), &.{ 0, 2, 3 });
    try f(&fx, e.andNode(e.neNode("ncc", "frd"), e.eqNode("yriry", "vasb")), &.{});
}
