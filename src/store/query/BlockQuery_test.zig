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
        var bq = BlockQuery.init(io, alloc, self.table, &self.blockHeader, self.decompressionPool);
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
    var fx = try Fixture.init(io, alloc, &.{.{ .name = "sbb", .values = &.{ "n", "o" } }});
    defer fx.deinit(io, alloc);

    try fx.expectRows(io, alloc, null, &.{ 0, 1 });
}
