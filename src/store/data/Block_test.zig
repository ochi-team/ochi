const std = @import("std");

const Field = @import("../lines.zig").Field;
const Line = @import("../lines.zig").Line;
const Column = @import("Column.zig");
const BlockData = @import("BlockData.zig").BlockData;
const Unpacker = @import("Unpacker.zig").Unpacker;
const ValuesDecoder = @import("ValuesDecoder.zig");
const TimestampsEncoder = @import("TimestampsEncoder.zig");
const Table = @import("../data/Table.zig");
const CompressionPool = @import("../compression/CompressionPool.zig");
const DecompressionPool = @import("../compression/DecompressionPool.zig");

const Block = @import("Block.zig");
const areSameFields = Block.areSameFields;
const canBeSavedAsInvariant = Block.canBeSavedAsInvariant;
const maxColumns = Block.maxColumns;
const maxLines = Block.maxLines;

const SID = @import("../lines.zig").SID;
const TableWriter = @import("TableWriter.zig");
const MemTable = @import("MemTable.zig");
const TableReader = @import("TableReader.zig");
const BlockHeader = @import("BlockHeader.zig");

const testing = std.testing;

fn expectEqualBlocks(a: *const Block, b: *const Block) !void {
    try testing.expectEqualSlices(u64, a.timestamps, b.timestamps);
    try testing.expectEqual(a.firstInvariant, b.firstInvariant);

    const colsA = a.getColumns();
    const colsB = b.getColumns();
    try testing.expectEqual(colsA.len, colsB.len);
    for (colsA, colsB) |ca, cb| {
        try testing.expectEqualStrings(ca.key, cb.key);
        try testing.expectEqual(ca.values.len, cb.values.len);
        for (ca.values, cb.values) |va, vb| {
            try testing.expectEqualStrings(va, vb);
        }
    }

    const invariantA = a.getInvariantColumns();
    const invariantB = b.getInvariantColumns();
    try testing.expectEqual(invariantA.len, invariantB.len);
    for (invariantA, invariantB) |ca, cb| {
        try testing.expectEqualStrings(ca.key, cb.key);
        try testing.expectEqual(ca.values.len, 1);
        try testing.expectEqual(cb.values.len, 1);
        try testing.expectEqualStrings(ca.values[0], cb.values[0]);
    }
}

test "initFromLines and initFromData produce identical blocks" {
    const alloc = testing.allocator;
    const io = testing.io;

    const sid = SID{ .id = 1, .tenantID = 1111 };

    var f1 = [_]Field{ .{ .key = "app", .value = "seq" }, .{ .key = "level", .value = "info" } };
    var f2 = [_]Field{ .{ .key = "app", .value = "seq" }, .{ .key = "level", .value = "warn" } };
    var f3 = [_]Field{ .{ .key = "app", .value = "seq" }, .{ .key = "level", .value = "error" } };
    var f4 = [_]Field{ .{ .key = "cpu", .value = "0.8" }, .{ .key = "memory", .value = "512MB" } };
    var lines1 = [_]Line{
        .{ .timestampNs = 1, .fields = &f1 },
        .{ .timestampNs = 2, .fields = &f1 },
    };
    var lines2 = [_]Line{
        .{ .timestampNs = 1, .fields = &f1 },
        .{ .timestampNs = 2, .fields = &f2 },
        .{ .timestampNs = 3, .fields = &f3 },
    };
    var lines3 = [_]Line{
        .{ .timestampNs = 1, .fields = &f1 },
        .{ .timestampNs = 2, .fields = &f4 },
    };

    const Case = struct {
        lines: []Line,
    };
    const cases = &[_]Case{
        .{
            .lines = &lines1,
        },
        .{
            .lines = &lines2,
        },
        .{
            .lines = &lines3,
        },
    };

    for (cases) |case| {
        const timestampsEncoders = try TimestampsEncoder.TimestampsEncoderPool.init(alloc, 1);
        defer timestampsEncoders.deinit(alloc);
        const compressionPool = try CompressionPool.init(alloc, 1);
        defer compressionPool.deinit(alloc);
        const decompressionPool = try DecompressionPool.init(alloc, 1);
        defer decompressionPool.deinit(alloc);

        var blockA = try Block.initFromLines(alloc, case.lines);
        defer blockA.deinit(alloc);

        const memTable = try MemTable.init(alloc);
        const table = try Table.fromMem(io, alloc, memTable, decompressionPool);
        defer table.close(io);

        const writer = try TableWriter.initMem(alloc, memTable, timestampsEncoders, compressionPool);
        defer writer.deinit(alloc);

        var bh = BlockHeader.initFromBlock(&blockA, sid);
        try writer.writeBlock(io, alloc, &blockA, &bh);

        const sr = TableReader{
            .table = table,
            .metaIndexBuf = writer.metaindexDst.buffer.items,
            .columnsKeysBuf = writer.columnKeysDst.buffer.items,
            .columnIdxsBuf = writer.columnIdxsDst.buffer.items,
            .columnIDGen = writer.columnIDGen,
            .colIdx = &writer.colIdx,
        };

        var bd = BlockData.initEmpty();
        defer bd.deinit(alloc);
        try bd.readFrom(io, alloc, &bh, &sr);

        var unpacker = Unpacker(false).init(decompressionPool);
        defer unpacker.deinit(alloc);
        var decoder: ValuesDecoder = .{};
        defer decoder.deinit(alloc);

        var blockB = try Block.initFromData(io, alloc, timestampsEncoders, &bd, false, &unpacker, &decoder);
        defer blockB.deinit(alloc);

        try expectEqualBlocks(&blockA, &blockB);

        var gatheredLines = std.ArrayList(Line).empty;
        defer {
            for (gatheredLines.items) |line| alloc.free(line.fields);
            gatheredLines.deinit(alloc);
        }
        try blockA.gatherLines(alloc, &gatheredLines);
        try testing.expectEqual(case.lines.len, gatheredLines.items.len);
        for (case.lines, gatheredLines.items) |origLine, gatheredLine| {
            try testing.expectEqual(origLine.timestampNs, gatheredLine.timestampNs);
            try testing.expectEqual(origLine.fields.len, gatheredLine.fields.len);
            for (origLine.fields, gatheredLine.fields) |of, gf| {
                try testing.expectEqualStrings(of.key, gf.key);
                try testing.expectEqualStrings(of.value, gf.value);
            }
        }
    }
}

test "initFromData decodes multiple typed columns in one block without cross-column corruption" {
    const alloc = testing.allocator;
    const io = testing.io;

    const sid = SID{ .id = 1, .tenantID = 1111 };

    var fieldsStorage: [10][2]Field = undefined;
    var lines: [10]Line = undefined;
    for (0..10) |i| {
        fieldsStorage[i] = .{
            .{ .key = "client", .value = try std.fmt.allocPrint(alloc, "10.1.0.{d}", .{i}) },
            .{ .key = "seen", .value = try std.fmt.allocPrint(alloc, "2026-08-01T00:00:{d:0>2}.000Z", .{i}) },
        };
        lines[i] = .{ .timestampNs = @intCast(i + 1), .fields = fieldsStorage[i][0..] };
    }
    defer {
        for (fieldsStorage) |fields| {
            for (fields) |f| alloc.free(f.value);
        }
    }

    var blockA = try Block.initFromLines(alloc, &lines);
    defer blockA.deinit(alloc);

    const timestampsEncoders = try TimestampsEncoder.TimestampsEncoderPool.init(alloc, 1);
    defer timestampsEncoders.deinit(alloc);
    const compressionPool = try CompressionPool.init(alloc, 1);
    defer compressionPool.deinit(alloc);
    const decompressionPool = try DecompressionPool.init(alloc, 1);
    defer decompressionPool.deinit(alloc);

    const memTable = try MemTable.init(alloc);
    const table = try Table.fromMem(io, alloc, memTable, decompressionPool);
    defer table.close(io);

    const writer = try TableWriter.initMem(alloc, memTable, timestampsEncoders, compressionPool);
    defer writer.deinit(alloc);

    var bh = BlockHeader.initFromBlock(&blockA, sid);
    try writer.writeBlock(io, alloc, &blockA, &bh);

    const sr = TableReader{
        .table = table,
        .metaIndexBuf = writer.metaindexDst.buffer.items,
        .columnsKeysBuf = writer.columnKeysDst.buffer.items,
        .columnIdxsBuf = writer.columnIdxsDst.buffer.items,
        .columnIDGen = writer.columnIDGen,
        .colIdx = &writer.colIdx,
    };

    var bd = BlockData.initEmpty();
    defer bd.deinit(alloc);
    try bd.readFrom(io, alloc, &bh, &sr);

    var unpacker = Unpacker(false).init(decompressionPool);
    defer unpacker.deinit(alloc);
    var decoder: ValuesDecoder = .{};
    defer decoder.deinit(alloc);

    var blockB = try Block.initFromData(io, alloc, timestampsEncoders, &bd, false, &unpacker, &decoder);
    defer blockB.deinit(alloc);

    var gatheredLines = std.ArrayList(Line).empty;
    defer {
        for (gatheredLines.items) |line| alloc.free(line.fields);
        gatheredLines.deinit(alloc);
    }
    try blockB.gatherLines(alloc, &gatheredLines);

    try testing.expectEqual(lines.len, gatheredLines.items.len);
    for (lines, gatheredLines.items) |origLine, gatheredLine| {
        for (origLine.fields) |of| {
            var found = false;
            for (gatheredLine.fields) |gf| {
                if (std.mem.eql(u8, of.key, gf.key)) {
                    found = true;
                    try testing.expectEqualStrings(of.value, gf.value);
                }
            }
            try testing.expect(found);
        }
    }
}

test "areSameFields: happy path" {
    var fields1 = [_]Field{
        .{ .key = "level", .value = "info" },
        .{ .key = "app", .value = "seq" },
    };
    var fields2 = [_]Field{
        .{ .key = "level", .value = "warn" },
        .{ .key = "app", .value = "seq" },
    };
    var lines = [_]Line{
        .{
            .timestampNs = 1,
            .fields = fields1[0..],
        },
        .{
            .timestampNs = 2,
            .fields = fields2[0..],
        },
    };

    try testing.expectEqual(true, areSameFields(&lines));
}

test "areSameFields: unhappy path" {
    var fields1 = [_]Field{
        .{ .key = "cpu", .value = "0.1" },
        .{ .key = "app", .value = "seq" },
    };
    var fields2 = [_]Field{
        .{ .key = "level", .value = "warn" },
        .{ .key = "app", .value = "seq" },
    };
    var lines = [_]Line{
        .{
            .timestampNs = 1,
            .fields = fields1[0..],
        },
        .{
            .timestampNs = 2,
            .fields = fields2[0..],
        },
    };

    try testing.expectEqual(false, areSameFields(&lines));
}

test "areSameValuesWithinColumn: happy path" {
    var fields1 = [_]Field{
        .{ .key = "level", .value = "info" },
        .{ .key = "app", .value = "seq" },
    };
    var fields2 = [_]Field{
        .{ .key = "level", .value = "info" },
        .{ .key = "app", .value = "seq" },
    };
    var lines = [_]Line{
        .{
            .timestampNs = 1,
            .fields = fields1[0..],
        },
        .{
            .timestampNs = 2,
            .fields = fields2[0..],
        },
    };

    try testing.expectEqual(true, canBeSavedAsInvariant(&lines, 0));
    try testing.expectEqual(true, canBeSavedAsInvariant(&lines, 1));
}

test "areSameValuesWithinColumn: unhappy path" {
    var fields1 = [_]Field{
        .{ .key = "level", .value = "warn" },
        .{ .key = "app", .value = "seq" },
    };
    var fields2 = [_]Field{
        .{ .key = "level", .value = "info" },
        .{ .key = "app", .value = "seq" },
    };
    var lines = [_]Line{
        .{
            .timestampNs = 1,
            .fields = fields1[0..],
        },
        .{
            .timestampNs = 2,
            .fields = fields2[0..],
        },
    };

    try testing.expectEqual(false, canBeSavedAsInvariant(&lines, 0));
    try testing.expectEqual(true, canBeSavedAsInvariant(&lines, 1));
}

test "BlockInitMaxColumns" {
    const Case = struct {
        lines: usize,
        fieldsPerLine: usize,
        expectedLen: u32,
        expectedColumns: usize,
    };
    const cases = [_]Case{
        .{
            .lines = 10,
            .fieldsPerLine = 300,
            .expectedLen = 6,
            .expectedColumns = 1800,
        },
        .{
            .lines = maxColumns + 1,
            .fieldsPerLine = 1,
            .expectedLen = maxColumns,
            .expectedColumns = maxColumns,
        },
    };
    for (cases) |case| {
        const alloc = testing.allocator;
        const lines = try alloc.alloc(Line, case.lines);

        var keyNum: usize = 0;
        defer {
            for (lines) |l| {
                for (l.fields) |f| {
                    alloc.free(f.key);
                    alloc.free(f.value);
                }
                alloc.free(l.fields);
            }
            alloc.free(lines);
        }
        for (0..lines.len) |i| {
            const fields = try alloc.alloc(Field, case.fieldsPerLine);
            for (0..fields.len) |j| {
                fields[j].key = try std.fmt.allocPrint(alloc, "key_{d}", .{keyNum});
                fields[j].value = try std.fmt.allocPrint(alloc, "value_{d}", .{keyNum});
                keyNum += 1;
            }
            lines[i] = Line{
                .fields = fields,
                .timestampNs = 1,
            };
        }
        var b = try Block.initFromLines(alloc, lines);
        defer b.deinit(alloc);

        try testing.expectEqual(case.expectedLen, b.len());
        try testing.expectEqual(case.expectedColumns, b.columns.len);
    }
}

test "initFromLines allows maxLines" {
    const alloc = testing.allocator;

    var field = [_]Field{.{ .key = "level", .value = "info" }};
    var lines: [maxLines]Line = undefined;
    for (0..maxLines) |i| {
        lines[i] = .{
            .timestampNs = @intCast(i),
            .fields = &field,
        };
    }

    var b = try Block.initFromLines(alloc, &lines);
    defer b.deinit(alloc);

    try testing.expectEqual(maxLines, b.len());
    try testing.expectEqual(0, b.getColumns().len);
    try testing.expectEqual(1, b.getInvariantColumns().len);
}

test "initFromLines drops a single invalid row with too many fields" {
    const alloc = testing.allocator;

    const fields = try alloc.alloc(Field, maxColumns + 1);
    defer {
        for (fields) |field| {
            alloc.free(field.key);
        }
        alloc.free(fields);
    }

    for (fields, 0..) |*field, i| {
        field.* = .{
            .key = try std.fmt.allocPrint(alloc, "key_{d}", .{i}),
            .value = "value",
        };
    }

    var lines = [_]Line{.{ .timestampNs = 1, .fields = fields }};

    var b = try Block.initFromLines(alloc, &lines);
    defer b.deinit(alloc);

    try testing.expectEqual(0, b.len());
    try testing.expectEqual(0, b.columns.len);
}

test "Block.put" {
    const allocator = testing.allocator;

    const Case = struct {
        lines: []Line,
        expectedTimestamps: []const u64,
        expectedCols: []const Column,
        expectedInvariants: []const Column,
    };

    const expectedInvariants1 = blk: {
        var appVal = [_][]const u8{"seq"};
        var levelVal = [_][]const u8{"info"};
        var invariants = [_]Column{
            .{ .key = "app", .values = appVal[0..] },
            .{ .key = "level", .values = levelVal[0..] },
        };
        break :blk &invariants;
    };
    const linesArray = blk: {
        var fields1 = [_]Field{
            .{ .key = "level", .value = "info" },
            .{ .key = "app", .value = "seq" },
        };
        var fields2 = [_]Field{
            .{ .key = "level", .value = "info" },
            .{ .key = "app", .value = "seq" },
        };
        var arr = [_]Line{ .{
            .timestampNs = 100,
            .fields = &fields1,
        }, .{
            .timestampNs = 200,
            .fields = &fields2,
        } };
        break :blk &arr;
    };
    const expectedCols2 = blk: {
        var levelVal = [_][]const u8{ "info", "warn", "error" };
        var cols = [_]Column{
            .{ .key = "level", .values = levelVal[0..] },
        };
        break :blk &cols;
    };
    const expectedInvariants2 = blk: {
        var appVal = [_][]const u8{"seq"};
        var levelVal = [_][]const u8{"server1"};
        var invariants = [_]Column{
            .{ .key = "app", .values = appVal[0..] },
            .{ .key = "host", .values = levelVal[0..] },
        };
        break :blk &invariants;
    };
    const linesArray2 = blk: {
        var fields1 = [_]Field{
            .{ .key = "level", .value = "info" },
            .{ .key = "app", .value = "seq" },
            .{ .key = "host", .value = "server1" },
        };
        var fields2 = [_]Field{
            .{ .key = "level", .value = "warn" },
            .{ .key = "app", .value = "seq" },
            .{ .key = "host", .value = "server1" },
        };
        var fields3 = [_]Field{
            .{ .key = "level", .value = "error" },
            .{ .key = "app", .value = "seq" },
            .{ .key = "host", .value = "server1" },
        };
        var lines = [_]Line{
            .{
                .timestampNs = 100,
                .fields = fields1[0..],
            },
            .{
                .timestampNs = 200,
                .fields = fields2[0..],
            },
            .{
                .timestampNs = 300,
                .fields = fields3[0..],
            },
        };
        break :blk &lines;
    };
    const linesArray3 = blk: {
        var fields1 = [_]Field{
            .{ .key = "level", .value = "info" },
            .{ .key = "app", .value = "seq" },
        };
        var fields2 = [_]Field{
            .{ .key = "cpu", .value = "0.8" },
            .{ .key = "memory", .value = "512MB" },
        };
        var lines = [_]Line{
            .{
                .timestampNs = 100,
                .fields = fields1[0..],
            },
            .{
                .timestampNs = 200,
                .fields = fields2[0..],
            },
        };
        break :blk &lines;
    };
    const expectedCols3 = blk: {
        var appVal = [_][]const u8{ "seq", "" };
        var levelVal = [_][]const u8{ "info", "" };
        var cpuVal = [_][]const u8{ "", "0.8" };
        var memVal = [_][]const u8{ "", "512MB" };
        var cols = [_]Column{
            .{ .key = "app", .values = appVal[0..] },
            .{ .key = "cpu", .values = cpuVal[0..] },
            .{ .key = "level", .values = levelVal[0..] },
            .{ .key = "memory", .values = memVal[0..] },
        };
        break :blk &cols;
    };
    const linesArray4 = blk: {
        var fields1 = [_]Field{
            .{ .key = "level", .value = "info" },
            .{ .key = "app", .value = "seq" },
            .{ .key = "host", .value = "server1" },
        };
        var fields2 = [_]Field{
            .{ .key = "level", .value = "warn" },
            .{ .key = "cpu", .value = "1" },
        };
        var fields3 = [_]Field{
            .{ .key = "app", .value = "seq" },
            .{ .key = "memory", .value = "512MB" },
        };
        var lines = [_]Line{
            .{
                .timestampNs = 100,
                .fields = fields1[0..],
            },
            .{
                .timestampNs = 200,
                .fields = fields2[0..],
            },
            .{
                .timestampNs = 300,
                .fields = fields3[0..],
            },
        };
        break :blk &lines;
    };
    const expectedCols4 = blk: {
        var levelVal = [_][]const u8{ "info", "warn", "" };
        var appVal = [_][]const u8{ "seq", "", "seq" };
        var cpuVal = [_][]const u8{ "", "1", "" };
        var hostVal = [_][]const u8{ "server1", "", "" };
        var memVal = [_][]const u8{ "", "", "512MB" };
        var cols = [_]Column{
            .{ .key = "app", .values = appVal[0..] },
            .{ .key = "cpu", .values = cpuVal[0..] },
            .{ .key = "host", .values = hostVal[0..] },
            .{ .key = "level", .values = levelVal[0..] },
            .{ .key = "memory", .values = memVal[0..] },
        };
        break :blk &cols;
    };
    const linesArray5 = blk: {
        // a large value that exceeds maxInvariantColumnValueSize
        // TODO: audit undefined usage if it's possible to avoid them on empty buffers
        var largeValue: [300]u8 = undefined;
        @memset(&largeValue, 'x');
        var fields1 = [_]Field{
            .{ .key = "level", .value = "info" },
            .{ .key = "message", .value = &largeValue },
        };
        var fields2 = [_]Field{
            .{ .key = "level", .value = "info" },
            .{ .key = "message", .value = &largeValue },
        };
        var lines = [_]Line{
            .{
                .timestampNs = 100,
                .fields = fields1[0..],
            },
            .{
                .timestampNs = 200,
                .fields = fields2[0..],
            },
        };
        break :blk &lines;
    };
    const expectedCols5 = blk: {
        const longValue = linesArray5[0].fields[1].value;
        var appVal = [_][]const u8{ longValue, longValue };
        var cols = [_]Column{
            .{ .key = "message", .values = appVal[0..] },
        };
        break :blk &cols;
    };
    const expectedInvariants5 = blk: {
        var levelVal = [_][]const u8{"info"};
        var invariants = [_]Column{
            .{ .key = "level", .values = levelVal[0..] },
        };
        break :blk &invariants;
    };

    const cases = [_]Case{
        .{
            .lines = linesArray,
            .expectedTimestamps = &[_]u64{ 100, 200 },
            .expectedCols = &[_]Column{},
            .expectedInvariants = expectedInvariants1,
        },
        .{
            .lines = linesArray2,
            .expectedTimestamps = &[_]u64{ 100, 200, 300 },
            .expectedCols = expectedCols2,
            .expectedInvariants = expectedInvariants2,
        },
        .{
            .lines = linesArray3,
            .expectedTimestamps = &[_]u64{ 100, 200 },
            .expectedCols = expectedCols3,
            .expectedInvariants = &[_]Column{},
        },
        .{
            .lines = linesArray4,
            .expectedTimestamps = &[_]u64{ 100, 200, 300 },
            .expectedCols = expectedCols4,
            .expectedInvariants = &[_]Column{},
        },
        .{
            .lines = linesArray5,
            .expectedTimestamps = &[_]u64{ 100, 200 },
            .expectedCols = expectedCols5,
            .expectedInvariants = expectedInvariants5,
        },
    };

    for (cases) |case| {
        var block = try Block.initFromLines(allocator, case.lines);
        defer block.deinit(allocator);

        for (case.expectedTimestamps, 0..) |expectedTs, i| {
            try testing.expectEqual(expectedTs, block.timestamps[i]);
        }

        const actualCols = block.getColumns();
        try testing.expectEqual(case.expectedCols.len, actualCols.len);
        for (case.expectedCols, 0..) |expectedCol, i| {
            try testing.expectEqualStrings(expectedCol.key, actualCols[i].key);
            try testing.expectEqual(expectedCol.values.len, actualCols[i].values.len);
            for (expectedCol.values, 0..) |expectedVal, j| {
                try testing.expectEqualStrings(expectedVal, actualCols[i].values[j]);
            }
        }

        const actualInvariants = block.getInvariantColumns();
        try testing.expectEqual(case.expectedInvariants.len, actualInvariants.len);
        for (case.expectedInvariants, 0..) |expectedInvariant, i| {
            try testing.expectEqualStrings(expectedInvariant.key, actualInvariants[i].key);
            try testing.expectEqual(expectedInvariant.values.len, actualInvariants[i].values.len);
            for (expectedInvariant.values, 0..) |expectedVal, j| {
                try testing.expectEqualStrings(expectedVal, actualInvariants[i].values[j]);
            }
        }
    }
}
