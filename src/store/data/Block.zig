const std = @import("std");
const Allocator = std.mem.Allocator;
const Io = std.Io;

const Field = @import("../lines.zig").Field;
const Line = @import("../lines.zig").Line;
const Column = @import("Column.zig");
const BlockData = @import("BlockData.zig").BlockData;
const Unpacker = @import("Unpacker.zig");
const ValuesDecoder = @import("ValuesDecoder.zig");
const TimestampsEncoder = @import("TimestampsEncoder.zig");
const Table = @import("../data/Table.zig");
const Logger = @import("logging");

const tracy = @import("tracy");

const sizing = @import("sizing.zig");

pub const maxColumns = 2000;
// at least 1 line fits a block with a large gap
// maxLines is a max amount of lines that we can put into a block,
// it's mostly for sanity check assuming the maxBlockSize
// TODO: we should log blocks meta data if there are more than 64 * 1024 lines
// it requires make it as a soft limit,
// it means every line is less than 32 bytes
pub const maxLines = 1024 * 1024;

comptime {
    std.debug.assert(@import("../lines.zig").defaultMaxFieldsPerLine * 2 == maxColumns);
}

fn columnLessThan(_: void, one: Column, another: Column) bool {
    return std.mem.lessThan(u8, one.key, another.key);
}

const Block = @This();

firstInvariant: u32,
columns: []Column,
timestamps: []u64,

pub fn initFromLines(allocator: Allocator, lines: []const Line) !*Block {
    const b = try allocator.create(Block);
    errdefer allocator.destroy(b);

    b.* = .{
        .firstInvariant = undefined,
        .columns = undefined,
        .timestamps = undefined,
    };

    try b.put(allocator, lines);
    std.debug.assert(b.timestamps.len <= maxColumns);
    b.sort();
    return b;
}

pub fn initFromData(io: Io, alloc: Allocator, data: *BlockData, unpacker: *Unpacker, decoder: *ValuesDecoder) !*Block {
    const z = tracy.Zone.begin(.{
        .src = @src(),
        .name = "data.Block.initFromData",
    });
    defer z.end();
    std.debug.assert(data.len <= maxLines);

    const tsEncoder = try TimestampsEncoder.init(alloc);
    defer tsEncoder.deinit(alloc);

    const tss = try alloc.alloc(u64, data.len);
    errdefer alloc.free(tss);
    try tsEncoder.decode(tss, data.timestampsData.data);

    const firstInvariant: u32 = @intCast(data.columnsData.items.len);
    const invariantColsLen = if (data.invariantColumns) |invariants| invariants.len else 0;
    const columns = try alloc.alloc(Column, data.columnsData.items.len + invariantColsLen);

    for (0..data.columnsData.items.len) |i| {
        const colData = &data.columnsData.items[i];
        var col = &columns[i];
        col.key = colData.key;
        col.values = try unpacker.unpackValues(alloc, colData.bloomValues, data.len);
        try decoder.decode(io, col.values, colData.type, colData.dict.values.items);
    }

    if (data.invariantColumns) |invariants| {
        for (invariants, 0..) |*invariant, i| {
            columns[firstInvariant + i].key = invariant.key;
            // move the values to the block instead of copying them
            columns[firstInvariant + i].values = &[_][]const u8{};
            std.mem.swap([][]const u8, &columns[firstInvariant + i].values, &invariant.values);
        }
    }

    const b = try alloc.create(Block);

    b.* = .{
        .firstInvariant = firstInvariant,
        .columns = columns,
        .timestamps = tss,
    };
    return b;
}

pub fn gatherLines(self: *const Block, alloc: Allocator, lines: *std.ArrayList(Line)) !void {
    const cols = self.getColumns();
    const invariants = self.getInvariantColumns();

    try lines.ensureUnusedCapacity(alloc, self.timestamps.len);

    const initialLen = lines.items.len;
    var appendedCount: usize = 0;
    errdefer {
        for (lines.items[initialLen .. initialLen + appendedCount]) |line| alloc.free(line.fields);
        lines.shrinkRetainingCapacity(initialLen);
    }

    for (self.timestamps, 0..) |ts, i| {
        var fieldCount: usize = invariants.len;
        for (cols) |col| {
            if (col.values[i].len > 0) fieldCount += 1;
        }

        const fields = try alloc.alloc(Field, fieldCount);
        errdefer alloc.free(fields);
        var fi: usize = 0;

        for (invariants) |invariant| {
            fields[fi] = .{ .key = invariant.key, .value = invariant.values[0] };
            fi += 1;
        }
        for (cols) |col| {
            const value = col.values[i];
            if (value.len == 0) continue;
            fields[fi] = .{ .key = col.key, .value = value };
            fi += 1;
        }

        lines.appendAssumeCapacity(.{
            .timestampNs = ts,
            .fields = fields,
        });
        appendedCount += 1;
    }
}

pub fn deinit(self: *Block, allocator: Allocator) void {
    for (self.columns) |col| {
        allocator.free(col.values);
    }
    allocator.free(self.columns);
    allocator.free(self.timestamps);
    allocator.destroy(self);
}

pub fn getColumns(self: *const Block) []Column {
    return self.columns[0..self.firstInvariant];
}
// getInvariantColumns gives columns with a single value
pub fn getInvariantColumns(self: *const Block) []Column {
    return self.columns[self.firstInvariant..];
}

pub fn len(self: *Block) usize {
    return self.timestamps.len;
}

pub fn size(self: *Block) u32 {
    return sizing.blockJsonSize(self);
}

fn put(self: *Block, allocator: Allocator, lines: []const Line) !void {
    std.debug.assert(lines.len > 0);

    // Fast path if all lines have the same fields
    if (areSameFields(lines)) {
        return self.putSameFields(allocator, lines);
    }

    return self.putDynamicFields(allocator, lines);
}

fn putSameFields(self: *Block, allocator: Allocator, lines: []const Line) !void {
    self.timestamps = try allocator.alloc(u64, lines.len);
    errdefer allocator.free(self.timestamps);
    for (lines, 0..) |line, i| {
        self.timestamps[i] = line.timestampNs;
    }

    const firstLine = lines[0];
    var columns = try allocator.alloc(Column, firstLine.fields.len);
    errdefer allocator.free(columns);

    @memset(columns, .{ .key = "", .values = &[_][]const u8{} });

    // TODO: Compare with bitset instead of bool array?
    // First pass: identify which columns are invariant
    var invariantMaskBuffer: [maxColumns]bool = undefined;
    var invariantMask = invariantMaskBuffer[0..firstLine.fields.len];

    var invariantCount: usize = 0;
    for (0..firstLine.fields.len) |fieldIdx| {
        if (canBeSavedAsInvariant(lines, fieldIdx)) {
            invariantMask[fieldIdx] = true;
            invariantCount += 1;
        } else {
            invariantMask[fieldIdx] = false;
        }
    }

    // Second pass: populate columns with regular columns first, then invariant
    var regularIdx: usize = 0;
    var invariantIdx: usize = firstLine.fields.len - invariantCount;

    errdefer {
        for (columns) |col| {
            if (col.values.len != 0) {
                allocator.free(col.values);
            }
        }
    }
    for (firstLine.fields, 0..) |field, fieldIdx| {
        const isFieldInvariant = invariantMask[fieldIdx];
        const targetIdx = if (isFieldInvariant) invariantIdx else regularIdx;
        var col = &columns[targetIdx];
        col.key = field.key;

        if (isFieldInvariant) {
            col.values = try allocator.alloc([]const u8, 1);
            col.values[0] = field.value;
            invariantIdx += 1;
        } else {
            col.values = try allocator.alloc([]const u8, lines.len);
            for (lines, 0..) |line, lineIdx| {
                col.values[lineIdx] = line.fields[fieldIdx].value;
            }
            regularIdx += 1;
        }
    }

    self.firstInvariant = @intCast(firstLine.fields.len - invariantCount);

    self.columns = columns;
}

fn putDynamicFields(self: *Block, allocator: Allocator, lines: []const Line) !void {
    // Builds hash map of unique column keys to their index
    var columnI = std.StringHashMap(usize).init(allocator);
    defer columnI.deinit();
    var linesProcessed = lines;
    for (lines, 0..) |line, i| {
        // TODO: better to move out of the block in order to handle more keys
        const uniqueKeysCount = columnI.count() + line.fields.len;
        if (uniqueKeysCount > maxColumns) {
            Logger.log(.warn, "skipping log line, exceeded max allowed unique keys", .{
                .max = maxColumns,
                .given = uniqueKeysCount,
            });
            linesProcessed = lines[0..i];
            break;
        }

        for (line.fields) |field| {
            if (!columnI.contains(field.key)) {
                try columnI.put(field.key, columnI.count());
            }
        }
    }
    const timestamps = try allocator.alloc(u64, linesProcessed.len);
    errdefer allocator.free(timestamps);
    for (0..linesProcessed.len) |i| {
        timestamps[i] = linesProcessed[i].timestampNs;
    }
    self.timestamps = timestamps;

    var columns = try allocator.alloc(Column, columnI.count());
    errdefer allocator.free(columns);

    @memset(columns, .{ .key = "", .values = &[_][]u8{} });
    errdefer {
        for (columns) |col| {
            if (col.values.len != 0) {
                allocator.free(col.values);
            }
        }
    }

    var columnIter = columnI.iterator();
    while (columnIter.next()) |entry| {
        const key = entry.key_ptr.*;
        const idx = entry.value_ptr.*;

        var col = &columns[idx];
        col.key = key;
        col.values = try allocator.alloc([]const u8, linesProcessed.len);
        @memset(col.values, "");
    }

    for (linesProcessed, 0..) |line, i| {
        for (line.fields) |field| {
            const idx = columnI.get(field.key).?;
            columns[idx].values[i] = field.value;
        }
    }

    self.firstInvariant = @intCast(columns.len);
    var i: usize = 0;
    while (i < self.firstInvariant) {
        if (columns[i].isInvariant()) {
            self.firstInvariant -= 1;
            std.mem.swap(Column, &columns[i], &columns[self.firstInvariant]);
        } else {
            i += 1;
        }
    }

    self.columns = columns;
}

fn sort(self: *Block) void {
    std.sort.pdq(Column, self.getColumns(), {}, columnLessThan);
    std.sort.pdq(Column, self.getInvariantColumns(), {}, columnLessThan);
}

// TODO: Investigate if we need to check for unique/duplicated fields keys as well.
fn areSameFields(lines: []const Line) bool {
    if (lines.len < 2) {
        return true;
    }

    const firstLine = lines[0];
    for (lines[1..]) |line| {
        if (line.fields.len != firstLine.fields.len) {
            return false;
        }

        for (firstLine.fields, 0..) |field, i| {
            if (!std.mem.eql(u8, field.key, line.fields[i].key)) {
                return false;
            }
        }
    }

    return true;
}

fn canBeSavedAsInvariant(lines: []const Line, index: usize) bool {
    // If len is zero, then there's nothing to do.
    if (lines.len == 0) {
        return true;
    }

    const value = lines[0].fields[index].value;

    // If value is too large, then we consider it not invariant.
    // Not sure if this would work though?
    if (value.len > Column.maxInvariantColumnValueSize) {
        return false;
    }

    for (lines[1..]) |line| {
        if (std.mem.eql(u8, line.fields[index].value, value) == false) {
            return false;
        }
    }

    return true;
}

pub fn assert(self: *const Block) void {
    const timestampsAreSorted = std.sort.isSorted(u64, self.timestamps, {}, std.sort.asc(u64));
    std.debug.assert(timestampsAreSorted);

    for (self.getColumns()) |col| {
        std.debug.assert(col.values.len == self.timestamps.len);
    }
}

const SID = @import("../lines.zig").SID;
const TableWriter = @import("TableWriter.zig");
const MemTable = @import("MemTable.zig");
const TableReader = @import("TableReader.zig");
const BlockHeader = @import("BlockHeader.zig");

fn expectEqualBlocks(a: *const Block, b: *const Block) !void {
    try std.testing.expectEqualSlices(u64, a.timestamps, b.timestamps);
    try std.testing.expectEqual(a.firstInvariant, b.firstInvariant);

    const colsA = a.getColumns();
    const colsB = b.getColumns();
    try std.testing.expectEqual(colsA.len, colsB.len);
    for (colsA, colsB) |ca, cb| {
        try std.testing.expectEqualStrings(ca.key, cb.key);
        try std.testing.expectEqual(ca.values.len, cb.values.len);
        for (ca.values, cb.values) |va, vb| {
            try std.testing.expectEqualStrings(va, vb);
        }
    }

    const invariantA = a.getInvariantColumns();
    const invariantB = b.getInvariantColumns();
    try std.testing.expectEqual(invariantA.len, invariantB.len);
    for (invariantA, invariantB) |ca, cb| {
        try std.testing.expectEqualStrings(ca.key, cb.key);
        try std.testing.expectEqual(ca.values.len, 1);
        try std.testing.expectEqual(cb.values.len, 1);
        try std.testing.expectEqualStrings(ca.values[0], cb.values[0]);
    }
}

test "initFromLines and initFromData produce identical blocks" {
    const alloc = std.testing.allocator;
    const io = std.testing.io;

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
        const blockA = try Block.initFromLines(alloc, case.lines);
        defer blockA.deinit(alloc);

        const memTable = try MemTable.init(alloc);
        const table = try Table.fromMem(alloc, memTable);
        defer table.close(io);

        const writer = try TableWriter.initMem(alloc, memTable);
        defer writer.deinit(alloc);

        var bh = BlockHeader.initFromBlock(blockA, sid);
        try writer.writeBlock(io, alloc, blockA, &bh);

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

        const unpacker = try Unpacker.init(alloc);
        const decoder = try ValuesDecoder.init(alloc);

        const blockB = try Block.initFromData(io, alloc, &bd, unpacker, decoder);
        defer blockB.deinit(alloc);
        defer unpacker.deinit(alloc);
        defer decoder.deinit();

        try expectEqualBlocks(blockA, blockB);

        var gatheredLines = std.ArrayList(Line).empty;
        defer {
            for (gatheredLines.items) |line| alloc.free(line.fields);
            gatheredLines.deinit(alloc);
        }
        try blockA.gatherLines(alloc, &gatheredLines);
        try std.testing.expectEqual(case.lines.len, gatheredLines.items.len);
        for (case.lines, gatheredLines.items) |origLine, gatheredLine| {
            try std.testing.expectEqual(origLine.timestampNs, gatheredLine.timestampNs);
            try std.testing.expectEqual(origLine.fields.len, gatheredLine.fields.len);
            for (origLine.fields, gatheredLine.fields) |of, gf| {
                try std.testing.expectEqualStrings(of.key, gf.key);
                try std.testing.expectEqualStrings(of.value, gf.value);
            }
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

    try std.testing.expectEqual(true, areSameFields(&lines));
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

    try std.testing.expectEqual(false, areSameFields(&lines));
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

    try std.testing.expectEqual(true, canBeSavedAsInvariant(&lines, 0));
    try std.testing.expectEqual(true, canBeSavedAsInvariant(&lines, 1));
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

    try std.testing.expectEqual(false, canBeSavedAsInvariant(&lines, 0));
    try std.testing.expectEqual(true, canBeSavedAsInvariant(&lines, 1));
}

test "SelfInitMaxColumns" {
    const Case = struct {
        lines: usize,
        fieldsPerLine: usize,
        expectedLen: u32,
    };
    const cases = [_]Case{
        .{
            .lines = 10,
            .fieldsPerLine = 10,
            .expectedLen = 10,
        },
        .{
            .lines = 21,
            .fieldsPerLine = 100,
            .expectedLen = 20,
        },
        .{
            .lines = 10,
            .fieldsPerLine = 300,
            .expectedLen = 6,
        },
        .{
            .lines = maxColumns + 1,
            .fieldsPerLine = 1,
            .expectedLen = maxColumns,
        },
    };
    for (cases) |case| {
        const alloc = std.testing.allocator;
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
        const b = try Block.initFromLines(alloc, lines);
        defer b.deinit(alloc);

        try std.testing.expectEqual(case.expectedLen, b.len());
    }
}

test "Self.put" {
    const allocator = std.testing.allocator;

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
            try std.testing.expectEqual(expectedTs, block.timestamps[i]);
        }

        const actualCols = block.getColumns();
        try std.testing.expectEqual(case.expectedCols.len, actualCols.len);
        for (case.expectedCols, 0..) |expectedCol, i| {
            try std.testing.expectEqualStrings(expectedCol.key, actualCols[i].key);
            try std.testing.expectEqual(expectedCol.values.len, actualCols[i].values.len);
            for (expectedCol.values, 0..) |expectedVal, j| {
                try std.testing.expectEqualStrings(expectedVal, actualCols[i].values[j]);
            }
        }

        const actualInvariants = block.getInvariantColumns();
        try std.testing.expectEqual(case.expectedInvariants.len, actualInvariants.len);
        for (case.expectedInvariants, 0..) |expectedInvariant, i| {
            try std.testing.expectEqualStrings(expectedInvariant.key, actualInvariants[i].key);
            try std.testing.expectEqual(expectedInvariant.values.len, actualInvariants[i].values.len);
            for (expectedInvariant.values, 0..) |expectedVal, j| {
                try std.testing.expectEqualStrings(expectedVal, actualInvariants[i].values[j]);
            }
        }
    }
}
