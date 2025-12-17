const std = @import("std");

const Field = @import("../lines.zig").Field;
const Line = @import("../lines.zig").Line;
const Column = @import("Column.zig");

const sizing = @import("sizing.zig");

const maxColumns = 2000;

fn columnLessThan(_: void, one: Column, another: Column) bool {
    return std.mem.lessThan(u8, one.key, another.key);
}

const Self = @This();

firstCelled: u32,
columns: []Column,
timestamps: []u64,

pub fn init(allocator: std.mem.Allocator, lines: []*const Line) !*Self {
    const b = try allocator.create(Self);
    errdefer allocator.destroy(b);

    b.* = Self{
        .firstCelled = undefined,
        .columns = undefined,
        .timestamps = undefined,
    };

    try b.put(allocator, lines);
    std.debug.assert(b.timestamps.len <= maxColumns);
    b.sort();
    return b;
}

pub fn deinit(self: *Self, allocator: std.mem.Allocator) void {
    for (self.columns) |col| {
        allocator.free(col.values);
    }
    allocator.free(self.columns);
    allocator.free(self.timestamps);
    allocator.destroy(self);
}

pub inline fn getColumns(self: *const Self) []Column {
    return self.columns[0..self.firstCelled];
}
// celledColumns hold columns with a single value
pub inline fn getCelledColumns(self: *const Self) []Column {
    return self.columns[self.firstCelled..];
}

pub inline fn len(self: *Self) usize {
    return self.timestamps.len;
}

pub fn size(self: *Self) u32 {
    return sizing.blockJsonSize(self);
}

fn put(self: *Self, allocator: std.mem.Allocator, lines: []*const Line) !void {
    std.debug.assert(lines.len > 0);

    // Fast path if all lines have the same fields
    if (areSameFields(lines)) {
        return self.putSameFields(allocator, lines);
    }

    return self.putDynamicFields(allocator, lines);
}

fn putSameFields(self: *Self, allocator: std.mem.Allocator, lines: []*const Line) !void {
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
    // TODO: Use fixed buffer allocator (1-2kb)
    // First pass: identify which columns are celled
    var celledMask = try allocator.alloc(bool, firstLine.fields.len);
    defer allocator.free(celledMask);

    var celledCount: usize = 0;
    for (0..firstLine.fields.len) |fieldIdx| {
        if (canBeSavedAsCelled(lines, fieldIdx)) {
            celledMask[fieldIdx] = true;
            celledCount += 1;
        } else {
            celledMask[fieldIdx] = false;
        }
    }

    // Second pass: populate columns with regular columns first, then celled
    var regularIdx: usize = 0;
    var celledIdx: usize = firstLine.fields.len - celledCount;

    errdefer {
        for (columns) |col| {
            if (col.values.len != 0) {
                allocator.free(col.values);
            }
        }
    }
    for (firstLine.fields, 0..) |field, fieldIdx| {
        const isFieldCelled = celledMask[fieldIdx];
        const targetIdx = if (isFieldCelled) celledIdx else regularIdx;
        var col = &columns[targetIdx];
        col.key = field.key;

        if (isFieldCelled) {
            col.values = try allocator.alloc([]const u8, 1);
            col.values[0] = field.value;
            celledIdx += 1;
        } else {
            col.values = try allocator.alloc([]const u8, lines.len);
            for (lines, 0..) |line, lineIdx| {
                col.values[lineIdx] = line.fields[fieldIdx].value;
            }
            regularIdx += 1;
        }
    }

    self.firstCelled = @intCast(firstLine.fields.len - celledCount);

    self.columns = columns;
}

fn putDynamicFields(self: *Self, allocator: std.mem.Allocator, lines: []*const Line) !void {
    // Builds hash map of unique column keys to their index
    var columnI = std.StringHashMap(usize).init(allocator);
    defer columnI.deinit();
    var linesProcessed = lines;
    for (lines, 0..) |line, i| {
        const uniqueKeysCount = columnI.count() + line.fields.len;
        if (uniqueKeysCount > maxColumns) {
            std.debug.print(
                "skipping log line, exceeded max allowed unique keys: max={d},given={d}\n",
                .{ maxColumns, uniqueKeysCount },
            );
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

    self.firstCelled = @intCast(columns.len);
    var i: usize = 0;
    while (i < self.firstCelled) {
        if (columns[i].isCelled()) {
            self.firstCelled -= 1;
            std.mem.swap(Column, &columns[i], &columns[self.firstCelled]);
        } else {
            i += 1;
        }
    }

    self.columns = columns;
}

fn sort(self: *Self) void {
    std.mem.sortUnstable(Column, self.getColumns(), {}, columnLessThan);
    std.mem.sortUnstable(Column, self.getCelledColumns(), {}, columnLessThan);
}

// TODO: Investigate if we need to check for unique/duplicated fields keys as well.
fn areSameFields(lines: []*const Line) bool {
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

fn canBeSavedAsCelled(lines: []*const Line, index: usize) bool {
    // If len is zero, then there's nothing to do.
    if (lines.len == 0) {
        return true;
    }

    const value = lines[0].fields[index].value;

    // If value is too large, then we consider it not celled.
    // Not sure if this would work though?
    if (value.len > Column.maxCelledColumnValueSize) {
        return false;
    }

    for (lines[1..]) |line| {
        if (std.mem.eql(u8, line.fields[index].value, value) == false) {
            return false;
        }
    }

    return true;
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
    var lines = [_]*const Line{
        &.{
            .timestampNs = 1,
            .sid = undefined,
            .fields = fields1[0..],
            .encodedTags = undefined,
        },
        &.{
            .timestampNs = 2,
            .sid = undefined,
            .fields = fields2[0..],
            .encodedTags = undefined,
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
    var lines = [_]*const Line{
        &.{
            .timestampNs = 1,
            .sid = undefined,
            .fields = fields1[0..],
            .encodedTags = undefined,
        },
        &.{
            .timestampNs = 2,
            .sid = undefined,
            .fields = fields2[0..],
            .encodedTags = undefined,
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
    var lines = [_]*const Line{
        &.{
            .timestampNs = 1,
            .sid = undefined,
            .fields = fields1[0..],
            .encodedTags = undefined,
        },
        &.{
            .timestampNs = 2,
            .sid = undefined,
            .fields = fields2[0..],
            .encodedTags = undefined,
        },
    };

    try std.testing.expectEqual(true, canBeSavedAsCelled(&lines, 0));
    try std.testing.expectEqual(true, canBeSavedAsCelled(&lines, 1));
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
    var lines = [_]*const Line{
        &.{
            .timestampNs = 1,
            .sid = undefined,
            .fields = fields1[0..],
            .encodedTags = undefined,
        },
        &.{
            .timestampNs = 2,
            .sid = undefined,
            .fields = fields2[0..],
            .encodedTags = undefined,
        },
    };

    try std.testing.expectEqual(false, canBeSavedAsCelled(&lines, 0));
    try std.testing.expectEqual(true, canBeSavedAsCelled(&lines, 1));
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
        const lines = try alloc.alloc(*const Line, case.lines);

        var keyNum: usize = 0;
        defer {
            for (lines) |l| {
                for (l.fields) |f| {
                    alloc.free(f.key);
                    alloc.free(f.value);
                }
                alloc.free(l.fields);
                alloc.destroy(l);
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
            const line = try alloc.create(Line);
            line.* = Line{
                .fields = fields,
                .encodedTags = undefined,
                .sid = undefined,
                .timestampNs = 1,
            };
            lines[i] = line;
        }
        const b = try Self.init(alloc, lines);
        defer b.deinit(alloc);

        try std.testing.expectEqual(case.expectedLen, b.len());
    }
}

test "Self.put" {
    const allocator = std.testing.allocator;

    const Case = struct {
        lines: []*const Line,
        expectedTimestamps: []const u64,
        expectedCols: []const Column,
        expectedCells: []const Column,
    };

    const expectedCells1 = blk: {
        var appVal = [_][]const u8{"seq"};
        var levelVal = [_][]const u8{"info"};
        var cells = [_]Column{
            .{ .key = "app", .values = appVal[0..] },
            .{ .key = "level", .values = levelVal[0..] },
        };
        break :blk &cells;
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
        const line1 = Line{
            .timestampNs = 100,
            .sid = undefined,
            .fields = &fields1,
            .encodedTags = undefined,
        };
        const line2 = Line{
            .timestampNs = 200,
            .sid = undefined,
            .fields = &fields2,
            .encodedTags = undefined,
        };
        var arr = [_]*const Line{ &line1, &line2 };
        break :blk &arr;
    };
    const expectedCols2 = blk: {
        var levelVal = [_][]const u8{ "info", "warn", "error" };
        var cols = [_]Column{
            .{ .key = "level", .values = levelVal[0..] },
        };
        break :blk &cols;
    };
    const expectedCells2 = blk: {
        var appVal = [_][]const u8{"seq"};
        var levelVal = [_][]const u8{"server1"};
        var cells = [_]Column{
            .{ .key = "app", .values = appVal[0..] },
            .{ .key = "host", .values = levelVal[0..] },
        };
        break :blk &cells;
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
        var lines = [_]*const Line{
            &.{
                .timestampNs = 100,
                .sid = undefined,
                .fields = fields1[0..],
                .encodedTags = undefined,
            },
            &.{
                .timestampNs = 200,
                .sid = undefined,
                .fields = fields2[0..],
                .encodedTags = undefined,
            },
            &.{
                .timestampNs = 300,
                .sid = undefined,
                .fields = fields3[0..],
                .encodedTags = undefined,
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
        var lines = [_]*const Line{
            &.{
                .timestampNs = 100,
                .sid = undefined,
                .fields = fields1[0..],
                .encodedTags = undefined,
            },
            &.{
                .timestampNs = 200,
                .sid = undefined,
                .fields = fields2[0..],
                .encodedTags = undefined,
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
        var lines = [_]*const Line{
            &.{
                .timestampNs = 100,
                .sid = undefined,
                .fields = fields1[0..],
                .encodedTags = undefined,
            },
            &.{
                .timestampNs = 200,
                .sid = undefined,
                .fields = fields2[0..],
                .encodedTags = undefined,
            },
            &.{
                .timestampNs = 300,
                .sid = undefined,
                .fields = fields3[0..],
                .encodedTags = undefined,
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
        // a large value that exceeds maxCelledColumnValueSize
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
        var lines = [_]*const Line{
            &.{
                .timestampNs = 100,
                .sid = undefined,
                .fields = fields1[0..],
                .encodedTags = undefined,
            },
            &.{
                .timestampNs = 200,
                .sid = undefined,
                .fields = fields2[0..],
                .encodedTags = undefined,
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
    const expectedCells5 = blk: {
        var levelVal = [_][]const u8{"info"};
        var cells = [_]Column{
            .{ .key = "level", .values = levelVal[0..] },
        };
        break :blk &cells;
    };

    const cases = [_]Case{
        .{
            .lines = linesArray,
            .expectedTimestamps = &[_]u64{ 100, 200 },
            .expectedCols = &[_]Column{},
            .expectedCells = expectedCells1,
        },
        .{
            .lines = linesArray2,
            .expectedTimestamps = &[_]u64{ 100, 200, 300 },
            .expectedCols = expectedCols2,
            .expectedCells = expectedCells2,
        },
        .{
            .lines = linesArray3,
            .expectedTimestamps = &[_]u64{ 100, 200 },
            .expectedCols = expectedCols3,
            .expectedCells = &[_]Column{},
        },
        .{
            .lines = linesArray4,
            .expectedTimestamps = &[_]u64{ 100, 200, 300 },
            .expectedCols = expectedCols4,
            .expectedCells = &[_]Column{},
        },
        .{
            .lines = linesArray5,
            .expectedTimestamps = &[_]u64{ 100, 200 },
            .expectedCols = expectedCols5,
            .expectedCells = expectedCells5,
        },
    };

    for (cases) |case| {
        var block = try Self.init(allocator, case.lines);
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

        const actualCells = block.getCelledColumns();
        try std.testing.expectEqual(case.expectedCells.len, actualCells.len);
        for (case.expectedCells, 0..) |expectedCell, i| {
            try std.testing.expectEqualStrings(expectedCell.key, actualCells[i].key);
            try std.testing.expectEqual(expectedCell.values.len, actualCells[i].values.len);
            for (expectedCell.values, 0..) |expectedVal, j| {
                try std.testing.expectEqualStrings(expectedVal, actualCells[i].values[j]);
            }
        }
    }
}
