const std = @import("std");

const Field = @import("../lines.zig").Field;
const Line = @import("../lines.zig").Line;
const Encoder = @import("encoding").Encoder;

const sizing = @import("sizing.zig");

const maxColumns = 1000;

fn columnLessThan(_: void, one: Column, another: Column) bool {
    return std.mem.lessThan(u8, one.key, another.key);
}

pub const Column = struct {
    key: []const u8,
    values: [][]const u8,

    // makes no sense to keep large values in celled columns,
    // it won't help to improve performance
    pub const maxCelledColumnValueSize = 256;

    pub fn isCelled(self: *Column) bool {
        if (self.values.len == 0) {
            return true;
        }

        if (self.values[0].len > maxCelledColumnValueSize) {
            return false;
        }

        for (1..self.values.len) |i| {
            if (!std.mem.eql(u8, self.values[i], self.values[0])) {
                return false;
            }
        }

        return true;
    }

    pub fn encodeAsCelled(self: *Column, enc: *Encoder, encodeKey: bool) void {
        if (encodeKey) {
            enc.writeBytes(self.key);
        }
        enc.writeBytes(self.values[0]);
    }
};

pub const Block = struct {
    firstCelled: u32,
    columns: []Column,
    timestamps: []u64,

    pub fn init(allocator: std.mem.Allocator, lines: []*const Line) !*Block {
        const b = try allocator.create(Block);
        errdefer allocator.destroy(b);
        const timestamps = try allocator.alloc(u64, lines.len);
        errdefer allocator.free(timestamps);
        b.* = Block{
            .firstCelled = undefined,
            .columns = undefined,
            .timestamps = timestamps,
        };

        try b.put(allocator, lines);
        b.sort();
        return b;
    }

    pub fn deinit(self: *Block, allocator: std.mem.Allocator) void {
        for (self.columns) |col| {
            allocator.free(col.values);
        }
        allocator.free(self.columns);
        allocator.free(self.timestamps);
        allocator.destroy(self);
    }

    pub fn getColumns(self: *const Block) []Column {
        return self.columns[0..self.firstCelled];
    }
    // celledColumns hold columns with a single value
    pub fn getCelledColumns(self: *const Block) []Column {
        return self.columns[self.firstCelled..];
    }

    pub fn len(self: *Block) usize {
        return self.timestamps.len;
    }

    pub fn size(self: *Block) u32 {
        return sizing.blockJsonSize(self);
    }

    fn put(self: *Block, allocator: std.mem.Allocator, lines: []*const Line) !void {
        std.debug.assert(lines.len > 0);

        // Fast path if all lines have the same fields
        if (areSameFields(lines)) {
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
            return;
        }

        // Builds hash map of unique column keys to their index
        var columnI = std.StringHashMap(usize).init(allocator);
        defer columnI.deinit();
        for (lines, 0..) |line, i| {
            // TODO: implement maxColumns limit (1000 cols)

            for (line.fields) |field| {
                if (!columnI.contains(field.key)) {
                    try columnI.put(field.key, columnI.count());
                }
            }

            self.timestamps[i] = line.timestampNs;
        }

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
            col.values = try allocator.alloc([]const u8, lines.len);
            @memset(col.values, "");
        }

        for (lines, 0..) |line, i| {
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

    fn sort(self: *Block) void {
        if (self.len() > maxColumns) @panic("block size exceeded maxColumns");

        std.mem.sortUnstable(Column, self.getColumns(), {}, columnLessThan);
        std.mem.sortUnstable(Column, self.getCelledColumns(), {}, columnLessThan);
    }
};

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
            .sid = .{ .id = 1, .tenantID = "1234" },
            .fields = fields1[0..],
            .encodedTags = undefined,
        },
        &.{
            .timestampNs = 2,
            .sid = .{ .id = 1, .tenantID = "1234" },
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
            .sid = .{ .id = 1, .tenantID = "1234" },
            .fields = fields1[0..],
            .encodedTags = undefined,
        },
        &.{
            .timestampNs = 2,
            .sid = .{ .id = 1, .tenantID = "1234" },
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
            .sid = .{ .id = 1, .tenantID = "1234" },
            .fields = fields1[0..],
            .encodedTags = undefined,
        },
        &.{
            .timestampNs = 2,
            .sid = .{ .id = 1, .tenantID = "1234" },
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
            .sid = .{ .id = 1, .tenantID = "1234" },
            .fields = fields1[0..],
            .encodedTags = undefined,
        },
        &.{
            .timestampNs = 2,
            .sid = .{ .id = 1, .tenantID = "1234" },
            .fields = fields2[0..],
            .encodedTags = undefined,
        },
    };

    try std.testing.expectEqual(false, canBeSavedAsCelled(&lines, 0));
    try std.testing.expectEqual(true, canBeSavedAsCelled(&lines, 1));
}

test "put: fast path with same fields and all celled columns" {
    const allocator = std.testing.allocator;
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
            .timestampNs = 100,
            .sid = .{ .id = 1, .tenantID = "1234" },
            .fields = fields1[0..],
            .encodedTags = undefined,
        },
        &.{
            .timestampNs = 200,
            .sid = .{ .id = 1, .tenantID = "1234" },
            .fields = fields2[0..],
            .encodedTags = undefined,
        },
    };

    const timestamps = try allocator.alloc(u64, lines.len);
    defer allocator.free(timestamps);

    var block = Block{
        .firstCelled = undefined,
        .columns = undefined,
        .timestamps = timestamps,
    };

    try block.put(allocator, &lines);
    defer {
        for (block.columns) |col| {
            allocator.free(col.values);
        }
        allocator.free(block.columns);
    }

    // Check timestamps
    try std.testing.expectEqual(100, block.timestamps[0]);
    try std.testing.expectEqual(200, block.timestamps[1]);

    // All columns should be celled (same values)
    try std.testing.expectEqual(0, block.firstCelled);
    try std.testing.expectEqual(2, block.columns.len);

    // Check celled columns
    const celledCols = block.getCelledColumns();
    try std.testing.expectEqual(2, celledCols.len);

    // Both columns should have only 1 value stored
    for (celledCols) |col| {
        try std.testing.expectEqual(1, col.values.len);
    }
}

test "put: fast path with same fields and mixed columns" {
    const allocator = std.testing.allocator;
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
            .sid = .{ .id = 1, .tenantID = "1234" },
            .fields = fields1[0..],
            .encodedTags = undefined,
        },
        &.{
            .timestampNs = 200,
            .sid = .{ .id = 1, .tenantID = "1234" },
            .fields = fields2[0..],
            .encodedTags = undefined,
        },
        &.{
            .timestampNs = 300,
            .sid = .{ .id = 1, .tenantID = "1234" },
            .fields = fields3[0..],
            .encodedTags = undefined,
        },
    };

    const timestamps = try allocator.alloc(u64, lines.len);
    defer allocator.free(timestamps);

    var block = Block{
        .firstCelled = undefined,
        .columns = undefined,
        .timestamps = timestamps,
    };

    try block.put(allocator, &lines);
    defer {
        for (block.columns) |col| {
            allocator.free(col.values);
        }
        allocator.free(block.columns);
    }

    // Check timestamps
    try std.testing.expectEqual(100, block.timestamps[0]);
    try std.testing.expectEqual(200, block.timestamps[1]);
    try std.testing.expectEqual(300, block.timestamps[2]);

    // Should have 1 regular column (level) and 2 celled columns (app, host)
    try std.testing.expectEqual(1, block.firstCelled);
    try std.testing.expectEqual(3, block.columns.len);

    // Check regular column (level - varying values)
    const regularCols = block.getColumns();
    try std.testing.expectEqual(1, regularCols.len);
    try std.testing.expectEqual(3, regularCols[0].values.len);
    try std.testing.expectEqualStrings("level", regularCols[0].key);

    // Check celled columns (app, host - constant values)
    const celledCols = block.getCelledColumns();
    try std.testing.expectEqual(2, celledCols.len);
    for (celledCols) |col| {
        try std.testing.expectEqual(1, col.values.len);
    }
}

test "put: slow path with different fields" {
    const allocator = std.testing.allocator;
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
            .sid = .{ .id = 1, .tenantID = "1234" },
            .fields = fields1[0..],
            .encodedTags = undefined,
        },
        &.{
            .timestampNs = 200,
            .sid = .{ .id = 1, .tenantID = "1234" },
            .fields = fields2[0..],
            .encodedTags = undefined,
        },
    };

    const timestamps = try allocator.alloc(u64, lines.len);
    defer allocator.free(timestamps);

    var block = Block{
        .firstCelled = undefined,
        .columns = undefined,
        .timestamps = timestamps,
    };

    try block.put(allocator, &lines);
    defer {
        for (block.columns) |col| {
            allocator.free(col.values);
        }
        allocator.free(block.columns);
    }

    // Check timestamps
    try std.testing.expectEqual(100, block.timestamps[0]);
    try std.testing.expectEqual(200, block.timestamps[1]);

    // Should have 4 columns total (level, app, cpu, memory)
    try std.testing.expectEqual(4, block.columns.len);

    // All columns should have 2 value slots (some may be empty strings)
    for (block.columns) |col| {
        try std.testing.expectEqual(2, col.values.len);
    }
}

test "put: slow path with overlapping fields" {
    const allocator = std.testing.allocator;
    var fields1 = [_]Field{
        .{ .key = "level", .value = "info" },
        .{ .key = "app", .value = "seq" },
        .{ .key = "host", .value = "server1" },
    };
    var fields2 = [_]Field{
        .{ .key = "level", .value = "warn" },
        .{ .key = "cpu", .value = "0.9" },
    };
    var fields3 = [_]Field{
        .{ .key = "app", .value = "seq" },
        .{ .key = "memory", .value = "1GB" },
    };
    var lines = [_]*const Line{
        &.{
            .timestampNs = 100,
            .sid = .{ .id = 1, .tenantID = "1234" },
            .fields = fields1[0..],
            .encodedTags = undefined,
        },
        &.{
            .timestampNs = 200,
            .sid = .{ .id = 1, .tenantID = "1234" },
            .fields = fields2[0..],
            .encodedTags = undefined,
        },
        &.{
            .timestampNs = 300,
            .sid = .{ .id = 1, .tenantID = "1234" },
            .fields = fields3[0..],
            .encodedTags = undefined,
        },
    };

    const timestamps = try allocator.alloc(u64, lines.len);
    defer allocator.free(timestamps);

    var block = Block{
        .firstCelled = undefined,
        .columns = undefined,
        .timestamps = timestamps,
    };

    try block.put(allocator, &lines);
    defer {
        for (block.columns) |col| {
            allocator.free(col.values);
        }
        allocator.free(block.columns);
    }

    // Check timestamps
    try std.testing.expectEqual(100, block.timestamps[0]);
    try std.testing.expectEqual(200, block.timestamps[1]);
    try std.testing.expectEqual(300, block.timestamps[2]);

    // Should have 5 unique columns (level, app, host, cpu, memory)
    try std.testing.expectEqual(5, block.columns.len);

    // All columns should have 3 value slots
    for (block.columns) |col| {
        try std.testing.expectEqual(3, col.values.len);
    }

    // Verify app column exists (appears in lines 0 and 2 with same value, empty in line 1)
    var foundAppColumn = false;
    for (block.columns) |col| {
        if (std.mem.eql(u8, col.key, "app")) {
            foundAppColumn = true;
            try std.testing.expectEqualStrings("seq", col.values[0]);
            try std.testing.expectEqualStrings("", col.values[1]); // Missing in line 1
            try std.testing.expectEqualStrings("seq", col.values[2]);
        }
    }
    try std.testing.expect(foundAppColumn);
}

test "put: fast path with large value prevents celling" {
    const allocator = std.testing.allocator;

    // Create a large value that exceeds maxCelledColumnValueSize (256)
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
            .sid = .{ .id = 1, .tenantID = "1234" },
            .fields = fields1[0..],
            .encodedTags = undefined,
        },
        &.{
            .timestampNs = 200,
            .sid = .{ .id = 1, .tenantID = "1234" },
            .fields = fields2[0..],
            .encodedTags = undefined,
        },
    };

    const timestamps = try allocator.alloc(u64, lines.len);
    defer allocator.free(timestamps);

    var block = Block{
        .firstCelled = undefined,
        .columns = undefined,
        .timestamps = timestamps,
    };

    try block.put(allocator, &lines);
    defer {
        for (block.columns) |col| {
            allocator.free(col.values);
        }
        allocator.free(block.columns);
    }

    // Should have 1 regular column (message - too large) and 1 celled column (level)
    try std.testing.expectEqual(1, block.firstCelled);
    try std.testing.expectEqual(2, block.columns.len);

    // Check that message column is NOT celled (too large)
    const regularCols = block.getColumns();
    var foundMessage = false;
    for (regularCols) |col| {
        if (std.mem.eql(u8, col.key, "message")) {
            foundMessage = true;
            try std.testing.expectEqual(2, col.values.len);
        }
    }
    try std.testing.expect(foundMessage);

    // Check that level column IS celled
    const celledCols = block.getCelledColumns();
    var foundLevel = false;
    for (celledCols) |col| {
        if (std.mem.eql(u8, col.key, "level")) {
            foundLevel = true;
            try std.testing.expectEqual(1, col.values.len);
        }
    }
    try std.testing.expect(foundLevel);
}
