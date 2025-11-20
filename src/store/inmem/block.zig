const std = @import("std");

const Field = @import("../lines.zig").Field;
const Line = @import("../lines.zig").Line;
const fieldLessThan = @import("../lines.zig").fieldLessThan;

const maxColumns = 1000;

fn columnLessThan(_: void, one: Column, another: Column) bool {
    return std.mem.lessThan(u8, one.key, another.key);
}

// makes no sense to keep large values in celled columns,
// it won't help to improve performance
const maxCelledColumnValueSize = 256;

pub const Column = struct {
    key: []const u8,
    values: [][]const u8,

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

    pub fn getColumns(self: *Block) []Column {
        return self.columns[0..self.firstCelled];
    }
    // celledColumns hold columns with a single value
    pub fn getCelledColumns(self: *Block) []Column {
        return self.columns[self.firstCelled..];
    }

    pub fn len(self: *Block) usize {
        return self.timestamps.len;
    }

    const tsRfc3339Nano = "2006-01-02T15:04:05.999999999Z07:00";
    const tsLineJsonSurrounding = "{\"_time\":\"\"},\n";
    const lineTsSize = tsRfc3339Nano.len + tsLineJsonSurrounding.len;
    const lineSurroundSize = "\"\":\"\"},".len;

    // gives size in resulted json object
    // TODO: test against real resulted log record
    pub fn size(self: *Block) u64 {
        if (self.timestamps.len == 0) {
            return 0;
        }
        // timestamp key value reserved
        var res = lineTsSize * self.timestamps.len;

        for (self.getCelledColumns()) |col| {
            const colValueLen = if (col.values.len > 0) col.values[0].len else 0;
            res += (lineSurroundSize + col.key.len + colValueLen) * self.timestamps.len;
        }

        for (self.getColumns()) |col| {
            for (col.values) |val| {
                if (val.len == 0) {
                    continue;
                }

                res += (lineSurroundSize + col.key.len + val.len) * self.timestamps.len;
            }
        }

        return res;
    }

    fn put(self: *Block, allocator: std.mem.Allocator, lines: []*const Line) !void {
        // TODO: implement fast path when lines have same fields

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
