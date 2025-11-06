const std = @import("std");

const Field = @import("../lines.zig").Field;
const Line = @import("../lines.zig").Line;
const fieldLessThan = @import("../lines.zig").fieldLessThan;

const maxColumns = 1000;

fn columnLessThan(_: void, one: Column, another: Column) bool {
    return std.mem.lessThan(u8, one.key, another.key);
}

pub const Column = struct {
    key: []const u8,
    values: [][]const u8,
};

pub const Block = struct {
    columns: []Column,
    determinedColumns: []Field,
    timestamps: []u64,

    pub fn init(allocator: std.mem.Allocator, lines: []*const Line) !*Block {
        const b = try allocator.create(Block);
        const timestamps = try allocator.alloc(u64, lines.len);
        b.* = Block{
            .columns = undefined,
            .determinedColumns = undefined,
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
        allocator.free(self.determinedColumns);
        allocator.destroy(self);
    }

    pub fn len(self: *Block) usize {
        return self.columns.len + self.determinedColumns.len;
    }
    pub fn size(self: *Block) u64 {
        _ = self;
        unreachable;
    }

    fn put(self: *Block, allocator: std.mem.Allocator, lines: []*const Line) !void {
        // TODO: implement fast path when lines have same fields

        var columnI = std.StringHashMap(usize).init(allocator);
        for (lines) |line| {
            // TODO: implement maxColumns limit (1000 cols)

            for (line.fields) |field| {
                if (!columnI.contains(field.key)) {
                    try columnI.put(field.key, columnI.count());
                }
            }
        }

        for (lines, 0..) |line, i| {
            self.timestamps[i] = line.timestampNs;
        }
        const columns = try allocator.alloc(Column, columnI.count());
        const c = Column{ .key = "", .values = undefined };
        @memset(columns, c);

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
                const col = columns[idx];
                col.values[i] = field.value;
            }
        }

        // TODO: detect repeated columns having a single value across all the lines in the block,
        // if it exists - define as "determined" and remove from columns

        self.determinedColumns = try allocator.alloc(Field, 0);
        self.columns = columns;
    }

    fn sort(self: *Block) void {
        if (self.len() > maxColumns) @panic("columns and determinedColumns size exceeded maxColumns");

        std.mem.sortUnstable(Column, self.columns, {}, columnLessThan);
        std.mem.sortUnstable(Field, self.determinedColumns, {}, fieldLessThan);
    }
};
