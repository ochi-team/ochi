const std = @import("std");
const Allocator = std.mem.Allocator;
const Io = std.Io;

const Field = @import("../lines.zig").Field;
const Line = @import("../lines.zig").Line;
const Column = @import("Column.zig");
const BlockData = @import("BlockData.zig").BlockData;
const Unpacker = @import("Unpacker.zig").Unpacker;
const ValuesDecoder = @import("ValuesDecoder.zig");
const TimestampsEncoder = @import("TimestampsEncoder.zig");
const Logger = @import("logging");

const tracy = @import("tracy");

const sizing = @import("sizing.zig");

pub const maxColumns = 2048;
// maxLines is a max amount of lines that we can put into a block,
// assuming the maxBlockSize
// approximate value if the max body size ~4mb and an average small line 128 bytes
// the batch size is used in the ingest request, accumulator arena, data shard buffer
pub const maxLines: usize = 32 * 1014;

comptime {
    std.debug.assert(@import("../lines.zig").defaultMaxFieldsPerLine * 2 <= maxColumns);
}

fn columnLessThan(_: void, one: Column, another: Column) bool {
    return std.mem.lessThan(u8, one.key, another.key);
}

const Block = @This();

firstInvariant: u32,
columns: []Column,
timestamps: []u64,

pub fn initFromLines(allocator: Allocator, lines: []const Line) !Block {
    var b: Block = .{
        .firstInvariant = undefined,
        .columns = undefined,
        .timestamps = undefined,
    };

    try b.put(allocator, lines);
    std.debug.assert(b.timestamps.len <= maxLines);
    std.debug.assert(b.columns.len <= maxColumns);
    b.sort();
    return b;
}

pub fn initFromData(
    io: Io,
    alloc: Allocator,
    timestampsEncoders: *TimestampsEncoder.TimestampsEncoderPool,
    data: *BlockData,
    comptime leaky: bool,
    unpacker: *Unpacker(leaky),
    decoder: *ValuesDecoder,
) !Block {
    const z = tracy.Zone.begin(.{
        .src = @src(),
        .name = "data.Block.initFromData",
    });
    defer z.end();
    std.debug.assert(data.len <= maxLines);

    const tss = try alloc.alloc(u64, data.len);
    errdefer alloc.free(tss);
    try timestampsEncoders.decode(io, tss, data.timestampsData.data);

    const firstInvariant: u32 = @intCast(data.columnsData.items.len);
    const invariantColsLen = if (data.invariantColumns) |invariants| invariants.len else 0;
    const columns = try alloc.alloc(Column, data.columnsData.items.len + invariantColsLen);

    for (0..data.columnsData.items.len) |i| {
        const colData = &data.columnsData.items[i];
        var col = &columns[i];
        col.key = colData.key;
        col.values = try unpacker.unpackValues(io, alloc, colData.bloomValues, data.len);
        try decoder.decode(io, alloc, col.values, colData.type, colData.dict.values.items);
    }

    var lastCopied: u16 = 0;
    errdefer {
        for (columns[firstInvariant .. firstInvariant + lastCopied]) |col| {
            alloc.free(col.values);
        }
    }
    if (data.invariantColumns) |invariants| {
        for (invariants, 0..) |*invariant, i| {
            columns[firstInvariant + i].key = invariant.key;
            // move the values to the block instead of copying them
            columns[firstInvariant + i].values = try alloc.dupe([]const u8, invariant.values);
            // we know max columns fit into u16
            lastCopied = @intCast(i);
            // std.mem.swap([][]const u8, &columns[firstInvariant + i].values, &invariant.values);
        }
    }

    return .{
        .firstInvariant = firstInvariant,
        .columns = columns,
        .timestamps = tss,
    };
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
}

pub fn getColumns(self: *const Block) []Column {
    return self.columns[0..self.firstInvariant];
}
// getInvariantColumns gives columns with a single value
pub fn getInvariantColumns(self: *const Block) []Column {
    return self.columns[self.firstInvariant..];
}

pub fn len(self: *const Block) usize {
    return self.timestamps.len;
}

pub fn size(self: *const Block) u32 {
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
        const uniqueKeysCount = columnI.count() + line.fields.len;
        if (uniqueKeysCount > maxColumns) {
            // it means the stream has too large cardinality,
            // the users must look after the schema they generate
            // and adjust acordingly
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

// TODO: investigate if we need to check for unique/duplicated fields keys as well.
pub fn areSameFields(lines: []const Line) bool {
    if (lines[0].fields.len > maxColumns) return false;

    if (lines.len < 2) {
        @branchHint(.unlikely);
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

pub fn canBeSavedAsInvariant(lines: []const Line, index: usize) bool {
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
