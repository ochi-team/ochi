const std = @import("std");
const Allocator = std.mem.Allocator;

const QueryResult = @import("QueryResult.zig");
const Line = @import("../../store/lines.zig").Line;
const Field = @import("../../store/lines.zig").Field;
const copyFields = @import("../../store/lines.zig").copyFields;
const deinitLinesFull = @import("../../store/lines.zig").deinitLinesFull;
const freeFields = @import("../../store/lines.zig").freeFields;

pub const Sink = @This();

lines: std.ArrayList(Line) = .empty,

pub fn deinit(self: *Sink, alloc: Allocator) void {
    deinitLinesFull(alloc, &self.lines);
}

pub fn write(self: *Sink, alloc: Allocator, result: *const QueryResult) !void {
    const initialLen = self.lines.items.len;
    errdefer {
        for (self.lines.items[initialLen..]) |line| freeFields(alloc, line.fields);
        self.lines.shrinkRetainingCapacity(initialLen);
    }
    try self.lines.ensureUnusedCapacity(alloc, result.len);
    var fields: std.ArrayList(Field) = .empty;
    defer fields.deinit(alloc);
    try fields.ensureUnusedCapacity(alloc, result.invariants.items.len + result.columns.items.len);
    for (result.timestamps.items, 0..) |ts, row| {
        fields.clearRetainingCapacity();
        for (result.invariants.items) |col| {
            fields.appendAssumeCapacity(.{ .key = col.key, .value = col.values[0] });
        }
        for (result.columns.items) |col| {
            const value = col.values[row];
            if (value.len != 0) fields.appendAssumeCapacity(.{ .key = col.key, .value = value });
        }
        self.lines.appendAssumeCapacity(.{ .timestampNs = ts, .fields = try copyFields(alloc, fields.items) });
    }
}

