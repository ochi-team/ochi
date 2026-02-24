const std = @import("std");
const Allocator = std.mem.Allocator;

const Table = @import("../Table.zig");

const LookupTable = @This();

table: *Table,

// state
current: []const u8,
isRead: bool,

pub fn init(table: *Table) LookupTable {
    return .{
        .table = table,

        .current = "",
        .isRead = false,
    };
}

pub fn lessThan(one: LookupTable, another: LookupTable) bool {
    return std.mem.lessThan(u8, one.current, another.current);
}

fn seek(self: *LookupTable, alloc: Allocator, key: []const u8) void {
    self.isRead = false;

    if (std.mem.lessThan(u8, self.table.tableHeader.lastItem, key)) {
        self.isRead = true;
        return;
    }

    if (self.quickSeek(key)) {
        return;
    }
    unreachable;
}



fn next(self: *const LookupTable) bool {
    _ = self;
    unreachable;
}
