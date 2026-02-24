const std = @import("std");
const Allocator = std.mem.Allocator;

const Heap = @import("../../../stds/Heap.zig").Heap;

const IndexRecorder = @import("../IndexRecorder.zig");
const Table = @import("../Table.zig");
const LookupTable = @import("LookupTable.zig");

const Lookup = @This();

recorder: *IndexRecorder,

tables: std.ArrayList(*Table),
lookupTables: std.ArrayList(LookupTable),

heapArray: std.ArrayList(LookupTable),
tablesHeap: Heap(LookupTable, LookupTable.lessThan),

// state
current: []const u8,
isRead: bool,
seekedIsCurrent: bool,

// TODO: a good object to implement a memory pool for:
// 1. reuse a last item query buffer
// 2. reuse tables and its  lookup list capacity
pub fn init(alloc: Allocator, recorder: *IndexRecorder) !Lookup {
    var tables = try recorder.getTables(alloc);
    errdefer {
        for (tables.items) |t| t.release();
        tables.deinit(alloc);
    }

    var lookupTables = try std.ArrayList(LookupTable).initCapacity(alloc, tables.items.len);
    for (tables.items) |t| {
        const lt = LookupTable.init(t);
        lookupTables.appendAssumeCapacity(lt);
    }

    return .{
        .recorder = recorder,
        .tables = tables,
        .lookupTables = lookupTables,

        .heapArray = .empty,
        .tablesHeap = undefined,

        .current = undefined,
        .isRead = false,
        .seekedIsCurrent = false,
    };
}

pub fn deinit(self: *Lookup, alloc: Allocator) void {
    for (self.lookupTables.items) |*lt| lt.deinit();
    self.lookupTables.deinit(alloc);
    self.heapArray.deinit(alloc);
    for (self.tables.items) |t| t.release();
    self.tables.deinit(alloc);
}

pub fn findFirstByPrefix(self: *Lookup, alloc: Allocator, prefix: []const u8) !?[]const u8 {
    try self.seek(alloc, prefix);

    if (!self.next()) {
        return null;
    }

    if (std.mem.containsAtLeast(u8, self.current, 1, prefix)) {
        return self.current;
    }

    return null;
}

fn seek(self: *Lookup, alloc: Allocator, key: []const u8) !void {
    self.isRead = false;
    self.heapArray.clearRetainingCapacity();

    for (0..self.lookupTables.items.len) |i| {
        var lt = self.lookupTables.items[i];
        lt.seek(key);
        if (!lt.next()) {
            continue;
        }

        try self.heapArray.append(alloc, lt);
    }

    if (self.heapArray.items.len == 0) {
        self.isRead = true;
        return;
    }

    self.tablesHeap = .init(alloc, &self.heapArray);
    self.tablesHeap.heapify();
    self.current = self.tablesHeap.array.items[0].current;
    self.seekedIsCurrent = true;
}

fn next(self: *Lookup) bool {
    if (self.isRead) return false;

    if (self.seekedIsCurrent) {
        self.seekedIsCurrent = false;
        return true;
    }

    const hasNext = self.nextBlock();
    self.isRead = !hasNext;
    return hasNext;
}

fn nextBlock(self: *Lookup) bool {
    var lt = self.tablesHeap.array.items[0];
    if (lt.next()) {
        self.tablesHeap.fix(0);
        self.current = self.tablesHeap.array.items[0].current;
        return true;
    }

    _ = self.tablesHeap.pop();
    if (self.tablesHeap.array.items.len == 0) return false;

    self.current = self.tablesHeap.array.items[0].current;
    return true;
}
