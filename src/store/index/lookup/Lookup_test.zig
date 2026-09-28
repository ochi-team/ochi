const std = @import("std");
const Allocator = std.mem.Allocator;
const Io = std.Io;

const tracy = @import("tracy");

const Heap = @import("../../../stds/heap.zig").Heap;
const Cache = @import("../../../stds/Cache.zig").Cache;

const IndexRecorder = @import("../IndexRecorder.zig");
const MemBlock = @import("../MemBlock.zig");
const Table = @import("../Table.zig");
const LookupTable = @import("LookupTable.zig");
const TagRecordsParser = @import("../TagRecordsParser.zig");
const Logger = @import("logging");

const Lookup = @This();

recorder: *IndexRecorder,

tables: std.ArrayList(*Table),
lookupTables: std.ArrayList(LookupTable),

heapArray: std.ArrayList(*LookupTable),
tablesHeap: Heap(*LookupTable, LookupTable.lessThanPtr),

// state
current: []const u8,
isRead: bool,
seekedIsCurrent: bool,

/// empty is a scaffold to setup a blank value in a pool
pub const empty: Lookup = .{
    .recorder = undefined,
    .tables = .empty,
    .lookupTables = .empty,

    .heapArray = .empty,
    .tablesHeap = undefined,

    .current = undefined,
    .isRead = false,
    .seekedIsCurrent = false,
};

pub fn init(
    io: Io,
    requestArena: Allocator,
    cacheAlloc: Allocator,
    recorder: *IndexRecorder,
    cache: *Cache(*MemBlock),
) !Lookup {
    var self: Lookup = .empty;
    errdefer self.deinit(io, requestArena);
    try self.setup(io, requestArena, cacheAlloc, recorder, cache);
    return self;
}

pub fn setup(
    self: *Lookup,
    io: Io,
    alloc: Allocator,
    cacheAlloc: Allocator,
    recorder: *IndexRecorder,
    cache: *Cache(*MemBlock),
) !void {
    self.recorder = recorder;

    try recorder.collectTables(io, alloc, &self.tables);

    try self.lookupTables.ensureUnusedCapacity(alloc, self.tables.items.len);
    for (self.tables.items) |t| {
        self.lookupTables.appendAssumeCapacity(LookupTable.init(cacheAlloc, t, recorder.maxMemBlockSize, cache, recorder.decompressionPool));
    }
}

pub fn deinit(self: *Lookup, io: Io, alloc: Allocator) void {
    for (self.lookupTables.items) |*lt| lt.deinit(alloc);
    self.lookupTables.deinit(alloc);
    self.heapArray.deinit(alloc);
    for (self.tables.items) |t| t.release(io);
    self.tables.deinit(alloc);
}

pub fn reset(self: *Lookup, io: Io, alloc: Allocator) void {
    for (self.lookupTables.items) |*lt| lt.deinit(alloc);
    self.lookupTables.clearRetainingCapacity();
    self.heapArray.clearRetainingCapacity();
    for (self.tables.items) |t| t.release(io);
    self.tables.clearRetainingCapacity();
}

/// Returns the first item that starts with prefix, or null if none exist.
/// Semantics are the same as:
/// 1) seek to the first item >= prefix
/// 2) verify the returned candidate still has the prefix.
pub fn findFirstByPrefix(self: *Lookup, io: Io, alloc: Allocator, prefix: []const u8) !?[]const u8 {
    const z = tracy.Zone.begin(.{
        .src = @src(),
        .name = "Lookup.findFirstByPrefix",
    });
    defer z.end();

    try self.seek(io, alloc, prefix);

    if (!try self.next(io, alloc)) {
        return null;
    }

    if (self.current.len >= prefix.len and
        std.mem.eql(u8, self.current[0..prefix.len], prefix))
    {
        return self.current;
    }

    return null;
}

/// Returns an owned slice of owned slices representing items that start
/// with given prefixes, or null if none exist. The following flag determines
/// if the result was cut off or not.
/// TODO: make it configurable and reduce for tests to 10
/// TODO: take a meter to understand how often it hits the limit
pub const resultLimit = 1000;
pub const StreamIDsByPrefixesResult = struct {
    streamIDs: std.AutoArrayHashMapUnmanaged(u128, void),
};
pub fn findAllStreamIDsByPrefixes(
    self: *Lookup,
    io: Io,
    alloc: Allocator,
    prefixes: []const []const u8,
) !StreamIDsByPrefixesResult {
    std.debug.assert(prefixes.len > 0);
    for (prefixes) |prefix|
        std.debug.assert(prefix.len > 0);

    var streamIDs: std.AutoArrayHashMapUnmanaged(u128, void) = .empty;
    errdefer streamIDs.deinit(alloc);

    var state: TagRecordsParser = .{};
    defer state.deinit(alloc);

    for (prefixes) |prefix| {
        try self.seek(io, alloc, prefix);

        while (try self.next(io, alloc)) {
            if (self.current.len >= prefix.len and
                std.mem.eql(u8, self.current[0..prefix.len], prefix))
            {
                try state.setupStreamsRaw(self.current[prefix.len..]);
                try state.parseStreamIDs(alloc);

                for (state.streamIDs.items) |streamID| {
                    const gop = try streamIDs.getOrPut(alloc, streamID);

                    if (gop.found_existing) continue;

                    gop.key_ptr.* = streamID;
                }
            }

            if (streamIDs.count() >= resultLimit) {
                Logger.log(
                    .warn,
                    "stream ids count reached the limit, return index earlier",
                    .{ .limit = resultLimit },
                );
                return .{ .streamIDs = streamIDs };
            }
        }
    }

    return .{ .streamIDs = streamIDs };
}

fn seek(self: *Lookup, io: Io, alloc: Allocator, key: []const u8) !void {
    self.isRead = false;
    self.heapArray.clearRetainingCapacity();

    // Each table cursor is positioned at the first item >= key and then
    // contributes its current item to the global min-heap.
    for (0..self.lookupTables.items.len) |i| {
        var lt = &self.lookupTables.items[i];
        try lt.seek(io, alloc, key);
        if (!try lt.next(io, alloc)) {
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

fn next(self: *Lookup, io: Io, alloc: Allocator) !bool {
    if (self.isRead) return false;

    if (self.seekedIsCurrent) {
        self.seekedIsCurrent = false;
        return true;
    }

    const hasNext = try self.nextBlock(io, alloc);
    self.isRead = !hasNext;
    return hasNext;
}

fn nextBlock(self: *Lookup, io: Io, alloc: Allocator) !bool {
    // The heap stores pointers to the reusable table cursors owned by lookupTables.
    // Advancing the min cursor and fixing the heap yields the next global item.
    const lt = self.tablesHeap.array.items[0];
    if (try lt.next(io, alloc)) {
        self.tablesHeap.fix(0);
        self.current = self.tablesHeap.array.items[0].current;
        return true;
    }

    _ = self.tablesHeap.pop();
    if (self.tablesHeap.array.items.len == 0) return false;

    self.current = self.tablesHeap.array.items[0].current;
    return true;
}

test {
    _ = @import("Lookup_test.zig");
}
