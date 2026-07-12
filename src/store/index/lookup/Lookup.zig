const std = @import("std");
const Allocator = std.mem.Allocator;
const Io = std.Io;

const Heap = @import("../../../stds/heap.zig").Heap;
const Runtime = @import("../../../Runtime.zig");
const Cache = @import("../../../stds/Cache.zig").Cache;

const IndexRecorder = @import("../IndexRecorder.zig");
const MemBlock = @import("../MemBlock.zig");
const MemTable = @import("../MemTable.zig");
const Table = @import("../Table.zig");
const LookupTable = @import("LookupTable.zig");
const TagRecordsParser = @import("../TagRecordsParser.zig");
const CompressionPool = @import("../../compression/CompressionPool.zig");
const DecompressionPool = @import("../../compression/DecompressionPool.zig");
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

// TODO: a good object to implement a memory pool for:
// 1. reuse a last item query buffer
// 2. reuse tables and its  lookup list capacity
/// Initializes lookup cursors for all currently visible recorder tables.
pub fn init(io: Io, alloc: Allocator, longAlloc: Allocator, recorder: *IndexRecorder, cache: *Cache(*MemBlock)) !Lookup {
    var tables = try recorder.getTables(io, alloc);
    errdefer {
        for (tables.items) |t| t.release(io);
        tables.deinit(alloc);
    }

    var lookupTables = try std.ArrayList(LookupTable).initCapacity(alloc, tables.items.len);
    errdefer lookupTables.deinit(alloc);
    for (tables.items) |t| {
        const lt = LookupTable.init(longAlloc, t, recorder.maxMemBlockSize, cache, recorder.decompressionPool);
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

pub fn deinit(self: *Lookup, io: Io, alloc: Allocator) void {
    for (self.lookupTables.items) |*lt| lt.deinit(alloc);
    self.lookupTables.deinit(alloc);
    self.heapArray.deinit(alloc);
    for (self.tables.items) |t| t.release(io);
    self.tables.deinit(alloc);
}

/// Returns the first item that starts with prefix, or null if none exist.
/// Semantics are the same as:
/// 1) seek to the first item >= prefix
/// 2) verify the returned candidate still has the prefix.
pub fn findFirstByPrefix(self: *Lookup, io: Io, alloc: Allocator, prefix: []const u8) !?[]const u8 {
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
const resultLimit = 1000;
pub const StreamIDsByPrefixesResult = struct {
    streamIDs: std.AutoArrayHashMapUnmanaged(u128, void),
    cutOff: bool,
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

    // TODO: optimize so we dont iterate over next entries multiple times,
    // the isuue is seek resets the state and for every key we must restart the seek
    // we can pass a sorted list of prefixes and:
    // 1. split them into groups so we know if they share the same block/prefix
    // 2. if they ordered in .seek call we can skip previous block and continue from the current position
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
                Logger.log(.warn, "stream ids count reached the limit, return index earlier", .{ .limit = resultLimit });
                return .{
                    .streamIDs = streamIDs,
                    .cutOff = true,
                };
            }
        }
    }

    return .{
        .streamIDs = streamIDs,
        .cutOff = false,
    };
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

const testing = std.testing;

fn createMemTableFromItems(io: Io, alloc: Allocator, items: []const []const u8, compressionPool: *CompressionPool) !*Table {
    const decompressionPool = try DecompressionPool.init(alloc, 1);
    defer decompressionPool.deinit(alloc);

    var total: u32 = 0;
    for (items) |item| total += @intCast(item.len);

    var block = try MemBlock.init(alloc, .{
        .maxMemBlockSize = total + 16,
        .blocksCountHint = items.len,
    });
    for (items) |item| {
        const ok = block.add(item);
        try testing.expect(ok);
    }

    var blocks = [_]*MemBlock{block};
    const memTable = try MemTable.init(io, alloc, &blocks, compressionPool, decompressionPool);
    errdefer memTable.deinit(alloc);

    return Table.fromMem(io, alloc, memTable, decompressionPool);
}

fn createMemTableFromItemsInBlocks(
    io: Io,
    alloc: Allocator,
    items: []const []const u8,
    entriesPerBlock: usize,
    compressionPool: *CompressionPool,
) !*Table {
    const decompressionPool = try DecompressionPool.init(alloc, 1);
    defer decompressionPool.deinit(alloc);

    std.debug.assert(entriesPerBlock > 0);

    var blocks = std.ArrayList(*MemBlock).empty;
    defer {
        for (blocks.items) |block| block.deinit(alloc);
        blocks.deinit(alloc);
    }

    var i: usize = 0;
    while (i < items.len) {
        const end = @min(i + entriesPerBlock, items.len);
        var total: u32 = 0;
        for (items[i..end]) |item| total += @intCast(item.len);

        var block = try MemBlock.init(alloc, .{
            .maxMemBlockSize = total + 16,
            .blocksCountHint = end - i,
        });
        errdefer block.deinit(alloc);
        for (items[i..end]) |item| {
            const ok = block.add(item);
            try testing.expect(ok);
        }
        try blocks.append(alloc, block);

        i = end;
    }

    const movedBlocks = blocks.items;
    const memTable = try MemTable.init(io, alloc, movedBlocks, compressionPool, decompressionPool);
    errdefer memTable.deinit(alloc);
    blocks.items.len = 0;

    return Table.fromMem(io, alloc, memTable, decompressionPool);
}

fn createDiskTableFromItems(
    io: Io,
    alloc: Allocator,
    rootPath: []const u8,
    tableName: []const u8,
    items: []const []const u8,
    compressionPool: *CompressionPool,
) !*Table {
    const decompressionPool = try DecompressionPool.init(alloc, 1);
    defer decompressionPool.deinit(alloc);

    const tablePath = try std.fs.path.join(alloc, &.{ rootPath, tableName });
    errdefer alloc.free(tablePath);

    const memTable = try createMemTableFromItems(io, alloc, items, compressionPool);
    defer memTable.close(io);
    try memTable.inner.mem.storeToDisk(io, alloc, tablePath);

    return Table.open(io, alloc, tablePath, decompressionPool);
}

test "Lookup.findFirstByPrefix returns null on empty recorder" {
    const alloc = testing.allocator;
    const io = testing.io;

    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    const rootPath = try tmp.dir.realPathFileAlloc(io, ".", alloc);
    defer alloc.free(rootPath);

    const runtime = try Runtime.init(io, alloc, rootPath, 0.5);
    defer runtime.deinit(alloc);

    const compressionPool = try CompressionPool.init(alloc, 1);
    defer compressionPool.deinit(alloc);
    const decompressionPool = try DecompressionPool.init(alloc, 1);
    defer decompressionPool.deinit(alloc);

    const recorder = try IndexRecorder.init(io, alloc, rootPath, runtime, compressionPool, decompressionPool);
    defer recorder.deinit(io, alloc);
    recorder.stopped.stop(io);
    try recorder.g.await(io);

    const cache = try Cache(*MemBlock).init(alloc);
    defer cache.deinit();
    var lookup = try Lookup.init(io, alloc, alloc, recorder, cache);
    defer lookup.deinit(io, alloc);

    const prefixes = [_][]const u8{
        "",
        "key:",
        "zzzz",
    };
    for (prefixes) |prefix| {
        const actual = try lookup.findFirstByPrefix(io, alloc, prefix);
        try testing.expect(actual == null);
    }
}

test "Lookup.findAllStreamIDsByPrefixes returns empty on empty recorder" {
    const alloc = testing.allocator;
    const io = testing.io;

    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    const rootPath = try tmp.dir.realPathFileAlloc(io, ".", alloc);
    defer alloc.free(rootPath);

    const runtime = try Runtime.init(io, alloc, rootPath, 0.5);
    defer runtime.deinit(alloc);

    const compressionPool = try CompressionPool.init(alloc, 1);
    defer compressionPool.deinit(alloc);
    const decompressionPool = try DecompressionPool.init(alloc, 1);
    defer decompressionPool.deinit(alloc);

    const recorder = try IndexRecorder.init(io, alloc, rootPath, runtime, compressionPool, decompressionPool);
    defer recorder.deinit(io, alloc);
    recorder.stopped.stop(io);
    try recorder.g.await(io);

    const cache = try Cache(*MemBlock).init(alloc);
    defer cache.deinit();
    var lookup = try Lookup.init(io, alloc, alloc, recorder, cache);
    defer lookup.deinit(io, alloc);

    const prefixes = [_][]const u8{
        "key:",
        "zzzz",
    };
    var actual = try lookup.findAllStreamIDsByPrefixes(io, alloc, &prefixes);
    defer actual.streamIDs.deinit(alloc);

    try testing.expectEqual(actual.streamIDs.keys().len, 0);
}

test "Lookup.findFirstByPrefix matches lower-bound prefix behavior on mixed tables" {
    const alloc = testing.allocator;
    const io = testing.io;

    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    const rootPath = try tmp.dir.realPathFileAlloc(io, ".", alloc);
    defer alloc.free(rootPath);

    const runtime = try Runtime.init(io, alloc, rootPath, 0.5);
    defer runtime.deinit(alloc);

    const compressionPool = try CompressionPool.init(alloc, 1);
    defer compressionPool.deinit(alloc);
    const decompressionPool = try DecompressionPool.init(alloc, 1);
    defer decompressionPool.deinit(alloc);

    const recorder = try IndexRecorder.init(io, alloc, rootPath, runtime, compressionPool, decompressionPool);
    defer recorder.deinit(io, alloc);
    recorder.stopped.stop(io);
    try recorder.g.await(io);

    const tableAItems = [_][]const u8{
        "key:aa:002",
        "key:cc:002",
        "key:zz:100",
    };
    const tableBItems = [_][]const u8{
        "key:aa:001",
        "key:bb:010",
        "key:mm:900",
    };
    const tableDiskItems = [_][]const u8{
        "key:aa:099",
        "key:bb:001",
        "key:dd:000",
    };

    {
        const table = try createMemTableFromItems(io, alloc, &tableAItems, recorder.compressionPool);
        errdefer table.close(io);
        try recorder.memTables.append(alloc, table);
    }
    {
        const table = try createMemTableFromItems(io, alloc, &tableBItems, recorder.compressionPool);
        errdefer table.close(io);
        try recorder.memTables.append(alloc, table);
    }
    {
        const table = try createDiskTableFromItems(io, alloc, rootPath, "lookup-disk-table", &tableDiskItems, recorder.compressionPool);
        errdefer table.close(io);
        try recorder.diskTables.append(alloc, table);
    }

    const Case = struct {
        prefix: []const u8,
        expected: ?[]const u8,
    };

    const cases = [_]Case{
        .{ .prefix = "", .expected = "key:aa:001" },
        .{ .prefix = "a", .expected = null },
        .{ .prefix = "key:", .expected = "key:aa:001" },
        .{ .prefix = "key:aa:", .expected = "key:aa:001" },
        .{ .prefix = "key:aa:001", .expected = "key:aa:001" },
        .{ .prefix = "key:aa:001x", .expected = null },
        .{ .prefix = "key:aa:050", .expected = null },
        .{ .prefix = "key:bb:", .expected = "key:bb:001" },
        .{ .prefix = "key:bb:001", .expected = "key:bb:001" },
        .{ .prefix = "key:bb:011", .expected = null },
        .{ .prefix = "key:bc:", .expected = null },
        .{ .prefix = "key:mm:", .expected = "key:mm:900" },
        .{ .prefix = "key:zz:", .expected = "key:zz:100" },
        .{ .prefix = "key:zz:999", .expected = null },
        .{ .prefix = "zzzz", .expected = null },
    };

    const cache = try Cache(*MemBlock).init(alloc);
    defer cache.deinit();
    var lookup = try Lookup.init(io, alloc, alloc, recorder, cache);
    defer lookup.deinit(io, alloc);

    for (cases) |case| {
        const actual = try lookup.findFirstByPrefix(io, alloc, case.prefix);

        if (case.expected) |want| {
            try testing.expect(actual != null);
            try testing.expectEqualStrings(want, actual.?);
        } else {
            try testing.expect(actual == null);
        }
    }
    try recorder.flushForce(io, alloc);
}

test "Lookup.findAllStreamIDsByPrefixes matches lower-bound prefix behavior on mixed tables" {
    const alloc = testing.allocator;
    const io = testing.io;

    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    const rootPath = try tmp.dir.realPathFileAlloc(io, ".", alloc);
    defer alloc.free(rootPath);

    const runtime = try Runtime.init(io, alloc, rootPath, 0.5);
    defer runtime.deinit(alloc);

    const compressionPool = try CompressionPool.init(alloc, 1);
    defer compressionPool.deinit(alloc);
    const decompressionPool = try DecompressionPool.init(alloc, 1);
    defer decompressionPool.deinit(alloc);

    const recorder = try IndexRecorder.init(io, alloc, rootPath, runtime, compressionPool, decompressionPool);
    defer recorder.deinit(io, alloc);
    recorder.stopped.stop(io);
    try recorder.g.await(io);

    const tableAItems = [_][]const u8{
        "key:aa0000000000000002",
        "key:cc0000000000000002",
        "key:zz0000000000000100",
    };
    const tableBItems = [_][]const u8{
        "key:aa0000000000000001",
        "key:bb0000000000000010",
        "key:mm0000000000000900",
    };
    const tableDiskItems = [_][]const u8{
        "key:aa0000000000000099",
        "key:bb0000000000000001",
        "key:dd0000000000000000",
    };

    {
        const table = try createMemTableFromItems(io, alloc, &tableAItems, recorder.compressionPool);
        errdefer table.close(io);
        try recorder.memTables.append(alloc, table);
    }
    {
        const table = try createMemTableFromItems(io, alloc, &tableBItems, recorder.compressionPool);
        errdefer table.close(io);
        try recorder.memTables.append(alloc, table);
    }
    {
        const table = try createDiskTableFromItems(io, alloc, rootPath, "lookup-disk-table", &tableDiskItems, recorder.compressionPool);
        errdefer table.close(io);
        try recorder.diskTables.append(alloc, table);
    }

    const Case = struct {
        prefixes: []const []const u8,
        expected: ?[]const u128,
    };

    const cases = [_]Case{
        .{
            .prefixes = &[_][]const u8{"key:aa"},
            .expected = &[_]u128{
                std.mem.readInt(u128, "0000000000000001", .big),
                std.mem.readInt(u128, "0000000000000002", .big),
                std.mem.readInt(u128, "0000000000000099", .big),
            },
        },
        .{
            .prefixes = &[_][]const u8{ "key:aa", "key:bb" },
            .expected = &[_]u128{
                std.mem.readInt(u128, "0000000000000001", .big),
                std.mem.readInt(u128, "0000000000000002", .big),
                std.mem.readInt(u128, "0000000000000099", .big),
                std.mem.readInt(u128, "0000000000000010", .big),
            },
        },
        .{
            .prefixes = &[_][]const u8{ "key:cc", "key:bb" },
            .expected = &[_]u128{
                std.mem.readInt(u128, "0000000000000002", .big),
                std.mem.readInt(u128, "0000000000000001", .big),
                std.mem.readInt(u128, "0000000000000010", .big),
            },
        },
        .{
            .prefixes = &[_][]const u8{ "key:bb", "key:cc" },
            .expected = &[_]u128{
                std.mem.readInt(u128, "0000000000000001", .big),
                std.mem.readInt(u128, "0000000000000010", .big),
                std.mem.readInt(u128, "0000000000000002", .big),
            },
        },
        .{
            .prefixes = &[_][]const u8{ "key:aa", "key:aa" },
            .expected = &[_]u128{
                std.mem.readInt(u128, "0000000000000001", .big),
                std.mem.readInt(u128, "0000000000000002", .big),
                std.mem.readInt(u128, "0000000000000099", .big),
            },
        },
        .{
            .prefixes = &[_][]const u8{ "key:cc", "key:dd" },
            .expected = &[_]u128{
                std.mem.readInt(u128, "0000000000000002", .big),
                std.mem.readInt(u128, "0000000000000000", .big),
            },
        },
        .{
            .prefixes = &[_][]const u8{"zzz"},
            .expected = null,
        },
    };

    const cache = try Cache(*MemBlock).init(alloc);
    defer cache.deinit();
    var lookup = try Lookup.init(io, alloc, alloc, recorder, cache);
    defer lookup.deinit(io, alloc);

    for (cases) |case| {
        var actual = try lookup.findAllStreamIDsByPrefixes(io, alloc, case.prefixes);
        defer actual.streamIDs.deinit(alloc);

        if (case.expected) |want| {
            try testing.expectEqualSlices(u128, want, actual.streamIDs.keys());
            try testing.expect(!actual.cutOff);
        } else {
            try testing.expectEqual(actual.streamIDs.keys().len, 0);
        }
    }
    try recorder.flushForce(io, alloc);
}

test "Lookup cached disk mem block keeps prefix alive across lookups" {
    const alloc = testing.allocator;
    const io = testing.io;

    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    const rootPath = try tmp.dir.realPathFileAlloc(io, ".", alloc);
    defer alloc.free(rootPath);

    const runtime = try Runtime.init(io, alloc, rootPath, 0.5);
    defer runtime.deinit(alloc);

    const compressionPool = try CompressionPool.init(alloc, 1);
    defer compressionPool.deinit(alloc);
    const decompressionPool = try DecompressionPool.init(alloc, 1);
    defer decompressionPool.deinit(alloc);

    const recorder = try IndexRecorder.init(io, alloc, rootPath, runtime, compressionPool, decompressionPool);
    defer recorder.deinit(io, alloc);
    recorder.stopped.stop(io);
    try recorder.g.await(io);

    const tableDiskItems = [_][]const u8{
        "tenant-a-stream-0001",
        "tenant-a-stream-0002",
        "tenant-a-stream-0003",
    };
    {
        const table = try createDiskTableFromItems(io, alloc, rootPath, "lookup-cache-disk-table", &tableDiskItems, recorder.compressionPool);
        errdefer table.close(io);
        try recorder.diskTables.append(alloc, table);
    }

    const cache = try Cache(*MemBlock).init(alloc);
    defer cache.deinit();

    {
        var lookup = try Lookup.init(io, alloc, alloc, recorder, cache);
        defer lookup.deinit(io, alloc);

        const actual = try lookup.findFirstByPrefix(io, alloc, "tenant-a-stream-0002");
        try testing.expectEqualStrings("tenant-a-stream-0002", actual.?);
    }

    var it = cache.map.iterator();
    while (it.next()) |entry| {
        const block = entry.value_ptr.*.value;
        const prefix = block.prefix;
        const bufStart = @intFromPtr(block.buf.items.ptr);
        const bufEnd = bufStart + block.buf.items.len;
        const prefixStart = @intFromPtr(prefix.ptr);
        const prefixEnd = prefixStart + prefix.len;

        try testing.expect(prefix.len == 0 or
            (prefixStart >= bufStart and prefixEnd <= bufEnd));
    }

    const cachedKey = blk: {
        var keyIt = cache.map.iterator();
        const entry = keyIt.next().?;
        break :blk try alloc.dupe(u8, entry.key_ptr.*);
    };
    defer alloc.free(cachedKey);

    var block = try MemBlock.init(alloc, .{
        .maxMemBlockSize = 128,
        .blocksCountHint = tableDiskItems.len,
    });
    errdefer block.deinit(alloc);
    for (tableDiskItems) |item| {
        try testing.expect(block.add(item));
    }

    _ = try cache.put(io, cachedKey, block);
    const Case = struct {
        prefix: []const u8,
        expected: []const u8,
    };
    const cases = [_]Case{
        .{ .prefix = "tenant-a-stream-0001", .expected = "tenant-a-stream-0001" },
        .{ .prefix = "tenant-a-stream-0002", .expected = "tenant-a-stream-0002" },
        .{ .prefix = "tenant-a-stream-", .expected = "tenant-a-stream-0001" },
    };

    var lookup = try Lookup.init(io, alloc, alloc, recorder, cache);
    defer lookup.deinit(io, alloc);
    for (cases) |case| {
        const actual = try lookup.findFirstByPrefix(io, alloc, case.prefix);
        try testing.expectEqualStrings(case.expected, actual.?);
    }

    try recorder.flushForce(io, alloc);
}

test "Lookup.deinit after scan across multiple table blocks" {
    const alloc = testing.allocator;
    const io = testing.io;

    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    const rootPath = try tmp.dir.realPathFileAlloc(io, ".", alloc);
    defer alloc.free(rootPath);

    const runtime = try Runtime.init(io, alloc, rootPath, 0.5);
    defer runtime.deinit(alloc);

    const compressionPool = try CompressionPool.init(alloc, 1);
    defer compressionPool.deinit(alloc);
    const decompressionPool = try DecompressionPool.init(alloc, 1);
    defer decompressionPool.deinit(alloc);

    const recorder = try IndexRecorder.init(io, alloc, rootPath, runtime, compressionPool, decompressionPool);
    defer recorder.deinit(io, alloc);
    recorder.stopped.stop(io);
    try recorder.g.await(io);

    var items = try std.ArrayList([]const u8).initCapacity(alloc, 2200);
    defer items.deinit(alloc);
    for (0..items.capacity) |i| {
        const item = try alloc.alloc(u8, 20);
        errdefer alloc.free(item);
        @memcpy(item[0..4], "key:");
        std.mem.writeInt(u128, item[4..20], i, .big);
        items.appendAssumeCapacity(item);
    }
    defer {
        for (items.items) |item| alloc.free(item);
    }

    {
        const table = try createMemTableFromItemsInBlocks(io, alloc, items.items, 200, recorder.compressionPool);
        errdefer table.close(io);
        try recorder.memTables.append(alloc, table);
    }

    const cache = try Cache(*MemBlock).init(alloc);
    defer cache.deinit();
    var lookup = try Lookup.init(io, alloc, alloc, recorder, cache);
    defer lookup.deinit(io, alloc);

    try lookup.seek(io, alloc, "key:");
    var count: usize = 0;
    while (try lookup.next(io, alloc)) {
        count += 1;
    }
    try testing.expectEqual(items.items.len, count);

    try recorder.flushForce(io, alloc);
}

test "Lookup.findAllStreamIDsByPrefixes respects result limit cutoff" {
    const alloc = testing.allocator;
    const io = testing.io;

    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    const rootPath = try tmp.dir.realPathFileAlloc(io, ".", alloc);
    defer alloc.free(rootPath);

    const runtime = try Runtime.init(io, alloc, rootPath, 0.5);
    defer runtime.deinit(alloc);

    const compressionPool = try CompressionPool.init(alloc, 1);
    defer compressionPool.deinit(alloc);
    const decompressionPool = try DecompressionPool.init(alloc, 1);
    defer decompressionPool.deinit(alloc);

    const recorder = try IndexRecorder.init(io, alloc, rootPath, runtime, compressionPool, decompressionPool);
    defer recorder.deinit(io, alloc);
    recorder.stopped.stop(io);
    try recorder.g.await(io);

    var items = try std.ArrayList([]const u8).initCapacity(alloc, resultLimit + 1);
    defer items.deinit(alloc);

    const keyValue = "keyaa";
    for (0..resultLimit + 1) |i| {
        const item = try alloc.alloc(u8, keyValue.len + 16);
        errdefer alloc.free(item);
        std.mem.writeInt(u128, item[keyValue.len..][0..16], i, .big);
        @memcpy(item[0..keyValue.len], keyValue);
        items.appendAssumeCapacity(item);
    }
    defer {
        for (items.items) |item| {
            alloc.free(item);
        }
    }

    {
        const table = try createMemTableFromItems(io, alloc, items.items, recorder.compressionPool);
        errdefer table.close(io);
        try recorder.memTables.append(alloc, table);
    }

    const cache = try Cache(*MemBlock).init(alloc);
    defer cache.deinit();
    var lookup = try Lookup.init(io, alloc, alloc, recorder, cache);
    defer lookup.deinit(io, alloc);

    var actual = try lookup.findAllStreamIDsByPrefixes(io, alloc, &[_][]const u8{keyValue});
    defer actual.streamIDs.deinit(alloc);

    try testing.expect(actual.streamIDs.keys().len != 0);
    try testing.expect(actual.cutOff);
    try testing.expectEqual(@as(usize, resultLimit), actual.streamIDs.keys().len);
    try testing.expectEqual(0, actual.streamIDs.keys()[0]);
    try testing.expectEqual(999, actual.streamIDs.keys()[resultLimit - 1]);

    try recorder.flushForce(io, alloc);
}
