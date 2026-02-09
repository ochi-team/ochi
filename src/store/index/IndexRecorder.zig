const std = @import("std");
const Allocator = std.mem.Allocator;

const Entries = @import("Entries.zig");
const MemBlock = @import("MemBlock.zig");
const Table = @import("Table.zig");
const MemTable = @import("MemTable.zig");
const BlockWriter = @import("BlockWriter.zig");
const BlockReader = @import("BlockReader.zig");

const flush = @import("flush/flush.zig");

const Conf = @import("../../Conf.zig");

const TableKind = enum {
    mem,
    file,
};

const maxBlocksPerShard = 256;

// TODO: worth tuning on practice
const blocksInMemTable = 15;
const maxMemTables = 24;

const IndexRecorder = @This();

entries: *Entries,

blocksToFlush: std.ArrayList(*MemBlock),
mxBlocks: std.Thread.Mutex = .{},
flushAtUs: ?i64 = null,
blocksThresholdToFlush: u32,

// config fields
maxIndexBlockSize: u32,

stopped: std.atomic.Value(bool) = .init(false),
// limits amount of mem tables in order to handle too high ingestion rate,
// when mem tables are not merged fast enough
memTablesSem: std.Thread.Semaphore = .{
    .permits = maxMemTables,
},
memTablesMx: std.Thread.Mutex = .{},
tables: std.ArrayList(*Table),
memTables: std.ArrayList(*Table),

pool: *std.Thread.Pool,
// wg holds all the running jobs
wg: std.Thread.WaitGroup = .{},

needInvalidate: std.atomic.Value(bool) = .init(false),
indexCacheKeyVersion: std.atomic.Value(u64) = .init(0),

mergeIdx: std.atomic.Value(u64),
path: []const u8,

pub fn init(alloc: Allocator, path: []const u8) !*IndexRecorder {
    const conf = Conf.getConf();
    const entries = try Entries.init(alloc);
    errdefer entries.deinit(alloc);

    const blocksThresholdToFlush: u64 = @intCast(entries.shards.len * maxBlocksPerShard);

    // TODO: try using list of lists instead in order not to copy data from blocks to blocksToFlush
    var blocksToFlush = try std.ArrayList(*MemBlock).initCapacity(alloc, blocksThresholdToFlush);
    errdefer blocksToFlush.deinit(alloc);

    var pool = try alloc.create(std.Thread.Pool);
    errdefer alloc.destroy(pool);
    try pool.init(.{
        .allocator = alloc,
        .n_jobs = conf.server.pools.workerThreads,
    });

    var memTables = try std.ArrayList(*Table).initCapacity(alloc, maxMemTables);
    errdefer memTables.deinit(alloc);

    // TODO: move it to the config level
    var trimmedPath = path[0..];
    if (std.fs.path.isSep(trimmedPath[trimmedPath.len])) {
        trimmedPath = trimmedPath[0 .. trimmedPath.len - 1];
    }

    var tables = try Table.openAll(alloc, trimmedPath);
    errdefer {
        for (tables.items) |table| table.close(alloc);
        tables.deinit(alloc);
    }

    const t = try alloc.create(IndexRecorder);
    t.* = .{
        .entries = entries,
        .blocksThresholdToFlush = @intCast(entries.shards.len * maxBlocksPerShard),
        .blocksToFlush = blocksToFlush,
        .maxIndexBlockSize = Conf.getConf().app.maxIndexMemBlockSize,
        .pool = pool,
        .tables = tables,
        .memTables = memTables,
        .mergeIdx = .init(@intCast(std.time.nanoTimestamp())),
        .path = trimmedPath,
    };
    return t;
}

pub fn deinit(self: *IndexRecorder, alloc: Allocator) void {
    self.entries.deinit(alloc);
    self.blocksToFlush.deinit(alloc);
    self.tables.deinit(alloc);
    self.memTables.deinit(alloc);
    self.pool.deinit();
    alloc.destroy(self);
}

pub fn nextMergeIdx(self: *IndexRecorder) u64 {
    return self.mergeIdx.fetchAdd(1, .acquire);
}

pub fn add(self: *IndexRecorder, alloc: Allocator, entries: [][]const u8) !void {
    const shard = self.entries.next();
    const blocksList = try shard.add(alloc, entries, self.maxIndexBlockSize);
    if (blocksList == null) return;

    var blocks = blocksList.?;
    defer blocks.deinit(alloc);
    try self.flushBlocks(alloc, blocks.items);
}

fn flushBlocks(self: *IndexRecorder, alloc: Allocator, blocks: []*MemBlock) !void {
    if (blocks.len == 0) return;

    self.mxBlocks.lock();
    defer self.mxBlocks.unlock();

    if (self.blocksToFlush.items.len == 0) {
        self.flushAtUs = std.time.microTimestamp() + std.time.us_per_s;
    }

    try self.blocksToFlush.appendSlice(alloc, blocks);
    if (self.blocksToFlush.items.len >= self.blocksThresholdToFlush) {
        try self.flushBlocksToMemTables(alloc, self.blocksToFlush.items, false);
        self.blocksToFlush.clearRetainingCapacity();
    }
}

fn flushBlocksToMemTables(self: *IndexRecorder, alloc: Allocator, blocks: []*MemBlock, force: bool) !void {
    const tablesSize = (blocks.len + blocksInMemTable - 1) / blocksInMemTable;
    var memTables = try std.ArrayList(*MemTable).initCapacity(alloc, tablesSize);
    errdefer {
        for (memTables.items) |memTable| memTable.deinit(alloc);
        memTables.deinit(alloc);
    }

    var tail = blocks[0..];
    // TODO: benchmark parallel mem table creation
    while (tail.len > 0) {
        const offset = @min(blocksInMemTable, tail.len);
        const head = tail[0..offset];
        tail = tail[offset..];

        const memTable = try MemTable.init(alloc, head);
        memTables.appendAssumeCapacity(memTable);
    }

    // TODO: make mergeMemTables returning tail, all the merged and unmerged tables,
    // so that we could identify big enough tables and add them,
    // for high ingestion rate it's required to handle limited size of tables,
    // it requires:
    // 1. returning a tail from merge
    // 2. identify tables for optimal merge in the loop over the tables
    const mergedMemTable = try mergeMemTables(alloc, memTables.items);
    const table = try Table.fromMem(alloc, mergedMemTable);
    try self.addToMemTables(alloc, table, force);
}

/// merges mem tables to a bigger size ones
/// requires same Allocator that's used to create them,
/// because it deinits the merged ones
fn mergeMemTables(alloc: Allocator, memTables: []*MemTable) !*MemTable {
    // TODO: run merging job in parallel and benchmark whether it doesn't hurt general throughput

    std.debug.assert(memTables.len != 0);
    if (memTables.len == 1) return memTables[0];

    return MemTable.mergeMemTables(alloc, memTables);
}

fn addToMemTables(self: *IndexRecorder, alloc: Allocator, memTable: *Table, force: bool) !void {
    var semaphoreWaited = false; // if not stopped then wait for an available semaphore
    if (!self.stopped.load(.acquire)) {
        self.memTablesSem.wait();
        semaphoreWaited = true;
    }
    errdefer if (semaphoreWaited) self.memTablesSem.post();

    // TODO: ideally to know the amount of mem tables and call unlock without errdefer
    self.memTablesMx.lock();
    errdefer self.memTablesMx.unlock();
    try self.memTables.append(alloc, memTable);
    try self.startMemTablesMerge(alloc);
    self.memTablesMx.unlock();

    if (force) {
        self.invalidateStreamFilterCache();
    } else {
        if (!self.needInvalidate.load(.acquire)) {
            _ = self.needInvalidate.cmpxchgWeak(false, true, .release, .monotonic);
        }
    }
}

fn startMemTablesMerge(self: *IndexRecorder, alloc: Allocator) !void {
    if (self.stopped.load(.acquire)) return;

    // TODO: schedule a background job to merge mem tables,
    // changing it requires:
    // 1. holding a mutex for mem tables
    // 2. adding a semaphore to limit concurrent merges
    // 3. mark mem tables being merged to avoid double acquisition
    return self.runMemTablesMerger(alloc);
}

fn runMemTablesMerger(self: *IndexRecorder, alloc: Allocator) !void {
    while (true) {
        // TODO: implement disk space limit

        if (self.memTables.items.len == 0) {
            return;
        }

        // TODO: make sure error.Stopped is handled on the upper level
        try self.mergeTables(alloc, self.memTables.items, false);
    }
}

fn invalidateStreamFilterCache(self: *IndexRecorder) void {
    _ = self.indexCacheKeyVersion.fetchAdd(1, .acquire);
}

pub fn mergeTables(
    self: *IndexRecorder,
    alloc: Allocator,
    tables: []*Table,
    force: bool,
) !void {
    const tableKind = getDestinationTableKind(tables, force);
    var fba = std.heap.stackFallback(64, alloc);
    const fbaAlloc = fba.get();

    // 1 for / and 16 for 16 bytes of idx representation,
    // we can't bitcast it to [8]u8 because we need human readlable file names
    var destinationTablePath: []u8 = "";
    defer if (destinationTablePath.len > 0) fbaAlloc.free(destinationTablePath);
    if (tableKind == .file) {
        destinationTablePath = try fbaAlloc.alloc(u8, self.path.len + 1 + 16);
        const idx = self.nextMergeIdx();
        _ = try std.fmt.bufPrint(
            destinationTablePath,
            "{s}/{X:0>16}",
            .{ self.path, idx },
        );
    }

    // FIXME: implement a shutdown path
    // if (force and self.memTables.items.len == 1) {
    //     const table = self.memTables.items[0];
    //     table.storeToDisk(destinationTablePath);
    //     const newTable = openCreatedTable(destinationTablePath, self.memTables, null);
    //     self.swapTables(self.memTables, newTable, tableKind);
    //     return;
    // }

    var readers = try openTableReaders(alloc, tables);
    defer {
        for (readers.items) |reader| reader.deinit(alloc);
        readers.deinit(alloc);
    }

    var newMemTable: ?*MemTable = null;
    var blockWriter: BlockWriter = undefined;
    defer blockWriter.deinit(alloc);
    if (tableKind == .mem) {
        newMemTable = try MemTable.empty(alloc);
        blockWriter = BlockWriter.initFromMemTable(newMemTable.?);
    } else {
        var sourceItemsCount: u64 = 0;
        for (tables) |table| {
            sourceItemsCount += table.tableHeader.itemsCount;
        }
        const toCache = sourceItemsCount <= maxItemsPerCachedTable();
        blockWriter = BlockWriter.initFromDiskTable(destinationTablePath, toCache);
    }

    // try newMemTable.?.mergeBlocks(alloc, destinationTablePath, &blockWriter, &readers, &self.stopped);
    // if (newMemTable) |memTable| {
    //     _ = memTable;
    //     newMemTable = try MemTable.empty(alloc);
    // }
    //
    // const newTable = openCreatedTable(destinationTablePath, self.memTables.items, newMemTable.?);
    // try self.swapTables(alloc, newTable, tableKind);
}

// TODO: implement it, at the moment it does only mem tables merging
fn getDestinationTableKind(tables: []*Table, force: bool) TableKind {
    if (force) return .file;

    const size = getTablesSize(tables);
    if (size > getMaxInmemoryTableSize()) return .file;
    if (!areTablesMem(tables)) return .file;

    return .mem;
}

// 4mb is a minimal size for mem table,
// technically it makes minimum requirement as 1GB for the software,
// if edge use case comes up, we can lower it further up to 0.5-1mb, then configure it in build time
const minMemTableSize: u64 = 4 * 1024 * 1024;
// TODO: make it as a config field instead of calculated property
fn getMaxInmemoryTableSize() u64 {
    const conf = Conf.getConf();
    // only 10% of cache available for mem index
    // TODO: experiment with tuning cache size to 5%, 15%
    const maxmem = (conf.sys.cacheSize / 10) / maxMemTables;
    return @max(maxmem, minMemTableSize);
}

fn areTablesMem(tables: []*Table) bool {
    for (tables) |table| {
        if (table.mem) |_| {
            continue;
        } else {
            return false;
        }
    }

    return true;
}

fn getTablesSize(tables: []*Table) u64 {
    var n: u64 = 0;
    for (tables) |table| {
        n += table.size;
    }
    return n;
}

// TODO: move it to config instead of computed property
fn maxItemsPerCachedTable() u64 {
    const sysConf = Conf.getConf().sys;
    const restMem = sysConf.maxMem - sysConf.cacheSize;
    // we anticipate 4 bytes per index item in compressed form
    return @max(restMem / (4 * blocksInMemTable), minMemTableSize);
}

fn openTableReaders(alloc: Allocator, tables: []*Table) !std.ArrayList(*BlockReader) {
    var readers = try std.ArrayList(*BlockReader).initCapacity(alloc, tables.len);
    defer {
        for (readers.items) |reader| reader.deinit(alloc);
        readers.deinit(alloc);
    }
    for (tables) |table| {
        if (table.mem) |memTable| {
            const reader = try BlockReader.initFromMemTable(alloc, memTable);
            readers.appendAssumeCapacity(reader);
        } else {
            const reader = try BlockReader.initFromDiskTable(alloc, table.path);
            readers.appendAssumeCapacity(reader);
        }
    }

    return readers;
}

fn openCreatedTable(
    tablePath: []const u8,
    memTables: []*MemTable,
    newTable: *MemTable,
) *MemTable {
    const deadlineUs = flush.getFlushToDiskDeadline(memTables);
    newTable.flushAtUs = deadlineUs;
    _ = tablePath;
    return newTable;
}

fn swapTables(
    self: *IndexRecorder,
    alloc: Allocator,
    newTable: *MemTable,
    tableKind: TableKind,
) !void {
    self.memTables.clearRetainingCapacity();
    try self.memTables.append(alloc, newTable);
    _ = tableKind;
    // TODO: probably it's worth running startMemTablesMerge recurvisely here again,
    // I assume the loop keeps running and next iteration there is a single mem table,
    // so it must flush it to disk,
    // but the call might be necessary to support concurrent jobs running,
    // worth taking a metric of it
}

fn startCacheKeyInvalidator(self: *IndexRecorder) !void {
    // TODO: add time sleep jitter
    self.wg.spawnManager(startCacheKeyInvalidatorTask, .{self});
}

fn startCacheKeyInvalidatorTask(self: *IndexRecorder) void {
    while (true) {
        std.time.sleep(std.time.ns_per_s * 10);

        if (self.stopped.load(.acquire)) {
            self.invalidateStreamFilterCache();
            return;
        }

        if (self.needInvalidate.cmpxchgWeak(false, true, .release, .monotonic)) {
            self.invalidateStreamFilterCache();
        }
    }
}

fn startMemTablesFlusher(self: *IndexRecorder, _: Allocator) void {
    _ = self;
}

fn startEntriesFlusher(self: *IndexRecorder, _: Allocator) void {
    _ = self;
}
