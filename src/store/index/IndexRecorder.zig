const std = @import("std");
const Allocator = std.mem.Allocator;

const fs = @import("../../fs.zig");

const cap = @import("../table/cap.zig");

const Entries = @import("Entries.zig");
const MemBlock = @import("MemBlock.zig");
const Table = @import("Table.zig");
const MemTable = @import("MemTable.zig");
const BlockWriter = @import("BlockWriter.zig");
const BlockReader = @import("BlockReader.zig");

const flush = @import("../table/flush.zig");
const merge = @import("../table/merge.zig");
const swap = @import("../table/swap.zig");

const Conf = @import("../../Conf.zig");

const maxBlocksPerShard = 256;

// TODO: worth tuning on practice
const blocksInMemTable = 15;
const maxMemTables = 24;

const merger = merge.Merger(*Table, *MemTable, maxMemTables);
const swapper = swap.Swapper(IndexRecorder, Table);

fn sleepOrStop(stopped: *const std.atomic.Value(bool), ns: u64) void {
    const step = 250 * std.time.ns_per_ms;
    var remaining = ns;
    while (remaining > 0) {
        if (stopped.load(.acquire)) return;
        const s = @min(remaining, step);
        std.Thread.sleep(s);
        remaining -= s;
    }
}

const IndexRecorder = @This();

entries: *Entries,

blocksToFlush: std.ArrayList(*MemBlock),
mxBlocks: std.Thread.Mutex = .{},
// TODO: make it as atomic instead of locking to access this value,
// we still need mutex to access blocksToFlush
flushEntriesAtUs: i64 = std.math.maxInt(i64),
blocksThresholdToFlush: u32,

// config fields
// TODO: make it as a config access instead of a field
maxMemBlockSize: u32,

mxTables: std.Thread.Mutex = .{},
diskTables: std.ArrayList(*Table),
memTables: std.ArrayList(*Table),

concurrency: u16,
diskMergeSem: std.Thread.Semaphore,
memMergeSem: std.Thread.Semaphore,

pool: *std.Thread.Pool,
// wg holds all the running jobs
wg: std.Thread.WaitGroup = .{},
// limits amount of mem tables in order to handle too high ingestion rate,
// when mem tables are not merged fast enough
// TODO: find an optimal way to handle ingestion rate higher than merge rate
// 1. throttle ingestion: sub optimal
// 2. extend limit of inmemory tables
// 3. find a way to make flushing / merging more optimal
// 4. more aggresive memory merging
memTablesSem: std.Thread.Semaphore = .{
    .permits = maxMemTables,
},
stopped: std.atomic.Value(bool) = .init(false),

needInvalidate: std.atomic.Value(bool) = .init(false),
indexCacheKeyVersion: std.atomic.Value(u64) = .init(0),

mergeIdx: std.atomic.Value(u64),
path: []const u8,

pub fn init(alloc: Allocator, path: []const u8) !*IndexRecorder {
    std.debug.assert(path.len > 0);

    const conf = Conf.getConf();
    const concurrency = conf.server.pools.cpus;
    const entries = try Entries.init(alloc, concurrency);
    errdefer entries.deinit(alloc);

    const blocksThresholdToFlush: u32 = @intCast(entries.shards.len * maxBlocksPerShard);

    // TODO: try using list of lists instead in order not to copy data from blocks to blocksToFlush
    var blocksToFlush = try std.ArrayList(*MemBlock).initCapacity(alloc, blocksThresholdToFlush);
    errdefer blocksToFlush.deinit(alloc);

    var pool = try alloc.create(std.Thread.Pool);
    errdefer alloc.destroy(pool);
    try pool.init(.{
        .allocator = alloc,
        .n_jobs = conf.server.pools.workerThreads,
    });
    errdefer pool.deinit();

    var memTables = try std.ArrayList(*Table).initCapacity(alloc, maxMemTables);
    errdefer memTables.deinit(alloc);

    // TODO: move it to the config level and pass path as trimmed
    var trimmedPath = path[0..];
    if (std.fs.path.isSep(path[path.len - 1])) {
        trimmedPath = trimmedPath[0 .. trimmedPath.len - 1];
    }
    std.debug.assert(std.fs.path.isAbsolute(trimmedPath));

    var tables = try Table.openAll(alloc, trimmedPath);
    errdefer {
        for (tables.items) |table| table.close();
        tables.deinit(alloc);
    }

    const t = try alloc.create(IndexRecorder);
    t.* = .{
        .entries = entries,
        .blocksThresholdToFlush = blocksThresholdToFlush,
        .blocksToFlush = blocksToFlush,
        .maxMemBlockSize = Conf.getConf().app.maxIndexMemBlockSize,
        .pool = pool,
        .diskTables = tables,
        .memTables = memTables,
        .mergeIdx = .init(@intCast(std.time.nanoTimestamp())),
        .path = trimmedPath,
        .concurrency = concurrency,
        .diskMergeSem = .{
            .permits = @max(4, concurrency),
        },
        .memMergeSem = .{
            .permits = @max(4, concurrency),
        },
    };

    // the allocator is different from http life cycle,
    // but shared between all the background jobs
    // TODO: find a better allocator, perhaps an arena with regular reset

    // disk tables merge task is different,
    // it doesn't run infinitely, but runs a few merge cycles to process left overs
    // from the previous launches
    for (0..concurrency) |_| {
        t.startDiskTablesMerge(alloc);
    }

    t.startMemTablesFlusher(alloc);
    t.startMemBlockFlusher(alloc);
    t.startCacheKeyInvalidator();

    return t;
}

// TODO: find an approach to make it never fail,
// the only option it fails is OOM, so cleaning more memory in advance might be more reliable
// another problem it's hard to test it via checkAllAllocationFailures
// Then audit all deinits and use it instead
// TODO: make using this API instead of directly managing stopped state
pub fn stop(self: *IndexRecorder, alloc: Allocator) !void {
    self.stopped.store(true, .release);
    self.wg.wait();

    try self.flushForce(alloc);

    self.deinit(alloc);
}

pub fn flushForce(self: *IndexRecorder, alloc: Allocator) !void {
    // pass empty destination because we don't plan to push more data in
    var blocksDestination = std.ArrayList(*MemBlock).empty;
    defer blocksDestination.deinit(alloc);

    try self.flushMemEntries(alloc, &blocksDestination, true);
    try self.flushMemTables(alloc, true);
}

// TODO: this must assert there is no data inmemory or it flushes it immediately
// entires, blocks, memtables
pub fn deinit(self: *IndexRecorder, alloc: Allocator) void {
    // make sure deinit is never called outside of stop
    std.debug.assert(self.memTables.items.len == 0);

    for (self.blocksToFlush.items) |block| {
        block.deinit(alloc);
    }

    for (self.diskTables.items) |table| {
        table.release();
    }
    for (self.memTables.items) |table| {
        table.release();
    }

    self.entries.deinit(alloc);
    self.blocksToFlush.deinit(alloc);
    self.diskTables.deinit(alloc);
    self.memTables.deinit(alloc);
    _ = Conf.removeDiskSpace(self.path);
    self.pool.deinit();
    alloc.destroy(self.pool);
    alloc.destroy(self);
}

pub fn nextMergeIdx(self: *IndexRecorder) u64 {
    return self.mergeIdx.fetchAdd(1, .acquire);
}

pub fn add(self: *IndexRecorder, alloc: Allocator, entries: [][]const u8) !void {
    const shard = self.entries.next();
    const blocksList = try shard.add(alloc, entries, self.maxMemBlockSize);
    if (blocksList == null) return;

    var blocks = blocksList.?;
    defer blocks.deinit(alloc);
    try self.flushBlocks(alloc, blocks.items);
}

pub fn getTables(self: *IndexRecorder, alloc: Allocator) !std.ArrayList(*Table) {
    self.mxTables.lock();
    defer self.mxTables.unlock();

    const tablesLen = self.memTables.items.len + self.diskTables.items.len;
    var tables = try std.ArrayList(*Table).initCapacity(alloc, tablesLen);

    for (self.memTables.items) |table| {
        table.retain();
        tables.appendAssumeCapacity(table);
    }
    for (self.diskTables.items) |table| {
        table.retain();
        tables.appendAssumeCapacity(table);
    }

    return tables;
}

fn flushBlocks(self: *IndexRecorder, alloc: Allocator, blocks: []*MemBlock) !void {
    if (blocks.len == 0) return;

    // TODO: make a more narrow locking
    self.mxBlocks.lock();
    defer self.mxBlocks.unlock();
    if (self.blocksToFlush.items.len == 0) {
        self.flushEntriesAtUs = std.time.microTimestamp() + std.time.us_per_s;
    }

    try self.blocksToFlush.appendSlice(alloc, blocks);
    if (self.blocksToFlush.items.len >= self.blocksThresholdToFlush) {
        // TODO: metric how much capacity is actual capacity of it comparing to expected
        // TODO: this slice could have come out of a mem pool which preallocates such slices by 10x
        // and pops on demand
        var blocksToFlush = try std.ArrayList(*MemBlock).initCapacity(alloc, self.blocksToFlush.items.len);
        std.mem.swap(std.ArrayList(*MemBlock), &blocksToFlush, &self.blocksToFlush);
        defer blocksToFlush.deinit(alloc);

        try self.flushBlocksToMemTables(alloc, blocksToFlush.items, false);
    }
}

fn flushBlocksToMemTables(self: *IndexRecorder, alloc: Allocator, blocks: []*MemBlock, force: bool) !void {
    var fba = std.heap.stackFallback(2048, alloc);
    const fbaAlloc = fba.get();

    const tablesSize = (blocks.len + blocksInMemTable - 1) / blocksInMemTable;
    var memTables = try std.ArrayList(*Table).initCapacity(fbaAlloc, tablesSize);
    defer memTables.deinit(fbaAlloc);
    errdefer {
        for (memTables.items) |memTable| memTable.close();
    }

    var tail = blocks[0..];
    // TODO: benchmark parallel mem table creation
    while (tail.len > 0) {
        const offset = @min(blocksInMemTable, tail.len);
        const head = tail[0..offset];
        tail = tail[offset..];

        const memTable = try MemTable.init(alloc, head);
        const t = try Table.fromMem(alloc, memTable);
        memTables.appendAssumeCapacity(t);
    }

    const maxSize = merger.getMaxInmemoryTableSize();

    var left = try std.ArrayList(*Table).initCapacity(alloc, memTables.items.len);
    defer left.deinit(alloc);

    while (memTables.items.len > 1) {
        try mergeMemTables(alloc, &memTables);

        for (memTables.items) |table| {
            if (table.size >= maxSize) {
                try self.addToMemTables(alloc, table, force);
            } else {
                left.appendAssumeCapacity(table);
            }
        }

        memTables.clearRetainingCapacity();
        std.mem.swap(std.ArrayList(*Table), &memTables, &left);
    }

    if (memTables.items.len == 1) {
        try self.addToMemTables(alloc, memTables.items[0], force);
    }
}

/// merges mem tables to a bigger size ones
/// requires same Allocator that's used to create them,
/// because it deinits the merged ones
fn mergeMemTables(alloc: Allocator, memTables: *std.ArrayList(*Table)) !void {
    // TODO: run merging job in parallel and benchmark whether it doesn't hurt general throughput

    // TODO: take a metric to understand if capacity is enough for regular case
    var fba = std.heap.stackFallback(512, alloc);
    const fbaAlloc = fba.get();
    var mergedTables = try std.ArrayList(*Table).initCapacity(fbaAlloc, 8);
    defer mergedTables.deinit(fbaAlloc);

    std.debug.assert(memTables.items.len != 0);
    if (memTables.items.len == 1) return;

    var left = std.ArrayList(*Table).initBuffer(memTables.items[0..]);
    left.items.len = memTables.items.len;
    // var left = memTables.items[0..];
    while (left.items.len > 0) {
        const n = merger.selectTablesToMerge(&left);
        const toMerge = left.items[0..n];
        const tail = left.items[n..];
        left = std.ArrayList(*Table).initBuffer(tail);
        left.items.len = tail.len;

        const res = try MemTable.mergeMemTables(alloc, toMerge);
        for (toMerge) |t| t.close();
        const t = try Table.fromMem(alloc, res);
        try mergedTables.append(fbaAlloc, t);
    }

    memTables.clearRetainingCapacity();
    memTables.appendSliceAssumeCapacity(mergedTables.items);
}

fn addToMemTables(self: *IndexRecorder, alloc: Allocator, memTable: *Table, force: bool) !void {
    var semaphoreWaited = false; // if not stopped then wait for an available semaphore
    if (!self.stopped.load(.acquire)) {
        self.memTablesSem.wait();
        semaphoreWaited = true;
    }
    errdefer if (semaphoreWaited) self.memTablesSem.post();

    // TODO: ideally to know the amount of mem tables and call unlock without errdefer
    self.mxTables.lock();
    errdefer self.mxTables.unlock();
    try self.memTables.append(alloc, memTable);
    self.startMemTablesMerge(alloc);
    self.mxTables.unlock();

    if (force) {
        self.invalidateStreamFilterCache();
    } else {
        if (!self.needInvalidate.load(.acquire)) {
            _ = self.needInvalidate.cmpxchgWeak(false, true, .release, .monotonic);
        }
    }
}

// merge-flush
// the functions below describe merge/flush jobs
// the naming is grouped on the following levels
// 1. startX - starts an infinite (or limited) cycle of a task
// 2. runX - runs a given task that MUST be able to complete without stopped signal,
// it has a specific error handling and stopped signal

pub fn startDiskTablesMerge(self: *IndexRecorder, alloc: Allocator) void {
    if (self.stopped.load(.acquire)) {
        return;
    }

    self.pool.spawnWg(&self.wg, runDiskTablesMerger, .{ self, alloc });
}

fn runDiskTablesMerger(self: *IndexRecorder, alloc: Allocator) void {
    self.tablesMerger(alloc, &self.diskTables, &self.diskMergeSem) catch |err| {
        if (err == error.Stopped) return;

        self.stopped.store(true, .release);
        std.debug.print("failed to run disk tables merger: {s}\n", .{@errorName(err)});
    };
}

fn startMemTablesFlusher(self: *IndexRecorder, alloc: Allocator) void {
    self.pool.spawnWg(&self.wg, runMemTablesFlusher, .{ self, alloc });
}

fn runMemTablesFlusher(self: *IndexRecorder, alloc: Allocator) void {
    while (true) {
        if (self.stopped.load(.acquire)) {
            return;
        }

        self.flushMemTables(alloc, false) catch |err| {
            if (err == error.Stopped) return;

            std.debug.print("failed to run mem tables flusher: {s}\n", .{@errorName(err)});
            self.stopped.store(true, .release);
            return;
        };
        sleepOrStop(&self.stopped, std.time.ns_per_s);
    }
}

fn startMemBlockFlusher(self: *IndexRecorder, alloc: Allocator) void {
    self.pool.spawnWg(&self.wg, runMemBlockFlusher, .{ self, alloc });
}

fn runMemBlockFlusher(self: *IndexRecorder, alloc: Allocator) void {
    var blocksDestination = std.ArrayList(*MemBlock).initCapacity(alloc, self.blocksThresholdToFlush) catch {
        std.debug.print("failed to start mem blocks flusher, OOM", .{});
        self.stopped.store(true, .release);
        return;
    };
    defer blocksDestination.deinit(alloc);

    while (true) {
        if (self.stopped.load(.acquire)) {
            return;
        }

        self.flushMemEntries(alloc, &blocksDestination, false) catch |err| {
            if (err == error.Stopped) return;

            self.stopped.store(true, .release);
            std.debug.print("unexpected error on running mem blocks flusher, {s}", .{@errorName(err)});
            return;
        };
        blocksDestination.clearRetainingCapacity();
        sleepOrStop(&self.stopped, std.time.ns_per_s);
    }
}

fn startCacheKeyInvalidator(self: *IndexRecorder) void {
    self.pool.spawnWg(&self.wg, runCacheKeyInvalidator, .{self});
}

fn runCacheKeyInvalidator(self: *IndexRecorder) void {
    var ticks: u8 = 0;
    while (true) {
        // invalidate every 15 secs, check stopped every 3 secs
        sleepOrStop(&self.stopped, 3 * std.time.ns_per_s);
        ticks +%= 1;

        if (self.stopped.load(.acquire)) {
            self.invalidateStreamFilterCache();
            return;
        }

        if (ticks < 5) {
            continue;
        }
        ticks = 0;

        if (self.needInvalidate.cmpxchgWeak(true, false, .acq_rel, .acquire) == null) {
            self.invalidateStreamFilterCache();
        }
    }
}

/// it's not supposed to run at the beginning in backrgound,
/// we run it only on demand
pub fn startMemTablesMerge(self: *IndexRecorder, alloc: Allocator) void {
    if (self.stopped.load(.acquire)) return;

    self.pool.spawnWg(&self.wg, runMemTablesMerger, .{ self, alloc });
}

fn runMemTablesMerger(self: *IndexRecorder, alloc: Allocator) void {
    self.tablesMerger(alloc, &self.memTables, &self.memMergeSem) catch |err| {
        if (err == error.Stopped) return;

        self.stopped.store(true, .release);
        std.debug.print("failed to merge mem tables: {s}\n", .{@errorName(err)});
    };
}

fn flushMemTables(self: *IndexRecorder, alloc: Allocator, force: bool) !void {
    const nowUs = std.time.microTimestamp();
    const bufsize = 1024;
    // TODO: metric to understand whether it's enough
    var fba = std.heap.stackFallback(bufsize, alloc);
    const fbaAlloc = fba.get();

    var toFlush = try std.ArrayList(*Table).initCapacity(fbaAlloc, bufsize / 64);
    defer toFlush.deinit(fbaAlloc);

    self.mxTables.lock();
    errdefer self.mxTables.unlock();
    for (self.memTables.items) |memTable| {
        if (!memTable.inMerge and (force or memTable.mem.?.flushAtUs < nowUs)) {
            memTable.inMerge = true;
            try toFlush.append(fbaAlloc, memTable);
        }
    }
    self.mxTables.unlock();

    try self.flushMemTablesInChunks(alloc, toFlush);
}

fn flushMemEntries(
    self: *IndexRecorder,
    alloc: Allocator,
    blocksDestination: *std.ArrayList(*MemBlock),
    force: bool,
) !void {
    const nowUs = std.time.microTimestamp();

    self.mxBlocks.lock();
    if (force or nowUs >= self.flushEntriesAtUs) {
        std.mem.swap(std.ArrayList(*MemBlock), blocksDestination, &self.blocksToFlush);
    }
    self.mxBlocks.unlock();

    for (self.entries.shards) |*shard| {
        try shard.collectBlocks(alloc, blocksDestination, nowUs, force);
    }

    try self.flushBlocksToMemTables(alloc, blocksDestination.items, force);
}

fn flushMemTablesInChunks(self: *IndexRecorder, alloc: Allocator, toFlush: std.ArrayList(*Table)) !void {
    if (toFlush.items.len == 0) return;

    // TODO: consider running chunks merging in parallel
    var left = std.ArrayList(*Table).initBuffer(toFlush.items[0..]);
    left.items.len = toFlush.items.len;
    while (left.items.len > 0) {
        const n = merger.selectTablesToMerge(&left);
        std.debug.assert(n > 0);

        // pass stopped as null since we must be able to flush data to disk
        try self.mergeTables(alloc, left.items[0..n], true, null);
        const tail = left.items[n..];
        left = std.ArrayList(*Table).initBuffer(tail);
        left.items.len = tail.len;
    }
}

fn tablesMerger(
    self: *IndexRecorder,
    alloc: Allocator,
    tables: *std.ArrayList(*Table),
    sem: *std.Thread.Semaphore,
) anyerror!void {
    var tablesToMerge = try std.ArrayList(*Table).initCapacity(alloc, tables.items.len);
    defer tablesToMerge.deinit(alloc);

    while (true) {
        const maxDiskTableSize = cap.getMaxTableSize(self.path);

        self.mxTables.lock();
        // filteredTablesToMerge is a slice of tables ArrayList, no need to free it
        const window = merger.filterTablesToMerge(
            tables.items,
            &tablesToMerge,
            maxDiskTableSize,
        );
        self.mxTables.unlock();

        const w = window orelse return;
        const filteredTablesToMerge = tablesToMerge.items[w.lower..w.upper];
        if (filteredTablesToMerge.len == 0) return;

        // TODO: make sure error.Stopped is handled on the upper level
        sem.wait();
        errdefer sem.post();
        try self.mergeTables(alloc, filteredTablesToMerge, false, &self.stopped);
        sem.post();
        tablesToMerge.clearRetainingCapacity();
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
    stopped: ?*const std.atomic.Value(bool),
) !void {
    std.debug.assert(tables.len > 0);
    for (tables) |table| std.debug.assert(table.inMerge);

    var swapped = false;
    defer {
        if (!swapped) {
            self.mxTables.lock();
            for (tables) |table| table.inMerge = false;
            self.mxTables.unlock();
        }
    }

    const tableKind = merger.getDestinationTableKind(tables, force);

    // 1 for / and 16 for 16 bytes of idx representation,
    // we can't bitcast it to [8]u8 because we need human readlable file names
    var destinationTablePath: []u8 = "";
    errdefer if (destinationTablePath.len > 0) alloc.free(destinationTablePath);
    if (tableKind == .disk) {
        destinationTablePath = try alloc.alloc(u8, self.path.len + 1 + 16);
        const idx = self.nextMergeIdx();
        _ = try std.fmt.bufPrint(
            destinationTablePath,
            "{s}/{X:0>16}",
            .{ self.path, idx },
        );
    }

    if (force and tables.len == 1 and tables[0].mem != null) {
        const table = tables[0].mem.?;
        try table.storeToDisk(alloc, destinationTablePath);
        const newTable = try openCreatedTable(alloc, destinationTablePath, tables, null);
        try swapper.swapTables(self, alloc, tables, newTable, tableKind);
        swapped = true;
        return;
    }

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
        // TODO: test if we can record compressed size and make caching more reliable
        const fitsInCache = sourceItemsCount <= maxItemsPerCachedTable();
        blockWriter = try BlockWriter.initFromDiskTable(alloc, destinationTablePath, fitsInCache);
    }

    const tableHeader = try MemTable.mergeBlocks(
        alloc,
        &blockWriter,
        &readers,
        stopped,
    );
    if (newMemTable) |memTable| {
        memTable.tableHeader = tableHeader;
    } else {
        std.debug.assert(destinationTablePath.len > 0);
        var fbaFallback = std.heap.stackFallback(256, alloc);
        try tableHeader.writeFile(fbaFallback.get(), destinationTablePath);

        fs.syncPathAndParentDir(destinationTablePath);
    }

    const openTable = try openCreatedTable(alloc, destinationTablePath, tables, newMemTable);
    try swapper.swapTables(self, alloc, tables, openTable, tableKind);
    swapped = true;
}

// TODO: move it to config instead of computed property
fn maxItemsPerCachedTable() u64 {
    const sysConf = Conf.getConf().sys;
    const restMem = sysConf.maxMem - sysConf.cacheSize;
    // we anticipate 6 bytes per index item in compressed form
    return @max(restMem / (6 * blocksInMemTable), merge.minMemTableSize);
}

fn openTableReaders(alloc: Allocator, tables: []*Table) !std.ArrayList(*BlockReader) {
    var readers = try std.ArrayList(*BlockReader).initCapacity(alloc, tables.len);
    errdefer {
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
    alloc: Allocator,
    tablePath: []const u8,
    tables: []*Table,
    maybeMemTable: ?*MemTable,
) !*Table {
    if (maybeMemTable) |memTable| {
        memTable.flushAtUs = flush.getFlushTablesToDiskDeadline(*Table, *MemTable, tables);
        return Table.fromMem(alloc, memTable);
    }

    return Table.open(alloc, tablePath);
}

const testing = std.testing;

fn createMemTableFromItems(alloc: Allocator, items: []const []const u8) !*Table {
    var total: u32 = 0;
    for (items) |item| total += @intCast(item.len);
    var block = try MemBlock.init(alloc, total + 16);
    defer block.deinit(alloc);
    for (items) |item| {
        const ok = block.add(item);
        try testing.expect(ok);
    }
    var blocks = [_]*MemBlock{block};
    const memTable = try MemTable.init(alloc, &blocks);
    return Table.fromMem(alloc, memTable);
}

fn countMemItemsInRecorder(recorder: *IndexRecorder) u64 {
    var count: u64 = 0;
    for (recorder.memTables.items) |table| {
        count += table.tableHeader.itemsCount;
    }
    return count;
}

fn countDiskItemsInRecorder(recorder: *IndexRecorder) u64 {
    var count: u64 = 0;
    for (recorder.diskTables.items) |table| {
        count += table.tableHeader.itemsCount;
    }
    return count;
}

const stableItems = [_][]const u8{
    "item-a", "item-b", "item-c", "item-d", "item-e", "item-f", "item-g", "item-h",
};

test "IndexRecorder init and close empty dir, trim slash" {
    const alloc = testing.allocator;
    _ = try Conf.default(alloc);
    defer Conf.deinit();

    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    const rootPath = try tmp.dir.realpathAlloc(alloc, ".");
    defer alloc.free(rootPath);
    const pathWithSlash = try std.mem.concat(alloc, u8, &.{ rootPath, std.fs.path.sep_str });
    defer alloc.free(pathWithSlash);

    const recorder = try IndexRecorder.init(alloc, pathWithSlash);

    try testing.expectEqual(@as(usize, 0), recorder.diskTables.items.len);
    try testing.expectEqual(@as(usize, 0), recorder.memTables.items.len);
    try testing.expectEqual(@as(usize, 0), recorder.blocksToFlush.items.len);
    try testing.expect(std.mem.eql(u8, recorder.path, rootPath));

    try recorder.stop(alloc);
}

test "flushMemEntries non-force respects flush deadline" {
    const alloc = testing.allocator;
    _ = try Conf.default(alloc);
    defer Conf.deinit();

    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    const rootPath = try tmp.dir.realpathAlloc(alloc, ".");
    defer alloc.free(rootPath);

    const recorder = try IndexRecorder.init(alloc, rootPath);
    recorder.stopped.store(true, .release);
    recorder.wg.wait();
    defer recorder.deinit(alloc);

    var block = try MemBlock.init(alloc, 64);
    defer block.deinit(alloc);
    const ok = block.add("alpha");
    try testing.expect(ok);
    try recorder.blocksToFlush.append(alloc, block);

    var dst = try std.ArrayList(*MemBlock).initCapacity(alloc, 4);
    defer dst.deinit(alloc);

    recorder.flushEntriesAtUs = std.time.microTimestamp() + std.time.us_per_s;
    try recorder.flushMemEntries(alloc, &dst, false);
    try testing.expectEqual(@as(usize, 1), recorder.blocksToFlush.items.len);
    try testing.expectEqual(@as(usize, 0), recorder.memTables.items.len);

    recorder.flushEntriesAtUs = std.time.microTimestamp() - std.time.us_per_s;
    try recorder.flushMemEntries(alloc, &dst, false);
    try testing.expectEqual(@as(usize, 0), recorder.blocksToFlush.items.len);
    try testing.expect(recorder.memTables.items.len > 0);

    try recorder.flushForce(alloc);
}

test "mergeTables force single mem table creates disk table" {
    const alloc = testing.allocator;
    _ = try Conf.default(alloc);
    defer Conf.deinit();

    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    const rootPath = try tmp.dir.realpathAlloc(alloc, ".");
    defer alloc.free(rootPath);

    const recorder = try IndexRecorder.init(alloc, rootPath);
    recorder.stopped.store(true, .release);
    recorder.wg.wait();
    defer recorder.deinit(alloc);

    const table = try createMemTableFromItems(alloc, &.{ "k1", "k2", "k3" });
    try recorder.memTables.append(alloc, table);
    recorder.memTablesSem.wait();
    table.inMerge = true;

    var single = [_]*Table{table};
    try recorder.mergeTables(alloc, single[0..], true, null);
    try testing.expectEqual(@as(usize, 0), recorder.memTables.items.len);
    try testing.expectEqual(@as(usize, 1), recorder.diskTables.items.len);
    try testing.expect(recorder.diskTables.items[0].disk != null);
}

test "IndexRecorder add and reopen preserves item count" {
    const alloc = testing.allocator;
    _ = try Conf.default(alloc);
    defer Conf.deinit();

    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    const rootPath = try tmp.dir.realpathAlloc(alloc, ".");
    defer alloc.free(rootPath);

    const inserted: usize = 128;
    {
        const recorder = try IndexRecorder.init(alloc, rootPath);

        for (0..inserted) |i| {
            const item = stableItems[i % stableItems.len];
            var batch = [_][]const u8{item};
            try recorder.add(alloc, &batch);
        }

        recorder.stopped.store(true, .release);
        recorder.wg.wait();
        try recorder.flushForce(alloc);
        try testing.expectEqual(@as(usize, 0), recorder.memTables.items.len);
        try testing.expect(recorder.diskTables.items.len > 0);
        try testing.expectEqual(@as(u64, 0), countMemItemsInRecorder(recorder));
        try testing.expectEqual(@as(u64, inserted), countDiskItemsInRecorder(recorder));
        recorder.deinit(alloc);
    }

    {
        const reopened = try IndexRecorder.init(alloc, rootPath);
        reopened.stopped.store(true, .release);
        reopened.wg.wait();
        try testing.expect(reopened.diskTables.items.len > 0);
        try testing.expectEqual(@as(u64, 0), countMemItemsInRecorder(reopened));
        try testing.expectEqual(@as(u64, inserted), countDiskItemsInRecorder(reopened));
        reopened.deinit(alloc);
    }
}

const AddWorkerCtx = struct {
    recorder: *IndexRecorder,
    alloc: Allocator,
    workerID: usize,
    items: usize,
};

fn addWorker(ctx: *AddWorkerCtx) void {
    var i: usize = 0;
    while (i < ctx.items) : (i += 1) {
        const item = stableItems[(ctx.workerID + i) % stableItems.len];
        var batch = [_][]const u8{item};
        ctx.recorder.add(ctx.alloc, &batch) catch |err| {
            std.debug.panic("failed to add item in worker {d}: {s}", .{ ctx.workerID, @errorName(err) });
        };
    }
}

test "IndexRecorder concurrent add preserves item count" {
    const alloc = testing.allocator;
    _ = try Conf.default(alloc);
    defer Conf.deinit();

    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    const rootPath = try tmp.dir.realpathAlloc(alloc, ".");
    defer alloc.free(rootPath);

    const recorder = try IndexRecorder.init(alloc, rootPath);
    defer recorder.deinit(alloc);

    const workers = 4;
    const items_per_worker = 64;
    var ctxs: [workers]AddWorkerCtx = undefined;
    var threads: [workers]std.Thread = undefined;

    for (0..workers) |i| {
        ctxs[i] = .{
            .recorder = recorder,
            .alloc = alloc,
            .workerID = i,
            .items = items_per_worker,
        };
        threads[i] = try std.Thread.spawn(.{}, addWorker, .{&ctxs[i]});
    }
    for (threads) |t| t.join();

    recorder.stopped.store(true, .release);
    recorder.wg.wait();
    try recorder.flushForce(alloc);

    try testing.expectEqual(@as(u64, 0), countMemItemsInRecorder(recorder));
    try testing.expectEqual(@as(u64, workers * items_per_worker), countDiskItemsInRecorder(recorder));
}

test "IndexRecorder remove path after deinit" {
    const alloc = testing.allocator;
    _ = try Conf.default(alloc);
    defer Conf.deinit();

    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    const rootPath = try tmp.dir.realpathAlloc(alloc, "./");

    defer alloc.free(rootPath);

    const recorder = try IndexRecorder.init(alloc, rootPath);
    var batch = [_][]const u8{stableItems[1]};

    try recorder.add(alloc, &batch);
    // startMemTablesMerge is necessary to call before we stop the recorder
    recorder.startMemTablesMerge(alloc);

    const space = Conf.getFreeDiskSpace(rootPath);
    try testing.expectEqual(Conf.getConf().sys.diskSpace.get(rootPath).?.free, space);

    recorder.stopped.store(true, .release);
    recorder.wg.wait();
    recorder.deinit(alloc);

    try testing.expect(!Conf.getConf().sys.diskSpace.contains(rootPath));
}

test "IndexRecorder large entries write to 3 shards sequentially" {
    const alloc = testing.allocator;
    _ = try Conf.default(alloc);
    defer Conf.deinit();

    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    const rootPath = try tmp.dir.realpathAlloc(alloc, ".");
    defer alloc.free(rootPath);
    const maxIndexMemBlockSize = 32 * 1024;
    const countAdditionalEntries = Entries.maxBlocksPerShard - 1;
    const theLargest = "x" ** (maxIndexMemBlockSize);

    const recorder = try IndexRecorder.init(alloc, rootPath);
    defer {
        recorder.stopped.store(true, .release);
        recorder.wg.wait();
        recorder.deinit(alloc);
    }

    const firstShardEntries = try alloc.alloc([]const u8, Entries.maxBlocksPerShard);
    defer alloc.free(firstShardEntries);
    const secondShardEntries = try alloc.alloc([]const u8, Entries.maxBlocksPerShard);
    defer alloc.free(secondShardEntries);
    const thirdShardEntries = try alloc.alloc([]const u8, countAdditionalEntries);
    defer alloc.free(thirdShardEntries);

    for (firstShardEntries) |*entry| entry.* = theLargest;
    for (secondShardEntries) |*entry| entry.* = theLargest;
    for (thirdShardEntries) |*entry| entry.* = theLargest;

    try recorder.add(alloc, firstShardEntries);
    try recorder.add(alloc, secondShardEntries);
    try recorder.add(alloc, thirdShardEntries);

    try testing.expectEqual(@as(usize, 2 * Entries.maxBlocksPerShard), recorder.blocksToFlush.items.len);

    var blocksInShards: usize = 0;
    for (recorder.entries.shards) |shard| {
        blocksInShards += shard.blocks.items.len;
    }
    try testing.expectEqual(@as(usize, countAdditionalEntries), blocksInShards);
}
