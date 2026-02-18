const std = @import("std");
const Allocator = std.mem.Allocator;

const fs = @import("../../fs.zig");

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
    disk,
};

const maxBlocksPerShard = 256;

// TODO: worth tuning on practice
const blocksInMemTable = 15;
const maxMemTables = 24;

// we need to balance throughput and memory limits
// this number is just a guess
const amountOfTablesToMerge = 16;

const IndexRecorder = @This();

entries: *Entries,

blocksToFlush: std.ArrayList(*MemBlock),
mxBlocks: std.Thread.Mutex = .{},
// TODO: make it as atomic instead of locking to access this value,
// we still need mutex to access blocksToFlush
flushEntriesAtUs: ?i64 = null,
blocksThresholdToFlush: u32,

// config fields
// TODO: make it as a config access instead of a field
maxIndexBlockSize: u32,

stopped: std.atomic.Value(bool) = .init(false),
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
mxTables: std.Thread.Mutex = .{},
diskTables: std.ArrayList(*Table),
memTables: std.ArrayList(*Table),

diskMergeSem: std.Thread.Semaphore,
memMergeSem: std.Thread.Semaphore,

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

    var memTables = try std.ArrayList(*Table).initCapacity(alloc, maxMemTables);
    errdefer memTables.deinit(alloc);

    // TODO: move it to the config level and pass path as trimmed
    var trimmedPath = path[0..];
    if (std.fs.path.isSep(trimmedPath[trimmedPath.len])) {
        trimmedPath = trimmedPath[0 .. trimmedPath.len - 1];
    }
    std.debug.assert(std.fs.path.isAbsolute(trimmedPath));

    var tables = try Table.openAll(alloc, trimmedPath);
    errdefer {
        for (tables.items) |table| table.close(alloc);
        tables.deinit(alloc);
    }

    const diskMergeLimit = @max(4, conf.server.pools.cpus);
    const memMergeLimit = conf.server.pools.cpus;

    const t = try alloc.create(IndexRecorder);
    t.* = .{
        .entries = entries,
        .blocksThresholdToFlush = blocksThresholdToFlush,
        .blocksToFlush = blocksToFlush,
        .maxIndexBlockSize = Conf.getConf().app.maxIndexMemBlockSize,
        .pool = pool,
        .diskTables = tables,
        .memTables = memTables,
        .mergeIdx = .init(@intCast(std.time.nanoTimestamp())),
        .path = trimmedPath,
        .diskMergeSem = .{
            .permits = diskMergeLimit,
        },
        .memMergeSem = .{
            .permits = memMergeLimit,
        },
    };

    // the allocator is different from http life cycle,
    // but shared between all the background jobs
    // TODO: find a better allocator, perhaps an arena with regular reset

    // disk tables merge task is different,
    // it doesn't run infinitely, but runs a few merge cycles to process left overs
    // from the previous launches
    for (0..diskMergeLimit) |_| {
        t.startDiskTablesMerge(alloc);
    }

    t.startMemTablesFlusher(alloc);
    t.startMemBlockFlusher(alloc);
    t.pool.spawnWg(&t.wg, startCacheKeyInvalidator, .{t});

    return t;
}

pub fn deinit(self: *IndexRecorder, alloc: Allocator) void {
    self.entries.deinit(alloc);
    self.blocksToFlush.deinit(alloc);
    self.diskTables.deinit(alloc);
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
    errdefer self.mxBlocks.unlock();
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
        self.mxBlocks.unlock();
        defer blocksToFlush.deinit(alloc);

        try self.flushBlocksToMemTables(alloc, blocksToFlush.items, false);
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

fn startDiskTablesMerge(self: *IndexRecorder, alloc: Allocator) void {
    if (self.stopped.load(.acquire)) {
        return;
    }

    self.pool.spawnWg(&self.wg, runDiskTablesMerger, .{ self, alloc });
}

fn runDiskTablesMerger(self: *IndexRecorder, alloc: Allocator) void {
    self.tablesMerger(alloc, &self.diskTables, &self.diskMergeSem) catch |err| {
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

        self.flushMemTables(alloc, false);
        std.Thread.sleep(std.time.ns_per_s);
    }
}

fn startMemBlockFlusher(self: *IndexRecorder, alloc: Allocator) void {
    self.pool.spawnWg(&self.wg, runMemBlockFlusher, .{ self, alloc });
}

fn runMemBlockFlusher(self: *IndexRecorder, alloc: Allocator) void {
    var blocksDestination = std.ArrayList(*MemBlock).initCapacity(alloc, self.blocksThresholdToFlush) catch {
        std.debug.print("failed to start mem blocks flusher, OOM", .{});
        return;
    };
    defer blocksDestination.deinit(alloc);

    while (true) {
        if (self.stopped.load(.acquire)) {
            return;
        }

        self.runEntriesFlusher(alloc, &blocksDestination, false) catch |err| {
            switch (err) {
                error.OutOfMemory => {
                    std.debug.print("failed to run mem blocks flusher: OOM", .{});
                    return;
                },
                error.Stopped => {
                    return;
                },
                else => {
                    std.debug.print("unexpected error on running mem blocks flusher, {s}", .{@errorName(err)});
                    return;
                },
            }
        };
        blocksDestination.clearRetainingCapacity();
        std.Thread.sleep(std.time.ns_per_s);
    }
}

fn startCacheKeyInvalidator(self: *IndexRecorder) void {
    self.pool.spawnWg(&self.wg, runCacheKeyInvalidator, .{self});
}

fn runCacheKeyInvalidator(self: *IndexRecorder) void {
    while (true) {
        std.Thread.sleep(std.time.ns_per_s * 10);

        if (self.stopped.load(.acquire)) {
            self.invalidateStreamFilterCache();
            return;
        }

        if (self.needInvalidate.cmpxchgWeak(false, true, .release, .monotonic)) |yes| {
            if (yes) self.invalidateStreamFilterCache();
        }
    }
}

/// it's not supposed to run at the beginning in backrgound,
/// we run it only on demand
fn startMemTablesMerge(self: *IndexRecorder, alloc: Allocator) void {
    if (self.stopped.load(.acquire)) return;

    self.pool.spawnWg(&self.wg, runMemTablesMerger, .{ self, alloc });
}

fn runMemTablesMerger(self: *IndexRecorder, alloc: Allocator) void {
    self.tablesMerger(alloc, &self.memTables, &self.memMergeSem) catch |err| {
        std.debug.print("failed to merge mem tables: {s}\n", .{@errorName(err)});
    };
}

fn flushMemTables(self: *IndexRecorder, alloc: Allocator, force: bool) void {
    _ = self;
    _ = force;
    _ = alloc;
    unreachable;
}

fn runEntriesFlusher(
    self: *IndexRecorder,
    alloc: Allocator,
    blocksDestination: *std.ArrayList(*MemBlock),
    force: bool,
) !void {
    const nowUs = std.time.microTimestamp();

    self.mxBlocks.lock();
    errdefer self.mxBlocks.unlock();

    if (force) {
        std.mem.swap(std.ArrayList(*MemBlock), blocksDestination, &self.blocksToFlush);
    } else if (self.flushEntriesAtUs) |flushAtUs| {
        if (flushAtUs > nowUs) {
            std.mem.swap(std.ArrayList(*MemBlock), blocksDestination, &self.blocksToFlush);
        }
    }
    self.mxBlocks.unlock();

    for (self.entries.shards) |*shard| {
        try shard.collectBlocks(alloc, blocksDestination, nowUs, force);
    }

    try self.flushBlocksToMemTables(alloc, blocksDestination.items, force);
}

fn tablesMerger(
    self: *IndexRecorder,
    alloc: Allocator,
    tables: *std.ArrayList(*Table),
    sem: *std.Thread.Semaphore,
) anyerror!void {
    var tablesToMerge = std.ArrayList(*Table).empty;
    defer tablesToMerge.deinit(alloc);

    while (true) {
        const maxDiskTableSize = self.getMaxTableSize();

        self.mxTables.lock();
        errdefer self.mxTables.unlock();
        // filteredTablesToMerge is a slice of tables ArrayList, no need to free it
        const filteredTablesToMerge = try filterTablesToMerge(alloc, tables.items, &tablesToMerge, maxDiskTableSize);
        self.mxTables.unlock();
        if (filteredTablesToMerge.len == 0) {
            return;
        }

        // TODO: make sure error.Stopped is handled on the upper level
        sem.wait();
        errdefer sem.post();
        self.mergeTables(alloc, filteredTablesToMerge, false, &self.stopped) catch |err| {
            switch (err) {
                error.Stopped => return,
                else => return err,
            }
        };
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
    for (tables) |table| std.debug.assert(table.inMerge);

    defer {
        self.mxTables.lock();
        for (tables) |table| table.inMerge = false;
        self.mxTables.unlock();
    }

    const tableKind = getDestinationTableKind(tables, force);
    var fba = std.heap.stackFallback(64, alloc);
    const fbaAlloc = fba.get();

    // 1 for / and 16 for 16 bytes of idx representation,
    // we can't bitcast it to [8]u8 because we need human readlable file names
    var destinationTablePath: []u8 = "";
    defer if (destinationTablePath.len > 0) fbaAlloc.free(destinationTablePath);
    if (tableKind == .disk) {
        destinationTablePath = try fbaAlloc.alloc(u8, self.path.len + 1 + 16);
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
        try self.swapTables(alloc, tables, newTable, tableKind);
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
        const fitsInCache = sourceItemsCount <= maxItemsPerCachedTable();
        blockWriter = try BlockWriter.initFromDiskTable(alloc, destinationTablePath, fitsInCache);
    }

    const tableHeader = try MemTable.mergeBlocks(
        alloc,
        destinationTablePath,
        &blockWriter,
        &readers,
        stopped,
    );
    if (newMemTable) |memTable| {
        memTable.tableHeader = tableHeader;
    } else {
        fs.syncPathAndParentDir(destinationTablePath);
    }

    const openTable = try openCreatedTable(alloc, destinationTablePath, tables, newMemTable.?);
    try self.swapTables(alloc, tables, openTable, tableKind);
}

fn getDestinationTableKind(tables: []*Table, force: bool) TableKind {
    if (force) return .disk;

    const size = getTablesSize(tables);
    if (size > getMaxInmemoryTableSize()) return .disk;
    if (!areTablesMem(tables)) return .disk;

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
    alloc: Allocator,
    tablePath: []const u8,
    tables: []*Table,
    maybeMemTable: ?*MemTable,
) !*Table {
    if (maybeMemTable) |memTable| {
        memTable.flushAtUs = flush.getFlushToDiskDeadline(tables);
        return Table.fromMem(alloc, memTable);
    }

    return Table.open(alloc, tablePath);
}

fn swapTables(
    self: *IndexRecorder,
    alloc: Allocator,
    tables: []*Table,
    newTable: *Table,
    tableKind: TableKind,
) !void {
    self.mxTables.lock();
    errdefer self.mxTables.unlock();

    const removedDiskTables = removeTables(&self.memTables, tables);
    const removedMemTables = removeTables(&self.diskTables, tables);

    switch (tableKind) {
        .disk => {
            try self.diskTables.append(alloc, newTable);
            self.startDiskTablesMerge(alloc);
        },
        .mem => {
            try self.memTables.append(alloc, newTable);
            self.startMemTablesMerge(alloc);
        },
    }

    self.mxTables.unlock();

    if (removedDiskTables > 0 or tableKind == .disk) {
        try Table.writeNames(alloc, self.path, self.diskTables.items);
    }

    for (0..removedMemTables) |_| self.memTablesSem.post();
    if (tableKind == .mem) self.memTablesSem.wait();

    std.debug.assert(tables.len == removedDiskTables + removedMemTables);

    for (tables) |table| {
        // remove via reference counter,
        // it could have been open by a client.
        // order flag doesn't matter, we don't expect any other part to change it back to
        table.toRemove.store(true, .unordered);
        table.release(alloc);
    }
}

fn removeTables(tables: *std.ArrayList(*Table), remove: []*Table) u32 {
    var removed: u32 = 0;
    for (0..tables.items.len) |i| {
        for (remove) |r| {
            if (tables.items[i] == r) {
                _ = tables.swapRemove(i);
                removed += 1;
            }
        }
    }

    return removed;
}

fn getMaxTableSize(self: *IndexRecorder) u64 {
    _ = self;
    unreachable;
}

fn filterTablesToMerge(
    alloc: Allocator,
    tables: []*Table,
    toMerge: *std.ArrayList(*Table),
    maxDiskTableSize: u64,
) Allocator.Error![]*Table {
    try toMerge.ensureUnusedCapacity(alloc, tables.len);

    for (tables) |table| {
        if (!table.inMerge) {
            toMerge.appendAssumeCapacity(table);
        }
    }

    // tablesToMerge is a slice of toMerge ArrayList, no need to free it
    const tablesToMerge = filterLeveledTables(toMerge, maxDiskTableSize, amountOfTablesToMerge);
    for (tablesToMerge) |table| {
        std.debug.assert(!table.inMerge);
        table.inMerge = true;
    }

    return tablesToMerge;
}

// avoid merges where one big part is rewritten with tiny additions (leads to high write amplification)
// guess based number, might be changed on the practical data
const mergeMultiple = 2;

fn sortToMerge(toMerge: *std.ArrayList(*Table)) void {
    std.mem.sortUnstable(*Table, toMerge.items, {}, Table.lessThan);
}

fn filterLeveledTables(
    toMerge: *std.ArrayList(*Table),
    maxDiskTableSize: u64,
    maxTablesToMerge: comptime_int,
) []*Table {
    comptime if (maxTablesToMerge < 2) @compileError("maxTablesToMerge must be >= 2");

    if (toMerge.items.len < 2) return &.{};

    const maxSize = maxDiskTableSize / mergeMultiple;
    var idx: usize = 0;
    while (idx < toMerge.items.len) {
        if (toMerge.items[idx].size > maxSize) {
            _ = toMerge.swapRemove(idx);
            continue;
        }
        idx += 1;
    }

    sortToMerge(toMerge);

    // we want to merge at least a half of them
    const upperBound = @min(maxTablesToMerge, toMerge.items.len);
    const lowerBound = @max(2, (upperBound + 1) / 2);
    var maxScore: f64 = 0;
    var windowToMerge: []*Table = &.{};

    // +1 to make upperBound inclusive
    for (lowerBound..upperBound + 1) |i| {
        for (0..toMerge.items.len - i + 1) |j| {
            const mergeWindow = toMerge.items[j .. j + i];
            const largestTableSize: u64 = mergeWindow[mergeWindow.len - 1].size;

            if (mergeWindow[0].size * mergeWindow.len < largestTableSize) {
                // too much of a difference, it's not a balanced merge, unncecessary write
                continue;
            }

            var resultSize: u64 = 0;
            for (mergeWindow) |table| resultSize += table.size;
            // further iterations bring only bigger tables
            if (resultSize > maxDiskTableSize) break;

            const score: f64 = @as(f64, @floatFromInt(resultSize)) / @as(f64, @floatFromInt(largestTableSize));
            if (score < maxScore) continue;

            maxScore = score;
            windowToMerge = mergeWindow;
        }
    }

    const minScore: f64 = @max(@as(f64, @floatFromInt(maxTablesToMerge)) / 2, 2, mergeMultiple);
    if (maxScore < minScore) {
        // nothing to merge
        toMerge.clearRetainingCapacity();
        return &.{};
    }

    return windowToMerge;
}
