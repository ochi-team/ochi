const std = @import("std");
const Allocator = std.mem.Allocator;
const Io = std.Io;

const fs = @import("../../fs.zig");

const cap = @import("../table/cap.zig");

const Entries = @import("Entries.zig");
const MemBlock = @import("MemBlock.zig");
const Table = @import("Table.zig");
const MemTable = @import("MemTable.zig");
const BlockWriter = @import("BlockWriter.zig");
const BlockReader = @import("BlockReader.zig");
const LookupTable = @import("lookup/LookupTable.zig");

const flush = @import("../table/flush.zig");
const merge = @import("../table/merge.zig");
const swap = @import("../table/swap.zig");

const Conf = @import("../../Conf.zig");
const Runtime = @import("../../Runtime.zig");

// TODO: worth tuning on practice
const blocksInMemTable = 15;
const maxMemTables = 24;

const merger = merge.Merger(*Table, *MemTable, maxMemTables);
const swapper = swap.Swapper(IndexRecorder, Table);

fn sleepOrStop(io: Io, stopped: *const std.atomic.Value(bool), ns: u64) void {
    // TODO: make this interval configurable,
    // it must be shorter for tests and longer for production
    const step = 250;
    var remaining = ns;
    while (remaining > 0) {
        if (stopped.load(.acquire)) return;
        const s = @min(remaining, step);
        try Io.sleep(io, .fromMilliseconds(50), .real);
        remaining -= s;
    }
}

const Wg = struct {
    pub fn wait(self: *Wg) void {
        _ = self;
    }
};

// TODO rewrite using new Io
const IndexRecorder = @This();

entries: *Entries,
wg: Wg,

blocksToFlush: std.ArrayList(*MemBlock),
mxBlocks: Io.Mutex = .init,
// TODO: make it as atomic instead of locking to access this value,
// we still need mutex to access blocksToFlush (mxBlocks)
flushEntriesAtUs: i64 = std.math.maxInt(i64),
blocksThresholdToFlush: u32,

// config fields
// TODO: make it as a config access instead of a field
maxMemBlockSize: u32,

mxTables: Io.Mutex = .init,
diskTables: std.ArrayList(*Table),
memTables: std.ArrayList(*Table),

concurrency: u16,
diskMergeSem: Io.Semaphore,
memMergeSem: Io.Semaphore,

stopped: std.atomic.Value(bool) = .init(false),
// limits amount of mem tables in order to handle too high ingestion rate,
// when mem tables are not merged fast enough
// TODO: find an optimal way to handle ingestion rate higher than merge rate
// 1. throttle ingestion: sub optimal
// 2. extend limit of inmemory tables
// 3. find a way to make flushing / merging more optimal
// 4. more aggresive memory merging
memTablesSem: Io.Semaphore = .{
    .permits = maxMemTables,
},

needInvalidate: std.atomic.Value(bool) = .init(false),
indexCacheKeyVersion: std.atomic.Value(u64) = .init(0),

mergeIdx: std.atomic.Value(u64),
path: []const u8,
runtime: *Runtime,

pub fn init(io: Io, alloc: Allocator, path: []const u8, runtime: *Runtime) !*IndexRecorder {
    std.debug.assert(std.fs.path.isAbsolute(path));
    std.debug.assert(path[path.len - 1] != std.fs.path.sep);

    const concurrency = runtime.cpus;

    const entries = try Entries.init(alloc, concurrency);
    errdefer entries.deinit(alloc);

    const blocksThresholdToFlush: u32 = @intCast(entries.shards.len * Entries.maxBlocksPerShard);

    // TODO: try using list of lists instead in order not to copy data from blocks to blocksToFlush
    var blocksToFlush = try std.ArrayList(*MemBlock).initCapacity(alloc, blocksThresholdToFlush);
    errdefer blocksToFlush.deinit(alloc);

    var memTables = try std.ArrayList(*Table).initCapacity(alloc, maxMemTables);
    errdefer memTables.deinit(alloc);

    var tables = try Table.openAll(io, alloc, path);
    errdefer {
        for (tables.items) |table| table.close(io);
        tables.deinit(alloc);
    }

    const t = try alloc.create(IndexRecorder);
    t.* = .{
        .entries = entries,
        .wg = .{},
        .blocksThresholdToFlush = blocksThresholdToFlush,
        .blocksToFlush = blocksToFlush,
        .maxMemBlockSize = Conf.getConf().app.maxIndexMemBlockSize,
        .diskTables = tables,
        .memTables = memTables,
        .mergeIdx = .init(@intCast(Io.Timestamp.now(io, .real).nanoseconds)),
        .path = path,
        .runtime = runtime,
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

pub fn createDir(io: Io, path: []const u8) void {
    fs.createDirAssert(io, path);
    fs.syncPathAndParentDir(io, path);
}

// TODO: find an approach to make it never fail,
// the only option it fails is OOM, so cleaning more memory in advance might be more reliable
// another problem it's hard to test it via checkAllAllocationFailures
// Then audit all deinits and use it instead
// TODO: make using this API instead of directly managing stopped state
pub fn stop(self: *IndexRecorder, io: Io, alloc: Allocator) !void {
    self.stopped.store(true, .release);

    try self.flushForce(io, alloc);

    self.deinit(io, alloc);
}

pub fn flushForce(self: *IndexRecorder, io: Io, alloc: Allocator) !void {
    // pass empty destination because we don't plan to push more data in
    var blocksDestination = std.ArrayList(*MemBlock).empty;
    defer blocksDestination.deinit(alloc);

    try self.flushMemEntries(io, alloc, &blocksDestination, true);
    try self.flushMemTables(io, alloc, true);
}

// TODO: this must assert there is no data inmemory or it flushes it immediately
// entires, blocks, memtables
pub fn deinit(self: *IndexRecorder, io: Io, alloc: Allocator) void {
    // make sure deinit is never called outside of stop
    std.debug.assert(self.blocksToFlush.items.len == 0);
    std.debug.assert(self.memTables.items.len == 0);

    for (self.blocksToFlush.items) |block| {
        block.deinit(alloc);
    }

    for (self.diskTables.items) |table| {
        table.release(io);
    }
    for (self.memTables.items) |table| {
        table.release(io);
    }

    self.entries.deinit(alloc);
    self.blocksToFlush.deinit(alloc);
    self.diskTables.deinit(alloc);
    self.memTables.deinit(alloc);
    alloc.destroy(self);
}

pub fn nextMergeIdx(self: *IndexRecorder) u64 {
    return self.mergeIdx.fetchAdd(1, .acquire);
}

pub fn add(self: *IndexRecorder, io: Io, alloc: Allocator, entries: []const []const u8) !void {
    var entryIndex: usize = 0;

    while (entryIndex < entries.len) {
        const shard = self.entries.next();
        const blocksListResult = try shard.add(io, alloc, entries[entryIndex..], self.maxMemBlockSize);

        var blocksList = blocksListResult orelse return;
        defer blocksList.blocksToFlush.deinit(alloc);

        try self.flushBlocks(io, alloc, blocksList.blocksToFlush.items);
        entryIndex += blocksList.gatheredEntriesCount;
    }
}

pub fn getTables(self: *IndexRecorder, io: Io, alloc: Allocator) !std.ArrayList(*Table) {
    self.mxTables.lockUncancelable(io);
    defer self.mxTables.unlock(io);

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

fn flushBlocks(self: *IndexRecorder, io: Io, alloc: Allocator, blocks: []*MemBlock) !void {
    if (blocks.len == 0) return;

    // TODO: make a more narrow locking, ideally before we make flushEntriesAt field as atomic
    self.mxBlocks.lockUncancelable(io);
    defer self.mxBlocks.unlock(io);

    if (self.blocksToFlush.items.len == 0) {
        self.flushEntriesAtUs = Io.Timestamp.now(io, .real).toMicroseconds() + std.time.us_per_s;
    }

    try self.blocksToFlush.appendSlice(alloc, blocks);
    if (self.blocksToFlush.items.len >= self.blocksThresholdToFlush) {
        // TODO: metric how much capacity is actual capacity of it comparing to expected
        // TODO: this slice could have come out of a mem pool
        // and pops on demand
        var blocksToFlush = try std.ArrayList(*MemBlock).initCapacity(alloc, self.blocksToFlush.items.len);
        std.mem.swap(std.ArrayList(*MemBlock), &blocksToFlush, &self.blocksToFlush);
        defer blocksToFlush.deinit(alloc);

        try self.flushBlocksToMemTables(io, alloc, blocksToFlush.items, false);
    }
}

fn flushBlocksToMemTables(self: *IndexRecorder, io: Io, alloc: Allocator, blocks: []*MemBlock, force: bool) !void {
    std.debug.assert(blocks.len > 0);

    // enough for 256 tables, which a way beyond the expected amount
    var fba = std.heap.stackFallback(2048, alloc);
    const fbaAlloc = fba.get();

    const tablesSize = (blocks.len + blocksInMemTable - 1) / blocksInMemTable;
    var memTables = try std.ArrayList(*Table).initCapacity(fbaAlloc, tablesSize);
    defer memTables.deinit(fbaAlloc);
    errdefer {
        for (memTables.items) |memTable| memTable.close(io);
    }

    var tail = blocks[0..];
    // TODO: benchmark parallel mem table creation
    while (tail.len > 0) {
        const offset = @min(blocksInMemTable, tail.len);
        const head = tail[0..offset];
        tail = tail[offset..];

        const memTable = try MemTable.init(io, alloc, head);
        const t = try Table.fromMem(alloc, memTable);
        memTables.appendAssumeCapacity(t);
    }

    const maxSize = merger.getMaxInmemoryTableSize(self.runtime.cacheSize);

    var left = try std.ArrayList(*Table).initCapacity(alloc, memTables.items.len);
    defer left.deinit(alloc);

    // TODO: consider skipping this step and directly append tables to its collection,
    // it requires another way to handle mem tables semaphore,
    // but might reduce the load on merging small tables
    while (memTables.items.len > 1) {
        try mergeMemTables(io, alloc, &memTables);

        for (memTables.items) |table| {
            if (table.size >= maxSize) {
                try self.addToMemTables(io, alloc, table, force);
            } else {
                left.appendAssumeCapacity(table);
            }
        }

        memTables.clearRetainingCapacity();
        std.mem.swap(std.ArrayList(*Table), &memTables, &left);
    }

    if (memTables.items.len == 1) {
        try self.addToMemTables(io, alloc, memTables.items[0], force);
    }
}

/// merges mem tables to a bigger size ones
/// requires same Allocator that's used to create them,
/// because it deinits the merged ones
fn mergeMemTables(io: Io, alloc: Allocator, memTables: *std.ArrayList(*Table)) !void {
    // TODO: run merging job in parallel and benchmark whether it doesn't hurt general throughput

    // TODO: take a metric to understand if capacity is enough for regular case
    var fba = std.heap.stackFallback(512, alloc);
    const fbaAlloc = fba.get();
    var mergedTables = try std.ArrayList(*Table).initCapacity(fbaAlloc, 8);
    defer mergedTables.deinit(fbaAlloc);

    var memToMerge = try std.ArrayList(*MemTable).initCapacity(fbaAlloc, 8);
    defer memToMerge.deinit(fbaAlloc);

    std.debug.assert(memTables.items.len != 0);
    if (memTables.items.len == 1) return;

    var left = memTables.items[0..];
    while (left.len > 0) {
        var leftArray = std.ArrayList(*Table).initBuffer(left);
        leftArray.items.len = left.len;

        const n = merger.selectTablesToMerge(&leftArray);
        const toMerge = left[0..n];
        left = left[n..];

        try memToMerge.ensureUnusedCapacity(fbaAlloc, toMerge.len);
        for (toMerge) |table| {
            const mem = table.mem orelse {
                std.debug.panic("mergeMemTables expects mem tables only", .{});
            };
            memToMerge.appendAssumeCapacity(mem);
        }

        // TODO: I don't need it, we already have merging from []*Table, replace it
        const res = try MemTable.mergeMemTables(io, alloc, memToMerge.items);
        memToMerge.clearRetainingCapacity();

        for (toMerge) |t| t.close(io);
        const t = try Table.fromMem(alloc, res);
        try mergedTables.append(fbaAlloc, t);
    }

    // TODO: make it in place overwriting instead of holding a copy on stack
    memTables.clearRetainingCapacity();
    memTables.appendSliceAssumeCapacity(mergedTables.items);
}

fn addToMemTables(self: *IndexRecorder, io: Io, alloc: Allocator, memTable: *Table, force: bool) !void {
    // TODO: add a limit to wait max 3 minutes, otherwise something is broken

    var semaphoreAcquired = false;
    while (true) {
        // TODO removed with no replacement
        // self.memTablesSem.timedWait(std.time.ns_per_s) catch |err| {
        //     switch (err) {
        //         error.Timeout => {
        //             if (self.stopped.load(.acquire)) {
        //                 // TODO: audit all semaphore as a state usage, it can happen
        //                 // a background task fails and  doesn't post semaphore back
        //
        //                 // if force we don't care about semaphore, just need to flush it
        //                 if (force) break;
        //                 return error.Stopped;
        //             }
        //         },
        //     }
        //     continue;
        // };
        //
        semaphoreAcquired = true;
        break;
    }

    // TODO: ideally to know the amount of mem tables and call unlock it branchless
    self.mxTables.lockUncancelable(io);
    self.memTables.append(alloc, memTable) catch {
        if (semaphoreAcquired) self.memTablesSem.post(io);
        self.mxTables.unlock(io);
        return;
    };
    self.startMemTablesMerge(alloc);
    self.mxTables.unlock(io);

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
    _ = self;
    _ = alloc;
}

fn runDiskTablesMerger(self: *IndexRecorder, alloc: Allocator) void {
    self.tablesMerger(alloc, &self.diskTables, &self.diskMergeSem) catch |err| {
        if (err == error.Stopped) return;

        self.stopped.store(true, .release);
        std.debug.print("failed to run disk tables merger: {s}\n", .{@errorName(err)});
    };
}

fn startMemTablesFlusher(self: *IndexRecorder, alloc: Allocator) void {
    _ = self;
    _ = alloc;
}

fn runMemTablesFlusher(self: *IndexRecorder, io: Io, alloc: Allocator) void {
    while (true) {
        if (self.stopped.load(.acquire)) {
            return;
        }

        self.flushMemTables(io, alloc, false) catch |err| {
            if (err == error.Stopped) return;

            std.debug.print("failed to run mem tables flusher: {s}\n", .{@errorName(err)});
            self.stopped.store(true, .release);
            return;
        };
        sleepOrStop(&self.stopped, std.time.ns_per_s);
    }
}

fn startMemBlockFlusher(self: *IndexRecorder, alloc: Allocator) void {
    _ = self;
    _ = alloc;
}

fn runMemBlockFlusher(self: *IndexRecorder, io: Io, alloc: Allocator) void {
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

        self.flushMemEntries(io, alloc, &blocksDestination, false) catch |err| {
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
    _ = self;
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
    _ = self;
    _ = alloc;
}

fn runMemTablesMerger(self: *IndexRecorder, alloc: Allocator) void {
    self.tablesMerger(alloc, &self.memTables, &self.memMergeSem) catch |err| {
        if (err == error.Stopped) return;

        self.stopped.store(true, .release);
        std.debug.print("failed to merge mem tables: {s}\n", .{@errorName(err)});
    };
}

fn flushMemTables(self: *IndexRecorder, io: Io, alloc: Allocator, force: bool) !void {
    const nowUs = Io.Timestamp.now(io, .real).toMicroseconds();
    const bufsize = 256;
    // TODO: metric to understand whether it's enough
    var fba = std.heap.stackFallback(bufsize, alloc);
    const fbaAlloc = fba.get();

    var toFlush = try std.ArrayList(*Table).initCapacity(fbaAlloc, bufsize / @sizeOf(*Table));
    defer toFlush.deinit(fbaAlloc);

    self.mxTables.lockUncancelable(io);
    for (self.memTables.items) |memTable| {
        if (!memTable.inMerge and (force or memTable.mem.?.flushAtUs < nowUs)) {
            memTable.inMerge = true;
            toFlush.append(fbaAlloc, memTable) catch |err| {
                for (toFlush.items) |table| table.inMerge = false;
                self.mxTables.unlock(io);
                return err;
            };
        }
    }
    self.mxTables.unlock(io);

    try self.flushMemTablesInChunks(io, alloc, toFlush);
}

fn flushMemEntries(
    self: *IndexRecorder,
    io: Io,
    alloc: Allocator,
    blocksDestination: *std.ArrayList(*MemBlock),
    force: bool,
) !void {
    const nowUs = Io.Timestamp.now(io, .real).toMicroseconds();

    self.mxBlocks.lockUncancelable(io);
    if (force or nowUs >= self.flushEntriesAtUs) {
        std.mem.swap(std.ArrayList(*MemBlock), blocksDestination, &self.blocksToFlush);
    }
    self.mxBlocks.unlock(io);

    for (self.entries.shards) |*shard| {
        try shard.collectBlocks(io, alloc, blocksDestination, nowUs, force);
    }

    if (blocksDestination.items.len > 0) try self.flushBlocksToMemTables(io, alloc, blocksDestination.items, force);
}

fn flushMemTablesInChunks(self: *IndexRecorder, io: Io, alloc: Allocator, toFlush: std.ArrayList(*Table)) !void {
    if (toFlush.items.len == 0) return;

    // TODO: consider running chunks merging in parallel
    var left = std.ArrayList(*Table).initBuffer(toFlush.items[0..]);
    left.items.len = toFlush.items.len;
    while (left.items.len > 0) {
        const n = merger.selectTablesToMerge(&left);
        std.debug.assert(n > 0);

        // pass stopped as null since we must be able to flush data to disk
        try self.mergeTables(io, alloc, left.items[0..n], true, null);
        const tail = left.items[n..];
        left = std.ArrayList(*Table).initBuffer(tail);
        left.items.len = tail.len;
    }
}

fn tablesMerger(
    self: *IndexRecorder,
    io: Io,
    alloc: Allocator,
    tables: *std.ArrayList(*Table),
    sem: *Io.Semaphore,
) anyerror!void {
    var tablesToMerge = std.ArrayList(*Table).empty;
    defer tablesToMerge.deinit(alloc);

    while (true) {
        const maxDiskTableSize = cap.getMaxTableSize(self.runtime.getFreeDiskSpace(io));

        self.mxTables.lockUncancelable(io);
        // TODO: we have to know the max amount of tables in advance
        tablesToMerge.ensureUnusedCapacity(alloc, tables.items.len) catch |err| {
            self.mxTables.unlock(io);
            return err;
        };
        // filteredTablesToMerge is a slice of tables ArrayList, no need to free it
        const window = merger.filterTablesToMerge(
            tables.items,
            &tablesToMerge,
            maxDiskTableSize,
        );
        self.mxTables.unlock(io);

        const w = window orelse return;
        const filteredTablesToMerge = tablesToMerge.items[w.lower..w.upper];
        if (filteredTablesToMerge.len == 0) return;

        // TODO: make sure error.Stopped is handled on the upper level
        sem.waitUncancelable(io);
        errdefer sem.post(io);
        try self.mergeTables(io, alloc, filteredTablesToMerge, false, &self.stopped);
        sem.post(io);
        tablesToMerge.clearRetainingCapacity();
    }
}

fn invalidateStreamFilterCache(self: *IndexRecorder) void {
    _ = self.indexCacheKeyVersion.fetchAdd(1, .acquire);
}

pub fn mergeTables(
    self: *IndexRecorder,
    io: Io,
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
            self.mxTables.lock(io) catch {};
            for (tables) |table| table.inMerge = false;
            self.mxTables.unlock(io);
        }
    }

    const maxInmemoryTableSize = merger.getMaxInmemoryTableSize(self.runtime.cacheSize);
    const tableKind = merger.getDestinationTableKind(tables, force, maxInmemoryTableSize);

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
        try table.storeToDisk(io, alloc, destinationTablePath);
        const newTable = try openCreatedTable(io, alloc, destinationTablePath, tables, null);
        try swapper.swapTables(self, io, alloc, tables, newTable, tableKind);
        swapped = true;
        return;
    }

    var readers = try openTableReaders(io, alloc, tables);
    defer {
        for (readers.items) |reader| reader.deinit(alloc);
        readers.deinit(alloc);
    }

    // TODO: block writer deinit might be called before it's actually created,
    // if we get rid of all the undefined's it's solved
    var newMemTable: ?*MemTable = null;
    var blockWriter: BlockWriter = blk: {
        if (tableKind == .mem) {
            newMemTable = try MemTable.empty(alloc);
            break :blk BlockWriter.initFromMemTable(newMemTable.?);
        } else {
            var sourceItemsCount: u64 = 0;
            for (tables) |table| {
                sourceItemsCount += table.tableHeader.entriesCount;
            }
            // TODO: test if we can record compressed size and make caching more reliable
            const fitsInCache = sourceItemsCount <= maxItemsPerCachedTable(self.runtime.maxMem, self.runtime.cacheSize);
            break :blk try BlockWriter.initFromDiskTable(io, alloc, destinationTablePath, fitsInCache);
        }
    };
    defer blockWriter.deinit(alloc);

    const tableHeader = MemTable.mergeBlocks(
        io,
        alloc,
        &blockWriter,
        &readers,
        stopped,
    ) catch |err| {
        switch (err) {
            error.Stopped => {
                if (destinationTablePath.len > 0) {
                    // TODO removed with no replacement
                    // std.fs.deleteTreeAbsolute(destinationTablePath) catch |deleteErr| {
                    //     std.debug.print(
                    //         "failed to delete half way merged index table after stopped: {s}\n",
                    //         .{@errorName(deleteErr)},
                    //     );
                    // };
                }
                return err;
            },
            else => {
                std.debug.print("failed to merge tables: {s}\n", .{@errorName(err)});
                return err;
            },
        }
    };
    if (newMemTable) |memTable| {
        memTable.tableHeader = tableHeader;
    } else {
        std.debug.assert(destinationTablePath.len > 0);
        var fbaFallback = std.heap.stackFallback(256, alloc);
        // TODO: pass table header to openining a table and use it instead of reading from a file,
        // write a test in advance to confirm it's exact same header
        defer tableHeader.deinit(alloc);
        try tableHeader.writeFile(io, fbaFallback.get(), destinationTablePath);

        fs.syncPathAndParentDir(io, destinationTablePath);
    }

    const openTable = try openCreatedTable(io, alloc, destinationTablePath, tables, newMemTable);
    try swapper.swapTables(self, io, alloc, tables, openTable, tableKind);
    swapped = true;
}

// TODO: move it to config instead of computed property
fn maxItemsPerCachedTable(maxMem: u64, cacheSize: u64) u64 {
    const restMem = maxMem - cacheSize;
    // we anticipate 6 bytes per index item in compressed form
    return @max(restMem / (6 * blocksInMemTable), merge.minMemTableSize);
}

fn openTableReaders(io: Io, alloc: Allocator, tables: []*Table) !std.ArrayList(*BlockReader) {
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
            const reader = try BlockReader.initFromDiskTable(io, alloc, table.path);
            readers.appendAssumeCapacity(reader);
        }
    }

    return readers;
}

fn openCreatedTable(
    io: Io,
    alloc: Allocator,
    tablePath: []const u8,
    tables: []*Table,
    maybeMemTable: ?*MemTable,
) !*Table {
    if (maybeMemTable) |memTable| {
        memTable.flushAtUs = flush.getFlushTablesToDiskDeadline(io, *Table, *MemTable, tables);
        return Table.fromMem(alloc, memTable);
    }

    return Table.open(io, alloc, tablePath);
}

const testing = std.testing;

fn createMemTableFromItems(io: Io, alloc: Allocator, items: []const []const u8) !*Table {
    var total: u32 = 0;
    for (items) |item| total += @intCast(item.len);
    var block = try MemBlock.init(alloc, total + 16);
    defer block.deinit(alloc);
    for (items) |item| {
        const ok = block.add(item);
        try testing.expect(ok);
    }
    var blocks = [_]*MemBlock{block};
    const memTable = try MemTable.init(io, alloc, &blocks);
    return Table.fromMem(alloc, memTable);
}

fn countMemItemsInRecorder(recorder: *IndexRecorder) u64 {
    var count: u64 = 0;
    for (recorder.memTables.items) |table| {
        count += table.tableHeader.entriesCount;
    }
    return count;
}

fn countDiskItemsInRecorder(recorder: *IndexRecorder) u64 {
    var count: u64 = 0;
    for (recorder.diskTables.items) |table| {
        count += table.tableHeader.entriesCount;
    }
    return count;
}

const stableItems = [_][]const u8{
    "item-a", "item-b", "item-c", "item-d", "item-e", "item-f", "item-g", "item-h",
};

test "flushMemEntries non-force respects flush deadline" {
    const alloc = testing.allocator;
    const io = testing.io;

    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    const rootPath = try tmp.dir.realPathFileAlloc(io, ".", alloc);
    defer alloc.free(rootPath);

    const runtime = try Runtime.init(io, alloc, rootPath, 0.5);
    defer runtime.deinit(alloc);

    const recorder = try IndexRecorder.init(io, alloc, rootPath, runtime);
    recorder.stopped.store(true, .release);
    defer recorder.deinit(io, alloc);

    var block = try MemBlock.init(alloc, 64);
    defer block.deinit(alloc);
    const ok = block.add("alpha");
    try testing.expect(ok);
    try recorder.blocksToFlush.append(alloc, block);

    var dst = try std.ArrayList(*MemBlock).initCapacity(alloc, 4);
    defer dst.deinit(alloc);

    recorder.flushEntriesAtUs = Io.Timestamp.now(io, .real).toMicroseconds() + std.time.us_per_s;
    try recorder.flushMemEntries(io, alloc, &dst, false);
    try testing.expectEqual(1, recorder.blocksToFlush.items.len);
    try testing.expectEqual(0, recorder.memTables.items.len);

    recorder.flushEntriesAtUs = Io.Timestamp.now(io, .real).toMicroseconds() - std.time.us_per_s;
    try recorder.flushMemEntries(io, alloc, &dst, false);
    try testing.expectEqual(0, recorder.blocksToFlush.items.len);
    try testing.expect(recorder.memTables.items.len > 0);

    try recorder.flushForce(io, alloc);
}

test "mergeTables force single mem table creates disk table" {
    const alloc = testing.allocator;
    const io = testing.io;

    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    const rootPath = try tmp.dir.realPathFileAlloc(io, ".", alloc);
    defer alloc.free(rootPath);

    const runtime = try Runtime.init(io, alloc, rootPath, 0.5);
    defer runtime.deinit(alloc);

    const recorder = try IndexRecorder.init(io, alloc, rootPath, runtime);
    recorder.stopped.store(true, .release);
    defer recorder.deinit(io, alloc);

    const table = try createMemTableFromItems(io, alloc, &.{ "k1", "k2", "k3" });
    try recorder.memTables.append(alloc, table);
    recorder.memTablesSem.waitUncancelable(io);
    table.inMerge = true;

    var single = [_]*Table{table};
    try recorder.mergeTables(io, alloc, single[0..], true, null);
    try testing.expectEqual(@as(usize, 0), recorder.memTables.items.len);
    try testing.expectEqual(@as(usize, 1), recorder.diskTables.items.len);
    try testing.expect(recorder.diskTables.items[0].disk != null);
}

test "IndexRecorder add and reopen preserves item count" {
    const alloc = testing.allocator;
    const io = testing.io;

    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    const rootPath = try tmp.dir.realPathFileAlloc(io, ".", alloc);
    defer alloc.free(rootPath);

    const inserted: usize = 128;
    {
        const runtime = try Runtime.init(io, alloc, rootPath, 0.5);
        defer runtime.deinit(alloc);

        const recorder = try IndexRecorder.init(io, alloc, rootPath, runtime);
        defer recorder.deinit(io, alloc);

        for (0..inserted) |i| {
            const item = stableItems[i % stableItems.len];
            var batch = [_][]const u8{item};
            try recorder.add(io, alloc, &batch);
        }

        recorder.stopped.store(true, .release);
        try recorder.flushForce(io, alloc);
        try testing.expectEqual(@as(usize, 0), recorder.memTables.items.len);
        try testing.expect(recorder.diskTables.items.len > 0);
        try testing.expectEqual(@as(u64, 0), countMemItemsInRecorder(recorder));
        try testing.expectEqual(@as(u64, inserted), countDiskItemsInRecorder(recorder));
    }

    {
        const runtime = try Runtime.init(io, alloc, rootPath, 0.5);
        defer runtime.deinit(alloc);

        const reopened = try IndexRecorder.init(io, alloc, rootPath, runtime);
        defer reopened.deinit(io, alloc);
        reopened.stopped.store(true, .release);
        try testing.expect(reopened.diskTables.items.len > 0);
        try testing.expectEqual(@as(u64, 0), countMemItemsInRecorder(reopened));
        try testing.expectEqual(@as(u64, inserted), countDiskItemsInRecorder(reopened));
    }
}

const AddWorkerCtx = struct {
    io: Io,
    alloc: Allocator,
    recorder: *IndexRecorder,
    workerID: usize,
    items: usize,
};

fn addWorker(ctx: *AddWorkerCtx) void {
    var i: usize = 0;
    while (i < ctx.items) : (i += 1) {
        const item = stableItems[(ctx.workerID + i) % stableItems.len];
        var batch = [_][]const u8{item};
        ctx.recorder.add(ctx.io, ctx.alloc, &batch) catch |err| {
            std.debug.print("failed to add item in worker {d}: {s}\n", .{ ctx.workerID, @errorName(err) });
            return;
        };
    }
}

test "IndexRecorder concurrent add preserves item count" {
    const alloc = testing.allocator;
    const io = testing.io;

    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    const rootPath = try tmp.dir.realPathFileAlloc(io, ".", alloc);
    defer alloc.free(rootPath);

    const runtime = try Runtime.init(io, alloc, rootPath, 0.5);
    defer runtime.deinit(alloc);

    const recorder = try IndexRecorder.init(io, alloc, rootPath, runtime);
    defer recorder.deinit(io, alloc);

    var g: std.Io.Group = .init;
    errdefer g.cancel(io);

    const workers = 4;
    const items_per_worker = 64;
    var ctxs: [workers]AddWorkerCtx = undefined;

    for (0..workers) |i| {
        ctxs[i] = .{
            .io = io,
            .alloc = alloc,
            .recorder = recorder,
            .workerID = i,
            .items = items_per_worker,
        };
        g.async(io, addWorker, .{&ctxs[i]});
    }

    try g.await(io);

    recorder.stopped.store(true, .release);
    try recorder.flushForce(io, alloc);

    try testing.expectEqual(@as(u64, 0), countMemItemsInRecorder(recorder));
    try testing.expectEqual(@as(u64, workers * items_per_worker), countDiskItemsInRecorder(recorder));
}

test "IndexRecorder reads free disk space from runtime" {
    const alloc = testing.allocator;
    const io = testing.io;

    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    const rootPath = try tmp.dir.realPathFileAlloc(io, "./", alloc);

    defer alloc.free(rootPath);

    const runtime = try Runtime.init(io, alloc, rootPath, 0.5);
    defer runtime.deinit(alloc);

    const recorder = try IndexRecorder.init(io, alloc, rootPath, runtime);
    var batch = [_][]const u8{stableItems[1]};

    try recorder.add(io, alloc, &batch);
    // startMemTablesMerge is necessary to call before we stop the recorder
    recorder.startMemTablesMerge(alloc);

    const firstSpace = runtime.getFreeDiskSpace(io);
    const secondSpace = runtime.getFreeDiskSpace(io);
    try testing.expect(firstSpace > 0);
    try testing.expectEqual(firstSpace, secondSpace);

    recorder.stopped.store(true, .release);
    recorder.deinit(io, alloc);
}

test "IndexRecorder large entries write to 3 shards sequentially" {
    const alloc = testing.allocator;
    const io = testing.io;

    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    const rootPath = try tmp.dir.realPathFileAlloc(io, ".", alloc);
    defer alloc.free(rootPath);
    const maxIndexMemBlockSize = 32 * 1024;
    const countAdditionalEntries = Entries.maxBlocksPerShard - 1;
    const theLargest = "x" ** (maxIndexMemBlockSize);

    const runtime = try Runtime.init(io, alloc, rootPath, 0.5);
    defer runtime.deinit(alloc);

    const recorder = try IndexRecorder.init(io, alloc, rootPath, runtime);

    const firstShardEntries = try alloc.alloc([]const u8, Entries.maxBlocksPerShard);
    defer alloc.free(firstShardEntries);
    const secondShardEntries = try alloc.alloc([]const u8, Entries.maxBlocksPerShard);
    defer alloc.free(secondShardEntries);
    const thirdShardEntries = try alloc.alloc([]const u8, countAdditionalEntries);
    defer alloc.free(thirdShardEntries);

    for (firstShardEntries) |*entry| entry.* = theLargest;
    for (secondShardEntries) |*entry| entry.* = theLargest;
    for (thirdShardEntries) |*entry| entry.* = theLargest;

    try recorder.add(io, alloc, firstShardEntries);
    try recorder.add(io, alloc, secondShardEntries);
    try recorder.add(io, alloc, thirdShardEntries);

    try testing.expectEqual(@as(usize, 2 * Entries.maxBlocksPerShard), recorder.blocksToFlush.items.len);

    var blocksInShards: usize = 0;
    for (recorder.entries.shards) |shard| {
        blocksInShards += shard.blocks.items.len;
    }
    try testing.expectEqual(@as(usize, countAdditionalEntries), blocksInShards);

    try recorder.stop(io, alloc);
}

test "IndexRecorder 3 shards addings small entries doesn't flush them" {
    const alloc = testing.allocator;
    const io = testing.io;

    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    const rootPath = try tmp.dir.realPathFileAlloc(io, ".", alloc);
    defer alloc.free(rootPath);
    const shortValue = "short";

    const runtime = try Runtime.init(io, alloc, rootPath, 0.5);
    defer runtime.deinit(alloc);
    runtime.cpus = 3;

    const recorder = try IndexRecorder.init(io, alloc, rootPath, runtime);
    try testing.expectEqual(recorder.entries.shards.len, runtime.cpus);

    for (0..runtime.cpus) |_| {
        try recorder.add(io, alloc, &.{shortValue});
    }

    try testing.expectEqual(0, recorder.blocksToFlush.items.len);
    for (recorder.entries.shards) |*shard| {
        shard.mx.lockUncancelable(io);
        defer shard.mx.unlock(io);

        try testing.expectEqual(1, shard.blocks.items.len);
        try testing.expectEqual(1, shard.blocks.items[0].memEntries.items.len);
        try testing.expectEqualStrings(shortValue, shard.blocks.items[0].memEntries.items[0]);
    }

    try recorder.stop(io, alloc);
    // after deinit we can't validate the blocks are empty, but can read the files

    var tables = try Table.openAll(io, alloc, rootPath);
    try testing.expectEqual(tables.items.len, 1);
    defer {
        for (tables.items) |table| table.release(io);
        tables.deinit(alloc);
    }
    const flushedTable = tables.items[0];

    var lookup = LookupTable.init(flushedTable, Conf.getConf().app.maxIndexMemBlockSize);
    defer lookup.deinit(alloc);

    try lookup.seek(alloc, shortValue);
    var readItems: usize = 0;
    while (try lookup.next(alloc)) {
        try testing.expectEqualStrings(shortValue, lookup.current);
        readItems += 1;
    }
    try testing.expectEqual(runtime.cpus, readItems);
}

test "IndexRecorder large entries write to 3 shards" {
    const alloc = testing.allocator;
    const io = testing.io;

    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    const rootPath = try tmp.dir.realPathFileAlloc(io, ".", alloc);
    defer alloc.free(rootPath);
    const maxIndexMemBlockSize = 32 * 1024;
    //countAdditionalEntries < Entries.maxBlocksPerShard
    const countAdditionalEntries = Entries.maxBlocksPerShard - 1;
    //2 shards full-filled and third shard is not completely filled
    const totalEntries = (2 * Entries.maxBlocksPerShard) + countAdditionalEntries;
    const theLargest = "x" ** maxIndexMemBlockSize;
    var testEntries: [][]const u8 = try alloc.alloc([]const u8, totalEntries);
    defer alloc.free(testEntries);

    for (0..totalEntries) |i| {
        testEntries[i] = theLargest;
    }

    const runtime = try Runtime.init(io, alloc, rootPath, 0.5);
    defer runtime.deinit(alloc);

    const recorder = try IndexRecorder.init(io, alloc, rootPath, runtime);
    defer recorder.deinit(io, alloc);

    try recorder.add(io, alloc, testEntries);

    try testing.expectEqual(totalEntries - countAdditionalEntries, recorder.blocksToFlush.items.len);

    recorder.stopped.store(true, .release);

    try recorder.flushForce(io, alloc);

    try testing.expectEqual(@as(usize, 0), recorder.memTables.items.len);
    try testing.expect(recorder.diskTables.items.len > 0);
    try testing.expectEqual(@as(u64, 0), countMemItemsInRecorder(recorder));
    try testing.expectEqual(@as(u64, totalEntries), countDiskItemsInRecorder(recorder));
}
