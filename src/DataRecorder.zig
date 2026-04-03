const std = @import("std");
const Allocator = std.mem.Allocator;

const fs = @import("fs.zig");

const Conf = @import("Conf.zig");
const Line = @import("store/lines.zig").Line;

const MemTable = @import("store/inmem/MemTable.zig");
const BlockWriter = @import("store/inmem/BlockWriter.zig");
const StreamWriter = @import("store/inmem/StreamWriter.zig");
const Table = @import("store/data/Table.zig");
const BlockReader = @import("store/inmem/reader.zig").BlockReader;
const mergeData = @import("store/data/merge.zig").mergeData;

const flush = @import("store/table/flush.zig");
const merge = @import("store/table/merge.zig");
const cap = @import("store/table/cap.zig");
const swap = @import("store/table/swap.zig");

const maxMemTables = 24;
// TODO: it looks inconsistent, some take ponters, some don't
const merger = merge.Merger(*Table, *MemTable, maxMemTables);
const swapper = swap.Swapper(DataRecorder, Table);

fn sleepOrStop(stopped: *const std.atomic.Value(bool), ns: u64) void {
    // TODO: make this interval configurable,
    // it must be shorter for tests and longer for production
    const step = 250 * std.time.ns_per_ms;
    var remaining = ns;
    while (remaining > 0) {
        if (stopped.load(.acquire)) return;
        const s = @min(remaining, step);
        std.Thread.sleep(s);
        remaining -= s;
    }
}

// TODO: move flush interval to config
fn setFlushTime() i64 {
    // now + 1s
    return std.time.microTimestamp() + std.time.us_per_s;
}

pub const DataRecorder = @This();

pub const DataShard = struct {
    mx: std.Thread.Mutex = .{},
    lines: std.ArrayList(Line) = .empty,
    size: u64 = 0,
    // TODO: currently there is a single background process flushing the data shards
    // try instead assign a timer task to a shard and benchmark on high amount of shard (high amount of cpu)
    flushAtUs: ?i64 = null,

    // threshold as 90% of a max block size
    const flushSizeThreshold = 9 * (MemTable.maxBlockSize / 10);
    fn mustFlush(self: *DataShard) bool {
        return self.size >= flushSizeThreshold;
    }

    // flush sends all the data to a mem Table,
    // is not a thread safe, assumes the shard is locked
    fn flush(self: *DataShard, alloc: Allocator, sem: *std.Thread.Semaphore) !?*Table {
        if (self.lines.items.len == 0) {
            return null;
        }

        sem.wait();
        errdefer sem.post();

        const memTable = try MemTable.init(alloc);
        try memTable.addLines(alloc, self.lines.items);
        self.lines.clearRetainingCapacity();

        sem.post();

        memTable.flushAtUs = setFlushTime();
        return Table.fromMem(alloc, memTable);
    }
};

shards: []DataShard,
nextShard: std.atomic.Value(usize),

mxTables: std.Thread.Mutex,
memTables: std.ArrayList(*Table),
diskTables: std.ArrayList(*Table),

concurrency: u16,
diskMergeSem: std.Thread.Semaphore,
memMergeSem: std.Thread.Semaphore,

pool: *std.Thread.Pool,
wg: std.Thread.WaitGroup = .{},
memTablesSem: std.Thread.Semaphore = .{
    .permits = maxMemTables,
},
stopped: std.atomic.Value(bool) = .init(false),
mergeIdx: std.atomic.Value(usize),
path: []const u8,

pub fn init(alloc: Allocator, workersAllocator: Allocator, path: []const u8) !*DataRecorder {
    const conf = Conf.getConf();
    const concurrency = conf.server.pools.cpus;

    std.debug.assert(conf.server.pools.cpus != 0);
    // 4 is a minimum amount for workers:
    // data shards flushare, mem table flusher, mem table merger, disk table merger
    // TODO: move basic validation to the config
    std.debug.assert(conf.server.pools.workerThreads >= 4);

    const shards = try alloc.alloc(DataShard, concurrency);
    errdefer alloc.free(shards);
    for (shards) |*shard| {
        shard.* = .{};
    }

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

    const self = try alloc.create(DataRecorder);
    self.* = DataRecorder{
        .shards = shards,
        .nextShard = std.atomic.Value(usize).init(0),
        .mergeIdx = std.atomic.Value(usize).init(0),

        .mxTables = .{},
        .concurrency = concurrency,
        .memTables = memTables,
        .diskTables = tables,
        .diskMergeSem = .{
            .permits = @max(4, concurrency),
        },
        .memMergeSem = .{
            .permits = @max(4, concurrency),
        },

        .pool = pool,
        .memTablesSem = .{ .permits = concurrency },
        .stopped = std.atomic.Value(bool).init(false),
        .path = path,
    };

    // the allocator is different from http life cycle,
    // but shared between all the background jobs
    // TODO: find a better allocator, perhaps an arena with regular reset
    self.pool.spawnWg(&self.wg, startMemTableFlusher, .{ self, workersAllocator });
    self.pool.spawnWg(&self.wg, startDataShardsFlusher, .{ self, workersAllocator });

    return self;
}

pub fn deinit(self: *DataRecorder, allocator: Allocator) void {
    self.stopped.store(true, .release);
    self.wg.wait();
    self.pool.deinit();
    allocator.destroy(self.pool);
    allocator.free(self.shards);
    allocator.destroy(self);
}

fn startMemTableFlusher(self: *DataRecorder, allocator: Allocator) void {
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();

    const alloc = arena.allocator();
    var iteration: usize = 0;
    while (!self.stopped.load(.acquire)) {
        sleepOrStop(&self.stopped, std.time.ns_per_s);
        self.flushMemTable(alloc, false) catch unreachable;
        _ = arena.reset(.retain_capacity);
        iteration += 1;
    }
    self.flushMemTable(alloc, true) catch unreachable;
}

fn flushMemTable(self: *DataRecorder, allocator: Allocator, force: bool) !void {
    const nowUs = std.time.microTimestamp();

    self.mxTables.lock();
    defer self.mxTables.unlock();

    var tables = std.ArrayList(*Table).initCapacity(allocator, self.memTables.items.len) catch |err| {
        self.handleErr(err);
        return;
    };
    for (self.memTables.items) |memTable| {
        const isTimeToMerge = memTable.mem.?.flushAtUs <= nowUs;
        if (!memTable.inMerge and (force or isTimeToMerge)) {
            tables.appendAssumeCapacity(memTable);
        }
    }

    // TODO: reshuffle tables to merge in order to build more effective file sizes
    self.memTablesSem.wait();
    try self.mergeTables(allocator, tables.items, force, &self.stopped);
    self.memTablesSem.post();
}

/// startDataShardsFlusher runs a worker to flush DataShard on flushAtUs
fn startDataShardsFlusher(self: *DataRecorder, allocator: Allocator) void {
    // half a sec
    // TODO: test it with 1 sec
    const flushInterval = std.time.ns_per_s / 2;

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();

    const alloc = arena.allocator();
    while (!self.stopped.load(.acquire)) {
        sleepOrStop(&self.stopped, flushInterval);
        self.flushDataShards(alloc, false);
        _ = arena.reset(.retain_capacity);
    }
    self.flushDataShards(alloc, true);
}

fn flushDataShards(self: *DataRecorder, allocator: Allocator, force: bool) void {
    if (force) {
        for (self.shards) |*shard| {
            shard.mx.lock();
            self.flushShard(allocator, shard);
            shard.mx.unlock();
        }
        return;
    }

    const nowUs = std.time.microTimestamp();
    for (self.shards) |*shard| {
        // if it's not locked we are adding lines just know, makes no sense to lock it yet
        if (shard.mx.tryLock()) {
            if (shard.flushAtUs) |flushAtUs| {
                if (flushAtUs < nowUs) {
                    self.flushShard(allocator, shard);
                }
            }
            shard.mx.unlock();
        }
    }
}

fn flushShard(self: *DataRecorder, alloc: Allocator, shard: *DataShard) void {
    const maybeMemTable = shard.flush(alloc, &self.memTablesSem) catch |err| {
        self.handleErr(err);
        return;
    };
    if (maybeMemTable) |memTable| {
        self.mxTables.lock();
        defer self.mxTables.unlock();
        self.memTables.append(alloc, memTable) catch |err| {
            self.handleErr(err);
            return;
        };

        self.startMemTableMerger(alloc);
    }
}

fn handleErr(self: *DataRecorder, err: anyerror) void {
    std.debug.print("ERROR: failed to flush a data shard, err={}\n", .{err});
    self.stopped.store(true, .release);
    // TODO: broadcast the app must close
    return;
}

fn startMemTableMerger(self: *DataRecorder, allocator: Allocator) void {
    if (self.stopped.load(.acquire)) return;

    self.pool.spawnWg(&self.wg, runMemTableMerger, .{ self, allocator });
}

fn runMemTableMerger(self: *DataRecorder, alloc: Allocator) void {
    self.tablesMerger(alloc, &self.memTables, &self.memTablesSem) catch |err| {
        std.debug.print("failed to merge mem tables: {s}\n", .{@errorName(err)});
    };
}

pub fn startDiskTablesMerge(self: *DataRecorder, alloc: Allocator) void {
    _ = self;
    _ = alloc;
}

pub fn startMemTablesMerge(self: *DataRecorder, alloc: Allocator) void {
    _ = self;
    _ = alloc;
}

fn tablesMerger(
    self: *DataRecorder,
    alloc: Allocator,
    tables: *std.ArrayList(*Table),
    sem: *std.Thread.Semaphore,
) !void {
    var tablesToMerge = std.ArrayList(*Table).empty;
    defer tablesToMerge.deinit(alloc);

    while (true) {
        const maxDiskTableSize = cap.getMaxTableSize(self.path);

        self.mxTables.lock();
        errdefer self.mxTables.unlock();
        // filteredTablesToMerge is a slice of tables ArrayList, no need to free it
        const window = try merger.filterTablesToMerge(alloc, tables.items, &tablesToMerge, maxDiskTableSize);
        const w = window orelse {
            self.mxTables.unlock();
            return;
        };
        const filteredTablesToMerge = tablesToMerge.items[w.lower..w.upper];
        self.mxTables.unlock();
        if (filteredTablesToMerge.len == 0) {
            return;
        }

        // TODO: make sure error.Stopped is handled on the upper level
        sem.wait();
        errdefer sem.post();
        try self.mergeTables(alloc, filteredTablesToMerge, false, &self.stopped);
        sem.post();
        tablesToMerge.clearRetainingCapacity();
    }
}

fn nextMergeIdx(self: *DataRecorder) usize {
    return self.mergeIdx.fetchAdd(1, .acq_rel);
}

fn mergeTables(
    self: *DataRecorder,
    alloc: Allocator,
    tables: []*Table,
    force: bool,
    stopped: ?*std.atomic.Value(bool),
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
    const blockWriter = try BlockWriter.init(alloc);
    defer blockWriter.deinit(alloc);
    var streamWriter: *StreamWriter = undefined;
    if (tableKind == .mem) {
        newMemTable = try MemTable.init(alloc);
        streamWriter = try StreamWriter.initMem(alloc, 1);
    } else {
        var sourceCompressedSizeTotal: u64 = 0;
        for (tables) |table| {
            sourceCompressedSizeTotal += table.tableHeader.compressedSize;
        }
        const fitsInCache = sourceCompressedSizeTotal <= merger.maxCachableTableSize();
        streamWriter = try StreamWriter.initDisk(alloc, destinationTablePath, fitsInCache);
    }

    const tableHeader = try mergeData(alloc, streamWriter, &readers, stopped);
    if (newMemTable) |memTable| {
        memTable.tableHeader = tableHeader;
    } else {
        std.debug.assert(destinationTablePath.len > 0);

        var fba = std.heap.stackFallback(256, alloc);
        try tableHeader.writeFile(fba.get(), destinationTablePath);

        fs.syncPathAndParentDir(destinationTablePath);
    }

    const openTable = try openCreatedTable(alloc, destinationTablePath, tables, newMemTable);
    try swapper.swapTables(self, alloc, tables, openTable, tableKind);
    swapped = true;
}

pub fn addLines(self: *DataRecorder, alloc: Allocator, lines: []const Line, size: usize) !void {
    const i = self.nextShard.fetchAdd(1, .acquire) % self.shards.len;
    var shard = &self.shards[i];

    shard.mx.lock();

    try shard.lines.appendSlice(alloc, lines);
    shard.size += size;
    if (shard.mustFlush()) {
        shard.flushAtUs = null;
        self.flushShard(alloc, shard);
    } else if (shard.flushAtUs == null) {
        shard.flushAtUs = setFlushTime();
    }

    shard.mx.unlock();
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
