const std = @import("std");
const Allocator = std.mem.Allocator;

const Entries = @import("Entries.zig");
const MemBlock = @import("MemBlock.zig");
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

// TODO: rename to IndexRecorder, there is nothing about Table
const Self = @This();

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
memTablesSem: std.Thread.Semaphore = .{},
memTablesMx: std.Thread.Mutex = .{},
memTables: std.ArrayList(*MemTable) = .empty,

pool: *std.Thread.Pool,
// wg holds all the running jobs
wg: std.Thread.WaitGroup = .{},

needInvalidate: std.atomic.Value(bool) = .init(false),
indexCacheKeyVersion: std.atomic.Value(u64) = .init(0),

pub fn init(alloc: Allocator) !*Self {
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

    const t = try alloc.create(Self);
    t.* = .{
        .entries = entries,
        .blocksThresholdToFlush = @intCast(entries.shards.len * maxBlocksPerShard),
        .blocksToFlush = blocksToFlush,
        .maxIndexBlockSize = Conf.getConf().app.maxIndexMemBlockSize,
        .pool = pool,
    };
    return t;
}

pub fn deinit(self: *Self, alloc: Allocator) void {
    self.entries.deinit(alloc);
    self.blocksToFlush.deinit(alloc);
    self.pool.deinit();
    alloc.destroy(self);
}

pub fn add(self: *Self, alloc: Allocator, entries: [][]const u8) !void {
    const shard = self.entries.next();
    const blocksList = try shard.add(alloc, entries, self.maxIndexBlockSize);
    if (blocksList == null) return;

    var blocks = blocksList.?;
    defer blocks.deinit(alloc);
    try self.flushBlocks(alloc, blocks.items);
}

fn flushBlocks(self: *Self, alloc: Allocator, blocks: []*MemBlock) !void {
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

fn flushBlocksToMemTables(self: *Self, alloc: Allocator, blocks: []*MemBlock, force: bool) !void {
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
    try self.addToMemTables(alloc, mergedMemTable, force);
}

/// merges mem tables to a bigger size ones
/// requires same Allocator that's used to create them,
/// because it deinits the merged ones
fn mergeMemTables(alloc: Allocator, memTables: []*MemTable) !*MemTable {
    // TODO: run merging job in parallel and benchmark whether it doesn't hurt general throughput

    std.debug.assert(memTables.len != 0);
    if (memTables.len == 1) return memTables[0];

    return MemTable.mergeTables(alloc, memTables);
}

fn addToMemTables(self: *Self, alloc: Allocator, memTable: *MemTable, force: bool) !void {
    var semaphoreWaited = false; // if not stopped then wait for an available semaphore
    if (!self.stopped.load(.acquire)) {
        self.memTablesSem.wait();
        semaphoreWaited = true;
    }
    errdefer if (semaphoreWaited) self.memTablesSem.post();

    // TODO: ideally to know the amount of mem tables and call unlock immediately.
    // it cuts lock time down, but requires handling stop differently,
    // because we can't rely on the semaphore limit
    self.memTablesMx.lock();
    defer self.memTablesMx.unlock();
    try self.memTables.append(alloc, memTable);
    try self.startMemTablesMerge(alloc);

    if (force) {
        self.invalidateStreamFilterCache();
    } else {
        if (!self.needInvalidate.load(.acquire)) {
            _ = self.needInvalidate.cmpxchgWeak(false, true, .release, .monotonic);
        }
    }
}

fn startMemTablesMerge(self: *Self, alloc: Allocator) !void {
    if (self.stopped.load(.acquire)) return;

    // TODO: schedule a background job to merge mem tables,
    // changing it requires:
    // 1. holding a mutex for mem tables
    // 2. adding a semaphore to limit concurrent merges
    // 3. mark mem tables being merged to avoid double acquisition
    return self.runMemTablesMerger(alloc);
}

fn runMemTablesMerger(self: *Self, alloc: Allocator) !void {
    while (true) {
        // TODO: implement disk space limit

        if (self.memTables.items.len == 0) {
            return;
        }

        // TODO: make sure error.Stopped is handled on the upper level
        try self.mergeTables(alloc, false);
    }
}

fn invalidateStreamFilterCache(self: *Self) void {
    _ = self.indexCacheKeyVersion.fetchAdd(1, .acquire);
}

pub fn mergeTables(
    self: *Self,
    alloc: Allocator,
    force: bool,
) !void {
    const tableKind = getDestinationTableKind(self.memTables.items, force);
    const destinationTablePath = "";

    // TODO: implement merging into a file here

    // TODO: implement a shutdown path
    // if (force and memTables.len == 1) {
    //     const table = memTables[0];
    //     table.storeToDisk(destinationTablePath);
    //     const newTable = openCreatedTable(destinationTablePath, memTables, null);
    //     self.swapTables(memTables, newTable, tableKind);
    //     return;
    // }

    var readers = try openTableReaders(alloc, self.memTables.items);
    defer {
        for (readers.items) |reader| reader.deinit(alloc);
        readers.deinit(alloc);
    }

    // TODO: check table kind
    const newMemTable = try MemTable.empty(alloc);
    var blockWriter = BlockWriter.initFromMemTable(newMemTable);
    defer blockWriter.deinit(alloc);

    try newMemTable.mergeBlocks(alloc, destinationTablePath, &blockWriter, &readers, &self.stopped);
    const newTable = openCreatedTable(destinationTablePath, self.memTables.items, newMemTable);
    try self.swapTables(alloc, newTable, tableKind);
}

// TODO: implement it, at the moment it does only mem tables merging
fn getDestinationTableKind(tables: []*MemTable, force: bool) TableKind {
    _ = tables;
    _ = force;
    // const size = getTableSize(tables);
    // if (force or size > getMaxInmemoryTableSize()) {
    //     return .file;
    // }
    // if (!areTablesMem(tables)) {
    //     return .file;
    // }
    return .mem;
}

fn getTablesSize(tables: []*MemTable) u64 {
    var n: u64 = 0;
    for (tables) |table| {
        n += table.size();
    }
    return n;
}

fn openTableReaders(alloc: Allocator, tables: []*MemTable) !std.ArrayList(*BlockReader) {
    var readers = try std.ArrayList(*BlockReader).initCapacity(alloc, tables.len);
    defer {
        for (readers.items) |reader| reader.deinit(alloc);
        readers.deinit(alloc);
    }
    for (tables) |table| {
        // TODO: it must support opening from file as well,
        // but it requires accepting not only a mem table, but a disk table
        const reader = try BlockReader.initFromMemTable(alloc, table);
        readers.appendAssumeCapacity(reader);
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
    self: *Self,
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

fn startCacheKeyInvalidator(self: *Self) !void {
    // TODO: add time sleep jitter
    self.wg.spawnManager(startCacheKeyInvalidatorTask, .{self});
}

fn startCacheKeyInvalidatorTask(self: *Self) void {
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

fn startMemTablesFlusher(self: *Self, _: Allocator) void {
    _ = self;
}

fn startEntriesFlusher(self: *Self, _: Allocator) void {
    _ = self;
}
