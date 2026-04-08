// TODO: data and index recorders are both hold a lot in common,
// we must desine a single component to manage both
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
    // state

    mx: std.Thread.Mutex = .{},
    lines: std.ArrayList(Line) = .empty,

    size: u64 = 0,
    // TODO: currently there is a single background process flushing the data shards
    // try instead assign a timer task to a shard and benchmark on high amount of shard (high amount of cpu)
    flushAtUs: ?i64 = null,

    // threshold as 90% of a max block size
    const flushSizeThreshold = 9 * (MemTable.maxBlockSize / 10);
    fn mustFlush(self: *DataShard) bool {
        // TODO: check its timer?
        return self.size >= flushSizeThreshold;
    }

    // flush sends all the data to a mem Table,
    // is not a thread safe, assumes the shard is locked
    fn flush(self: *DataShard, alloc: Allocator, sem: *std.Thread.Semaphore) !?*Table {
        if (self.lines.items.len == 0) {
            return null;
        }

        const memTable = try MemTable.init(alloc);
        errdefer memTable.deinit(alloc);

        sem.wait();

        memTable.addLines(alloc, self.lines.items) catch |err| {
            sem.post();
            return err;
        };
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
// TODO: implement its usage, limit the amount of mem tables similar to index
// in order to let the mem merger handle it
memTablesSem: std.Thread.Semaphore = .{
    .permits = maxMemTables,
},
stopped: std.atomic.Value(bool) = .init(false),
mergeIdx: std.atomic.Value(usize),
path: []const u8,

// TODO: investigate the ownership of the Lines and background jobs,
// identify whether they match the data
pub fn init(alloc: Allocator, path: []const u8) !*DataRecorder {
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

    const t = try alloc.create(DataRecorder);
    t.* = DataRecorder{
        .shards = shards,
        .nextShard = std.atomic.Value(usize).init(0),
        .mergeIdx = .init(@intCast(std.time.nanoTimestamp())),

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
        .stopped = std.atomic.Value(bool).init(false),
        .path = trimmedPath,
    };

    for (0..concurrency) |_| {
        t.startDiskTablesMerge(alloc);
    }

    t.startMemTablesFlusher(alloc);
    t.startDataShardsFlusher(alloc);

    return t;
}

// TODO: find an approach to make it never fail,
// the only option it fails is OOM, so cleaning more memory in advance might be more reliable
// another problem it's hard to test it via checkAllAllocationFailures.
// Then audit all deinits and use it instead
// TODO: make using this API instead of directly managing stopped state
// TODO: this theoretically is not enough to stop the other jobs form starting,
// either lock stop or find another way to make sure none of the task are running after wg.wait
pub fn stop(self: *DataRecorder, alloc: Allocator) !void {
    self.stopped.store(true, .release);
    self.wg.wait();

    try self.flushForce(alloc);

    self.deinit(alloc);
}

pub fn flushForce(self: *DataRecorder, alloc: Allocator) !void {
    try self.flushDataShards(alloc, true);
    try self.flushMemTables(alloc, true);
}

pub fn deinit(self: *DataRecorder, allocator: Allocator) void {
    // make sure deinit is never called outside of stop
    std.debug.assert(self.memTables.items.len == 0);

    for (self.shards) |*shard| {
        shard.lines.deinit(allocator);
    }
    for (self.diskTables.items) |table| {
        table.release();
    }
    for (self.memTables.items) |table| {
        table.release();
    }

    self.memTables.deinit(allocator);
    self.diskTables.deinit(allocator);
    _ = Conf.removeDiskSpace(self.path);
    self.pool.deinit();
    allocator.destroy(self.pool);
    allocator.free(self.shards);
    allocator.destroy(self);
}

fn startMemTablesFlusher(self: *DataRecorder, alloc: Allocator) void {
    self.pool.spawnWg(&self.wg, runMemTablesFlusher, .{ self, alloc });
}

fn startDataShardsFlusher(self: *DataRecorder, alloc: Allocator) void {
    self.pool.spawnWg(&self.wg, runDataShardsFlusher, .{ self, alloc });
}

fn runMemTablesFlusher(self: *DataRecorder, alloc: Allocator) void {
    while (!self.stopped.load(.acquire)) {
        // TODO: setup a diagnostic pattern to inject more context about the error and log messages
        self.flushMemTables(alloc, false) catch |err| {
            if (err == error.Stopped) return;

            self.stopped.store(true, .release);
            std.debug.print("failed to run mem tables flusher: {s}\n", .{@errorName(err)});
            return;
        };

        sleepOrStop(&self.stopped, std.time.ns_per_s);
    }
}

fn runDataShardsFlusher(self: *DataRecorder, alloc: Allocator) void {
    // half a sec
    // TODO: test it with 1 sec
    const flushInterval = std.time.ns_per_s / 2;

    while (!self.stopped.load(.acquire)) {
        self.flushDataShards(alloc, false) catch |err| {
            if (err == error.Stopped) return;

            self.stopped.store(true, .release);
            std.debug.print("failed to run data shards flusher: {s}\n", .{@errorName(err)});
            return;
        };

        sleepOrStop(&self.stopped, flushInterval);
    }

    self.flushDataShards(alloc, true) catch |err| {
        if (err == error.Stopped) return;

        self.stopped.store(true, .release);
        std.debug.print("failed to run force data shards flusher: {s}\n", .{@errorName(err)});
        return;
    };
}

fn flushMemTables(self: *DataRecorder, allocator: Allocator, force: bool) !void {
    const nowUs = std.time.microTimestamp();

    self.mxTables.lock();

    var tables = std.ArrayList(*Table).initCapacity(allocator, self.memTables.items.len) catch |err| {
        self.mxTables.unlock();
        return err;
    };
    defer tables.deinit(allocator);

    for (self.memTables.items) |memTable| {
        const isTimeToMerge = memTable.mem.?.flushAtUs <= nowUs;
        if (!memTable.inMerge and (force or isTimeToMerge)) {
            memTable.inMerge = true;
            tables.appendAssumeCapacity(memTable);
        }
    }

    self.mxTables.unlock();

    if (tables.items.len == 0) {
        return;
    }

    try self.flushMemTablesInChunks(allocator, tables);
}

fn flushMemTablesInChunks(self: *DataRecorder, alloc: Allocator, toFlush: std.ArrayList(*Table)) !void {
    if (toFlush.items.len == 0) return;

    var left = std.ArrayList(*Table).initBuffer(toFlush.items[0..]);
    left.items.len = toFlush.items.len;

    while (left.items.len > 0) {
        const n = merger.selectTablesToMerge(&left);
        std.debug.assert(n > 0);

        // TODO: attempt to run it in parallel, add a semaphore then
        try self.mergeTables(alloc, left.items[0..n], true, null);

        const tail = left.items[n..];
        left = std.ArrayList(*Table).initBuffer(tail);
        left.items.len = tail.len;
    }
}

fn flushDataShards(self: *DataRecorder, allocator: Allocator, force: bool) !void {
    if (force) {
        for (self.shards) |*shard| {
            shard.mx.lock();
            defer shard.mx.unlock();
            try self.flushShard(allocator, shard);
        }
        return;
    }

    const nowUs = std.time.microTimestamp();
    for (self.shards) |*shard| {
        // if it's not locked we are adding lines just know, makes no sense to lock it yet
        // TODO: find out whether it's possible never flush them due to tryLock,
        // requires adding a debug log here
        if (shard.mx.tryLock()) {
            defer shard.mx.unlock();
            if (shard.flushAtUs) |flushAtUs| {
                if (flushAtUs < nowUs) {
                    try self.flushShard(allocator, shard);
                }
            }
        }
    }
}

fn flushShard(self: *DataRecorder, alloc: Allocator, shard: *DataShard) !void {
    const maybeMemTable = try shard.flush(alloc, &self.memMergeSem);
    if (maybeMemTable) |memTable| {
        self.mxTables.lock();
        defer self.mxTables.unlock();
        try self.memTables.append(alloc, memTable);

        shard.flushAtUs = null;
        shard.size = 0;

        self.startMemTablesMerge(alloc);
    }
}

pub fn startDiskTablesMerge(self: *DataRecorder, alloc: Allocator) void {
    if (self.stopped.load(.acquire)) return;

    self.pool.spawnWg(&self.wg, runDiskTablesMerger, .{ self, alloc });
}

pub fn startMemTablesMerge(self: *DataRecorder, alloc: Allocator) void {
    if (self.stopped.load(.acquire)) return;

    self.pool.spawnWg(&self.wg, runMemTableMerger, .{ self, alloc });
}

fn runDiskTablesMerger(self: *DataRecorder, alloc: Allocator) void {
    self.tablesMerger(alloc, &self.diskTables, &self.diskMergeSem) catch |err| {
        if (err == error.Stopped) return;

        self.stopped.store(true, .release);
        std.debug.print("failed to merge disk tables: {s}\n", .{@errorName(err)});
    };
}

fn runMemTableMerger(self: *DataRecorder, alloc: Allocator) void {
    self.tablesMerger(alloc, &self.memTables, &self.memMergeSem) catch |err| {
        if (err == error.Stopped) return;

        self.stopped.store(true, .release);
        std.debug.print("failed to merge mem tables: {s}\n", .{@errorName(err)});
    };
}

fn tablesMerger(
    self: *DataRecorder,
    alloc: Allocator,
    tables: *std.ArrayList(*Table),
    sem: *std.Thread.Semaphore,
) !void {
    var tablesToMerge = std.ArrayList(*Table).empty;
    defer tablesToMerge.deinit(alloc);

    while (!self.stopped.load(.acquire)) {
        const maxDiskTableSize = cap.getMaxTableSize(self.path);

        self.mxTables.lock();
        // TODO: we have to know the max amount of tables in advance
        tablesToMerge.ensureUnusedCapacity(alloc, tables.items.len) catch |err| {
            self.mxTables.unlock();
            return err;
        };
        // filteredTablesToMerge is a slice of tables ArrayList, no need to free it
        const window = merger.filterTablesToMerge(tables.items, &tablesToMerge, maxDiskTableSize);
        self.mxTables.unlock();

        const w = window orelse return;
        const filteredTablesToMerge = tablesToMerge.items[w.lower..w.upper];
        if (filteredTablesToMerge.len == 0) return;

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
    // TODO: remove this shame after rmoving writer from mem table
    var ownedStreamWriter: ?*StreamWriter = null;
    defer if (ownedStreamWriter) |w| w.deinit(alloc);
    if (tableKind == .mem) {
        newMemTable = try MemTable.init(alloc);
        streamWriter = newMemTable.?.streamWriter;
    } else {
        var sourceCompressedSizeTotal: u64 = 0;
        for (tables) |table| {
            sourceCompressedSizeTotal += table.tableHeader.compressedSize;
        }
        const fitsInCache = sourceCompressedSizeTotal <= merger.maxCachableTableSize();
        streamWriter = try StreamWriter.initDisk(alloc, destinationTablePath, fitsInCache);
        ownedStreamWriter = streamWriter;
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
    defer shard.mx.unlock();

    // TODO: we must now the limit on amount of lines per shard and append a known amount
    try shard.lines.appendSlice(alloc, lines);
    shard.size += size;
    if (shard.mustFlush()) {
        try self.flushShard(alloc, shard);
    } else if (shard.flushAtUs == null) {
        shard.flushAtUs = setFlushTime();
    }
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

const testing = std.testing;
const Field = @import("store/lines.zig").Field;

var stableFields = [_][2]Field{
    .{
        .{ .key = "level", .value = "info" },
        .{ .key = "app", .value = "ochi" },
    },
    .{
        .{ .key = "level", .value = "warn" },
        .{ .key = "app", .value = "ochi" },
    },
    .{
        .{ .key = "level", .value = "error" },
        .{ .key = "app", .value = "ochi" },
    },
    .{
        .{ .key = "region", .value = "us-east" },
        .{ .key = "service", .value = "api" },
    },
};

fn stableLine(ts: u64, streamID: u128, variant: usize) Line {
    const fields = stableFields[variant % stableFields.len][0..];
    return .{
        .timestampNs = ts,
        .sid = .{ .tenantID = "tenant-1", .id = streamID },
        .fields = fields,
    };
}

fn createMemTableFromLines(alloc: Allocator, lines: []Line) !*Table {
    const memTable = try MemTable.init(alloc);
    try memTable.addLines(alloc, lines);
    return Table.fromMem(alloc, memTable);
}

fn countMemLinesInRecorder(recorder: *DataRecorder) u64 {
    var n: u64 = 0;
    for (recorder.memTables.items) |table| {
        n += table.tableHeader.len;
    }
    return n;
}

fn countDiskLinesInRecorder(recorder: *DataRecorder) u64 {
    var n: u64 = 0;
    for (recorder.diskTables.items) |table| {
        n += table.tableHeader.len;
    }
    return n;
}

test "DataRecorder init and close empty dir, trim slash" {
    const alloc = testing.allocator;
    _ = try Conf.default(alloc);
    defer Conf.deinit();

    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    const rootPath = try tmp.dir.realpathAlloc(alloc, ".");
    defer alloc.free(rootPath);
    const pathWithSlash = try std.mem.concat(alloc, u8, &.{ rootPath, std.fs.path.sep_str });
    defer alloc.free(pathWithSlash);

    const recorder = try DataRecorder.init(alloc, pathWithSlash);

    try testing.expectEqual(@as(usize, 0), recorder.diskTables.items.len);
    try testing.expectEqual(@as(usize, 0), recorder.memTables.items.len);
    try testing.expectEqual(@as(usize, 0), countMemLinesInRecorder(recorder));
    try testing.expect(std.mem.eql(u8, recorder.path, rootPath));

    try recorder.stop(alloc);
}

test "flushDataShards non-force respects flush deadline" {
    const alloc = testing.allocator;
    _ = try Conf.default(alloc);
    defer Conf.deinit();

    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    const rootPath = try tmp.dir.realpathAlloc(alloc, ".");
    defer alloc.free(rootPath);

    const recorder = try DataRecorder.init(alloc, rootPath);
    recorder.stopped.store(true, .release);
    recorder.wg.wait();
    defer recorder.deinit(alloc);

    const line = stableLine(1, 1, 0);
    try recorder.shards[0].lines.append(alloc, line);
    recorder.shards[0].size = line.fieldsSize();

    recorder.shards[0].flushAtUs = std.time.microTimestamp() + std.time.us_per_s;
    try recorder.flushDataShards(alloc, false);
    try testing.expectEqual(@as(usize, 1), recorder.shards[0].lines.items.len);
    try testing.expectEqual(@as(usize, 0), recorder.memTables.items.len);

    recorder.shards[0].flushAtUs = std.time.microTimestamp() - std.time.us_per_s;
    try recorder.flushDataShards(alloc, false);
    try testing.expectEqual(@as(usize, 0), recorder.shards[0].lines.items.len);
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

    const recorder = try DataRecorder.init(alloc, rootPath);
    recorder.stopped.store(true, .release);
    recorder.wg.wait();
    defer recorder.deinit(alloc);

    var lines = [_]Line{
        stableLine(1, 1, 0),
        stableLine(2, 1, 1),
        stableLine(3, 1, 2),
    };
    const table = try createMemTableFromLines(alloc, lines[0..]);
    try recorder.memTables.append(alloc, table);
    table.inMerge = true;

    var single = [_]*Table{table};
    try recorder.mergeTables(alloc, single[0..], true, null);
    try testing.expectEqual(@as(usize, 0), recorder.memTables.items.len);
    try testing.expectEqual(@as(usize, 1), recorder.diskTables.items.len);
    try testing.expect(recorder.diskTables.items[0].disk != null);
}

test "DataRecorder.addAndReopenPreservesLineCount" {
    const alloc = testing.allocator;
    _ = try Conf.default(alloc);
    defer Conf.deinit();

    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    const rootPath = try tmp.dir.realpathAlloc(alloc, ".");
    defer alloc.free(rootPath);

    const inserted: usize = 96;
    {
        const recorder = try DataRecorder.init(alloc, rootPath);
        defer recorder.deinit(alloc);

        for (0..inserted) |i| {
            var batch = [_]Line{stableLine(@intCast(i + 1), 1, i)};
            try recorder.addLines(alloc, batch[0..], batch[0].fieldsSize());
        }

        recorder.stopped.store(true, .release);
        recorder.wg.wait();
        try recorder.flushForce(alloc);

        try testing.expectEqual(@as(usize, 0), recorder.memTables.items.len);
        try testing.expect(recorder.diskTables.items.len > 0);
        try testing.expectEqual(@as(u64, 0), countMemLinesInRecorder(recorder));
        try testing.expectEqual(@as(u64, inserted), countDiskLinesInRecorder(recorder));
    }

    {
        const reopened = try DataRecorder.init(alloc, rootPath);
        reopened.stopped.store(true, .release);
        reopened.wg.wait();
        defer reopened.deinit(alloc);

        try testing.expect(reopened.diskTables.items.len > 0);
        try testing.expectEqual(0, countMemLinesInRecorder(reopened));
        try testing.expectEqual(inserted, countDiskLinesInRecorder(reopened));
    }
}
