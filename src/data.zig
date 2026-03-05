const std = @import("std");
const Allocator = std.mem.Allocator;

const Conf = @import("Conf.zig");
const Line = @import("store/lines.zig").Line;

const cap = @import("store/table/cap.zig");

const MemTable = @import("store/inmem/MemTable.zig");
const BlockReader = @import("store/inmem/reader.zig").BlockReader;

const TableType = enum(u8) {
    inmemory = 0,
    disk = 1,
};

// TODO: move flush interval to config
fn setFlushTime() i64 {
    // now + 1s
    return std.time.microTimestamp() + std.time.us_per_s;
}

pub const DataShard = struct {
    mx: std.Thread.Mutex = .{},
    lines: std.ArrayList(*const Line) = .empty,
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
    fn flush(self: *DataShard, allocator: Allocator, sem: *std.Thread.Semaphore) !?*MemTable {
        if (self.lines.items.len == 0) {
            return null;
        }

        sem.wait();
        errdefer sem.post();

        const memTable = try MemTable.init(allocator);
        try memTable.addLines(allocator, self.lines.items);
        self.lines.clearRetainingCapacity();

        sem.post();

        memTable.flushAtUs = setFlushTime();
        return memTable;
    }
};

pub const Data = struct {
    shards: []DataShard,
    nextShard: std.atomic.Value(usize),
    mergeIdx: std.atomic.Value(usize),

    mxTables: std.Thread.Mutex,
    memTables: std.ArrayList(*MemTable),

    pool: *std.Thread.Pool,
    wg: std.Thread.WaitGroup,
    memMergeSem: std.Thread.Semaphore,
    stopped: std.atomic.Value(bool),

    path: []const u8,

    pub fn init(allocator: Allocator, workersAllocator: Allocator, path: []const u8) !*Data {
        const conf = Conf.getConf().server.pools;
        std.debug.assert(conf.cpus != 0);
        // 4 is a minimum amount for workers:
        // data shards flushare, mem table flusher, mem table merger, disk table merger
        std.debug.assert(conf.workerThreads >= 4);

        const shards = try allocator.alloc(DataShard, conf.cpus);
        errdefer allocator.free(shards);
        for (shards) |*shard| {
            shard.* = .{};
        }

        var pool = try allocator.create(std.Thread.Pool);
        errdefer allocator.destroy(pool);
        try pool.init(.{
            .allocator = allocator,
            .n_jobs = conf.workerThreads,
        });
        errdefer pool.deinit();

        const wg: std.Thread.WaitGroup = .{};

        const self = try allocator.create(Data);

        self.* = Data{
            .shards = shards,
            .nextShard = std.atomic.Value(usize).init(0),
            .mergeIdx = std.atomic.Value(usize).init(0),

            .mxTables = .{},
            .memTables = std.ArrayList(*MemTable).empty,

            .pool = pool,
            .wg = wg,
            .memMergeSem = .{ .permits = conf.cpus },
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

    pub fn deinit(self: *Data, allocator: Allocator) void {
        self.stopped.store(true, .release);
        self.wg.wait();
        self.pool.deinit();
        allocator.destroy(self.pool);
        allocator.free(self.shards);
        allocator.destroy(self);
    }

    fn startMemTableFlusher(self: *Data, allocator: Allocator) void {
        var arena = std.heap.ArenaAllocator.init(allocator);
        defer arena.deinit();

        const alloc = arena.allocator();
        var iteration: usize = 0;
        while (!self.stopped.load(.acquire)) {
            std.Thread.sleep(std.time.ns_per_s);
            self.flushMemTable(alloc, false) catch unreachable;
            _ = arena.reset(.retain_capacity);
            iteration += 1;
        }
        self.flushMemTable(alloc, true) catch unreachable;
    }

    fn flushMemTable(self: *Data, allocator: Allocator, force: bool) !void {
        const nowUs = std.time.microTimestamp();

        self.mxTables.lock();
        defer self.mxTables.unlock();

        var tables = std.ArrayList(*MemTable).initCapacity(allocator, self.memTables.items.len) catch |err| {
            self.handleErr(err);
            return;
        };
        for (self.memTables.items) |memTable| {
            const isTimeToMerge = if (memTable.flushAtUs) |flushAtUs| nowUs > flushAtUs else false;
            if (!memTable.isInMerge and (force or isTimeToMerge)) {
                tables.appendAssumeCapacity(memTable);
            }
        }

        // TODO: reshuffle tables to merge in order to build more effective file sizes
        self.memMergeSem.wait();
        try self.mergeTables(allocator, tables.items, force, &self.stopped);
        self.memMergeSem.post();
    }

    /// startDataShardsFlusher runs a worker to flush DataShard on flushAtUs
    fn startDataShardsFlusher(self: *Data, allocator: Allocator) void {
        // half a sec
        const flushInterval = std.time.ns_per_s / 2;

        var arena = std.heap.ArenaAllocator.init(allocator);
        defer arena.deinit();

        const alloc = arena.allocator();
        while (!self.stopped.load(.acquire)) {
            std.Thread.sleep(flushInterval);
            self.flushDataShards(alloc, false);
            _ = arena.reset(.retain_capacity);
        }
        self.flushDataShards(alloc, true);
    }

    fn flushDataShards(self: *Data, allocator: Allocator, force: bool) void {
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

    fn flushShard(self: *Data, alloc: Allocator, shard: *DataShard) void {
        const maybeMemTable = shard.flush(alloc, &self.memMergeSem) catch |err| {
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
            self.pool.spawnWg(&self.wg, startMemTableMerger, .{ self, alloc });
        }
    }

    fn handleErr(self: *Data, err: anyerror) void {
        std.debug.print("ERROR: failed to flush a data shard, err={}\n", .{err});
        self.stopped.store(true, .release);
        // TODO: broadcast the app must close
        return;
    }

    fn startMemTableMerger(self: *Data, allocator: Allocator) void {
        if (self.stopped.load(.acquire)) return;

        self.pool.spawnWg(&self.wg, runMemTableMerger, .{ self, allocator });
    }

    fn runMemTableMerger(self: *Data, alloc: Allocator) void {
        self.tablesMerger(alloc, &self.memTables, &self.memMergeSem) catch |err| {
            std.debug.print("failed to merge mem tables: {s}\n", .{@errorName(err)});
        };
    }

    fn tablesMerger(
        self: *Data,
        alloc: Allocator,
        tables: *std.ArrayList(*MemTable),
        sem: *std.Thread.Semaphore,
    ) !void {
        var tablesToMerge = std.ArrayList(*MemTable).empty;
        defer tablesToMerge.deinit(alloc);

        while (true) {
            const maxDiskTableSize = cap.getMaxTableSize(self.path);

            self.mxTables.lock();
            errdefer self.mxTables.unlock();
            // filteredTablesToMerge is a slice of tables ArrayList, no need to free it
            const window = try filterTablesToMerge(alloc, tables.items, &tablesToMerge, maxDiskTableSize);
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

    const MergeWindowBound = struct {
        upper: usize,
        lower: usize,
    };

    fn filterTablesToMerge(
        alloc: Allocator,
        tables: []*MemTable,
        toMerge: *std.ArrayList(*MemTable),
        maxDiskTableSize: u64,
    ) Allocator.Error!?MergeWindowBound {
        _ = alloc;
        _ = tables;
        _ = toMerge;
        _ = maxDiskTableSize;
        unreachable;
    }

    fn getDstTableType(self: *Data, allocator: Allocator) TableType {
        _ = self;
        _ = allocator;

        // TODO: implement a decision strategy
        return TableType.inmemory;
    }

    fn getDstTablePath(self: *Data, allocator: Allocator, dstTableType: TableType) ![]const u8 {
        // In-memory tables do not have a destination path.
        if (dstTableType == .inmemory) {
            return "";
        }

        // For file-backed tables, allocate exact buffer and format the path into it.
        const buf_len = self.path.len + 1 + 16; // "{path}/{mergeIdx_as_16_hex}"
        const destinationTablePath = try allocator.alloc(u8, buf_len);
        const mergeIdx = self.nextMergeIdx();
        _ = try std.fmt.bufPrint(
            destinationTablePath,
            "{s}/{X:0>16}",
            .{ self.path, mergeIdx },
        );

        return destinationTablePath;
    }

    fn nextMergeIdx(self: *Data) usize {
        return self.mergeIdx.fetchAdd(1, .acq_rel);
    }

    fn timer() std.time.Timer {
        return std.time.Timer.start() catch |err| std.debug.panic("failed to start timer: {s}", .{@errorName(err)});
    }

    // TODO: implement it
    fn openBlockStreamReaders(allocator: Allocator) ![]*BlockReader {
        const readers = try allocator.alloc(*BlockReader, 1);
        return readers;
    }

    fn mergeTables(
        self: *Data,
        alloc: Allocator,
        tables: []*MemTable,
        force: bool,
        stopped: ?*std.atomic.Value(bool),
    ) !void {
        _ = stopped;
        if (tables.len == 0) {
            return;
        }

        for (tables) |table| {
            std.debug.assert(table.isInMerge);
        }

        defer {
            self.mxTables.lock();
            for (tables) |table| {
                std.debug.assert(table.isInMerge);
                table.isInMerge = false;
            }
            self.mxTables.unlock();
        }

        var t = timer();
        const dstTableType = self.getDstTableType(alloc);
        if (dstTableType != .inmemory) {
            // TODO: do some disk reservations when disk type
        }

        switch (dstTableType) {
            // TODO: track progress
            .inmemory => {},
            .disk => {},
        }
        const readers = openBlockStreamReaders(alloc) catch std.debug.panic("failed to open block readers", .{});
        _ = readers;
        const dstPathTable = self.getDstTablePath(alloc, dstTableType) catch std.debug.panic("path problem dstPathPart", .{});
        defer if (dstTableType != .inmemory and dstPathTable.len > 0) alloc.free(dstPathTable);

        if (force and tables.len == 1 and dstTableType != .inmemory) {
            tables[0].flushToDisk(alloc, dstPathTable) catch std.debug.panic("failed to flush to disk path={s}", .{dstPathTable});
        }

        _ = t.lap();
        unreachable;
    }

    pub fn addLines(self: *Data, alloc: Allocator, lines: []*const Line, size: usize) !void {
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
};

fn tableSliceToMerge(alloc: Allocator, tables: *std.ArrayList(*MemTable), _: u64) !?[]*MemTable {
    var size: usize = 0;
    for (tables.items) |t| {
        if (!t.isInMerge) size += 1;
    }
    const interSlice = try alloc.alloc(*MemTable, size);
    defer alloc.free(interSlice);

    const maybeSlice = try filterToMerge(alloc, interSlice);
    const slice = maybeSlice orelse return null;
    for (slice) |t| {
        std.debug.assert(!t.isInMerge);
        t.isInMerge = true;
    }
    return slice;
}

fn filterToMerge(alloc: Allocator, tables: []*MemTable) !?[]*MemTable {
    if (tables.len < 2) return null;

    // TODO: not implemented
    const res = try alloc.alloc(*MemTable, tables.len);
    for (0..tables.len) |i| {
        res[i] = tables[i];
    }
    return tables;
}

const testing = std.testing;

test "dataWorker" {
    const alloc = testing.allocator;
    _ = try Conf.default(alloc);
    defer Conf.deinit();

    var d = try Data.init(alloc, alloc, "abc"[0..]);
    std.Thread.sleep(2 * std.time.ns_per_s);
    d.deinit(alloc);
}
