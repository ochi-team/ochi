const std = @import("std");

const Conf = @import("conf.zig");
const Line = @import("store/lines.zig").Line;

const TableMem = @import("store/inmem/TableMem.zig");
const BlockReader = @import("store/inmem/reader.zig").BlockReader;

const maxLevelSize = 100 * 1024 * 1024 * 1024;

const TableType = enum(u8) {
    inmemory = 0,
    disk = 1,
};

inline fn setFlushTime() i64 {
    // now + 1s
    return std.time.microTimestamp() + std.time.us_per_s;
}

pub const DataShard = struct {
    mx: std.Thread.Mutex = .{},
    lines: std.ArrayList(*const Line) = std.ArrayList(*const Line).empty,
    // TODO: currently there is a single background process flushing the data shards
    // try instead assign a timer task to a shard and benchmark on high amount of shard (high amount of cpu)
    flushAtUs: ?i64 = null,

    // threshold as 90% of a max block size
    const flushThreshold = 9 * (TableMem.maxBlockSize / 10);
    fn mustFlush(_: *DataShard) bool {
        // TODO: this is calculated not very precise, could be more optimal,
        // has to be fixed on understanding more incoming lines life time
        // return self.size >= flushThreshold;
        return true;
    }

    // flush sends all the data to a mem Table,
    // is not a thread safe, assumes the shard is locked
    fn flush(self: *DataShard, allocator: std.mem.Allocator, sem: *std.Thread.Semaphore) !?*TableMem {
        if (self.lines.items.len == 0) {
            return null;
        }

        sem.wait();
        errdefer sem.post();

        self.flushAtUs = null;
        const memTable = try TableMem.init(allocator);
        try memTable.addLines(allocator, self.lines.items);

        sem.post();

        memTable.flushAtUs = setFlushTime();
        return memTable;
    }
};

pub const Data = struct {
    shards: []DataShard,
    nextShard: std.atomic.Value(usize),
    mergeIdx: std.atomic.Value(usize),

    mx: std.Thread.Mutex,
    memTables: std.ArrayList(*TableMem),

    pool: *std.Thread.Pool,
    wg: std.Thread.WaitGroup,
    memTableSem: std.Thread.Semaphore,
    stopped: std.atomic.Value(bool),

    path: []const u8,

    pub fn init(allocator: std.mem.Allocator, workersAllocator: std.mem.Allocator, path: []const u8) !*Data {
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

            .mx = .{},
            .memTables = std.ArrayList(*TableMem).empty,

            .pool = pool,
            .wg = wg,
            .memTableSem = .{ .permits = conf.cpus },
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

    pub fn deinit(self: *Data, allocator: std.mem.Allocator) void {
        self.stopped.store(true, .release);
        self.wg.wait();
        self.pool.deinit();
        allocator.destroy(self.pool);
        allocator.free(self.shards);
        allocator.destroy(self);
    }

    fn startMemTableFlusher(self: *Data, allocator: std.mem.Allocator) void {
        var arena = std.heap.ArenaAllocator.init(allocator);
        defer arena.deinit();

        const alloc = arena.allocator();
        var iteration: usize = 0;
        while (!self.stopped.load(.acquire)) {
            std.Thread.sleep(std.time.ns_per_s);
            self.flushMemTable(alloc, false);
            _ = arena.reset(.retain_capacity);
            iteration += 1;
        }
        self.flushMemTable(alloc, true);
    }

    fn flushMemTable(self: *Data, allocator: std.mem.Allocator, force: bool) void {
        const nowUs = std.time.microTimestamp();

        self.mx.lock();
        defer self.mx.unlock();

        var tables = std.ArrayList(*TableMem).initCapacity(allocator, self.memTables.items.len) catch |err| {
            self.handleErr(err);
            return;
        };
        for (self.memTables.items) |memTable| {
            const isTimeToMerge = if (memTable.flushAtUs) |flushAtUs| nowUs > flushAtUs else false;
            if (!memTable.isInMerge and (force or isTimeToMerge)) {
                tables.appendAssumeCapacity(memTable);
            }
        }

        // TODO: reshuffle parts to merge in order to build more effective file sizes
        self.memTableSem.wait();
        self.mergeTables(allocator, tables.items, force);
        self.memTableSem.post();
    }

    /// startDataShardsFlusher runs a worker to flush DataShard on flushAtUs
    fn startDataShardsFlusher(self: *Data, allocator: std.mem.Allocator) void {
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

    fn flushDataShards(self: *Data, allocator: std.mem.Allocator, force: bool) void {
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

    fn flushShard(self: *Data, allocator: std.mem.Allocator, shard: *DataShard) void {
        const maybeMemTable = shard.flush(allocator, &self.memTableSem) catch |err| {
            self.handleErr(err);
            return;
        };
        if (maybeMemTable) |memTable| {
            self.mx.lock();
            defer self.mx.unlock();
            self.memTables.append(allocator, memTable) catch |err| {
                self.handleErr(err);
                return;
            };
            self.pool.spawnWg(&self.wg, startMemTableMerger, .{ self, allocator });
        }
    }

    fn handleErr(self: *Data, err: anyerror) void {
        std.debug.print("ERROR: failed to flush a data shard, err={}\n", .{err});
        self.stopped.store(true, .release);
        // TODO: broadcast the app must close
        return;
    }

    fn startMemTableMerger(self: *Data, allocator: std.mem.Allocator) void {
        while (true) {
            if (self.stopped.load(.acquire)) return;

            // TODO: validate it has enough space for the max amount
            const maxSize = maxLevelSize;
            self.mx.lock();
            var fallbackFba = std.heap.stackFallback(1024, allocator);
            const alloc = fallbackFba.get();
            const maybeMemTables = tableSliceToMerge(alloc, &self.memTables, maxSize) catch |err| {
                self.handleErr(err);
                return;
            };
            self.mx.unlock();

            const memTables = maybeMemTables orelse return;
            if (memTables.len == 0) return;

            defer alloc.free(memTables);

            self.memTableSem.wait();
            self.mergeTables(allocator, memTables, false);
            self.memTableSem.post();
        }
    }

    fn getDstTableType(self: *Data, allocator: std.mem.Allocator) TableType {
        _ = self;
        _ = allocator;

        // TODO: implement a decision strategy
        return TableType.inmemory;
    }

    fn getDstTablePath(self: *Data, allocator: std.mem.Allocator, dstTableType: TableType) ![]const u8 {
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
    fn openBlockStreamReaders(allocator: std.mem.Allocator) ![]*BlockReader {
        const readers = try allocator.alloc(*BlockReader, 1);
        return readers;
    }

    fn mergeTables(self: *Data, alloc: std.mem.Allocator, tables: []*TableMem, force: bool) void {
        if (tables.len == 0) {
            return;
        }

        for (tables) |table| {
            std.debug.assert(table.isInMerge);
        }

        defer {
            self.mx.lock();
            for (tables) |table| {
                std.debug.assert(table.isInMerge);
                table.isInMerge = false;
            }
            self.mx.unlock();
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
    }

    // FIXME: allocator must be the same as in the background workers to have same source of ownership for mem tables
    pub fn addLines(self: *Data, allocator: std.mem.Allocator, lines: std.ArrayList(*const Line)) void {
        const i = self.nextShard.fetchAdd(1, .acquire) % self.shards.len;
        var shard = &self.shards[i];

        shard.mx.lock();

        if (shard.flushAtUs == null) {
            shard.flushAtUs = setFlushTime();
        }
        shard.lines = lines;
        if (shard.mustFlush()) {
            self.flushShard(allocator, shard);
        }

        shard.mx.unlock();
    }
};

fn tableSliceToMerge(alloc: std.mem.Allocator, tables: *std.ArrayList(*TableMem), _: u64) !?[]*TableMem {
    var size: usize = 0;
    for (tables.items) |t| {
        if (!t.isInMerge) size += 1;
    }
    const interSlice = try alloc.alloc(*TableMem, size);
    defer alloc.free(interSlice);

    const maybeSlice = try filterToMerge(alloc, interSlice);
    const slice = maybeSlice orelse return null;
    for (slice) |t| {
        std.debug.assert(!t.isInMerge);
        t.isInMerge = true;
    }
    return slice;
}

fn filterToMerge(alloc: std.mem.Allocator, tables: []*TableMem) !?[]*TableMem {
    if (tables.len < 2) return null;

    // TODO: not implemented
    const res = try alloc.alloc(*TableMem, tables.len);
    for (0..tables.len) |i| {
        res[i] = tables[i];
    }
    return tables;
}

test "dataWorker" {
    _ = try Conf.default();

    const alloc = std.testing.allocator;

    var d = try Data.init(alloc, alloc, "abc"[0..]);
    std.Thread.sleep(2 * std.time.ns_per_s);
    d.deinit(alloc);
}
