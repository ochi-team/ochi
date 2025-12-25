const std = @import("std");

const getConf = @import("conf.zig").getConf;
const Line = @import("store/lines.zig").Line;

const TableMem = @import("store/inmem/TableMem.zig");

const maxLevelSize = 100 * 1024 * 1024 * 1024;

inline fn setFlushTime() i64 {
    return std.time.microTimestamp() + std.time.us_per_s;
}

pub const DataShard = struct {
    mx: std.Thread.Mutex = .{},
    lines: std.ArrayList(*const Line) = std.ArrayList(*const Line).empty,
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

    mx: std.Thread.Mutex,
    memTables: std.ArrayList(*TableMem),

    pool: *std.Thread.Pool,
    wg: std.Thread.WaitGroup,
    sem: std.Thread.Semaphore,
    stopped: std.atomic.Value(bool),

    pub fn init(allocator: std.mem.Allocator, workersAllocator: std.mem.Allocator) !*Data {
        const conf = getConf().server.pools;
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

            .mx = .{},
            .memTables = std.ArrayList(*TableMem).empty,

            .pool = pool,
            .wg = wg,
            .sem = .{ .permits = conf.cpus },
            .stopped = std.atomic.Value(bool).init(false),
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
        _ = self;
        _ = allocator;
        std.debug.print("flush completed with force={}\n", .{force});
    }

    /// startDataShardsFlusher runs a worker to flush DataShard on flushAtUs
    fn startDataShardsFlusher(self: *Data, allocator: std.mem.Allocator) void {
        var arena = std.heap.ArenaAllocator.init(allocator);
        defer arena.deinit();

        const alloc = arena.allocator();
        while (!self.stopped.load(.acquire)) {
            std.Thread.sleep(std.time.ns_per_s);
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
        const maybeMemTable = shard.flush(allocator, &self.sem) catch |err| {
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

            self.sem.wait();
            self.mergeTables(allocator, memTables, false);
            self.sem.post();
        }
    }

    fn mergeTables(self: *Data, alloc: std.mem.Allocator, tables: []*TableMem, force: bool) void {
        _ = self;
        _ = alloc;
        _ = tables;
        _ = force;
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

const Conf = @import("conf.zig").Conf;
test "dataWorker" {
    _ = Conf.default();

    const alloc = std.testing.allocator;
    var d = try Data.init(alloc, alloc);
    std.Thread.sleep(2 * std.time.ns_per_s);
    d.deinit(alloc);
}
