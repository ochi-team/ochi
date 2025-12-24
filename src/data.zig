const std = @import("std");

const getConf = @import("conf.zig").getConf;
const Line = @import("store/lines.zig").Line;

const TableMem = @import("store/inmem/TableMem.zig");

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
    fn flush(self: *DataShard, allocator: std.mem.Allocator) !void {
        if (self.lines.items.len == 0) {
            return;
        }

        self.flushAtUs = null;
        const memTable = try TableMem.init(allocator);
        defer memTable.deinit(allocator);
        try memTable.addLines(allocator, self.lines.items);
    }
};

pub const Data = struct {
    shards: []DataShard,
    nextShard: std.atomic.Value(usize),

    pool: *std.Thread.Pool,
    wg: std.Thread.WaitGroup,
    stopped: std.atomic.Value(bool),

    pub fn init(allocator: std.mem.Allocator, workersAllocator: std.mem.Allocator) !*Data {
        const conf = getConf().server.pools;
        std.debug.assert(conf.cpus != 0);
        std.debug.assert(conf.workerThreads != 0);

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
            .pool = pool,
            .wg = wg,
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
                shard.flush(allocator) catch {
                    std.debug.print("ERROR: failed to flush a data shard, OOM\n", .{});
                    self.stopped.store(true, .release);
                    // TODO: broadcast the app must close
                };
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
                        shard.flush(allocator) catch {
                            std.debug.print("ERROR: failed to flush a data shard, OOM\n", .{});
                            self.stopped.store(true, .release);
                            // TODO: broadcast the app must close
                        };
                    }
                }
                shard.mx.unlock();
            }
        }
    }

    pub fn addLines(self: *Data, allocator: std.mem.Allocator, lines: std.ArrayList(*const Line)) !void {
        const i = self.nextShard.fetchAdd(1, .acquire) % self.shards.len;
        var shard = &self.shards[i];

        shard.mx.lock();

        if (shard.flushAtUs == null) {
            shard.flushAtUs = std.time.microTimestamp();
        }
        shard.lines = lines;
        if (shard.mustFlush()) {
            try shard.flush(allocator);
        }

        shard.mx.unlock();
    }
};

const Conf = @import("conf.zig").Conf;
test "dataWorker" {
    _ = Conf.default();

    const alloc = std.testing.allocator;
    var d = try Data.init(alloc, alloc);
    std.Thread.sleep(2 * std.time.ns_per_s);
    d.deinit(alloc);
}
