const std = @import("std");
const Allocator = std.mem.Allocator;
const Io = std.Io;
const Dir = Io.Dir;

const zeit = @import("zeit");

const tracy = @import("tracy");
const xev = @import("xev");

const Layout = @import("Layout.zig");
const Stop = @import("stds/Stop.zig");

const StoreMeter = @import("observe/StoreMeter.zig");
const Logger = @import("logging");
const Cache = @import("stds/Cache.zig").Cache;
const Line = @import("store/lines.zig").Line;
const SID = @import("store/lines.zig").SID;
const Field = @import("store/lines.zig").Field;
const freeFields = @import("store/lines.zig").freeFields;
const deinitLinesFull = @import("store/lines.zig").deinitLinesFull;
const lineLatestFirst = @import("store/lines.zig").lineLatestFirst;
const Query = @import("query/Query.zig");

const TimerLoop = @import("stds/xev/TimerLoop.zig");

const Partition = @import("Partition.zig");
const MemBlock = @import("store/index/MemBlock.zig");
const filenames = @import("filenames.zig");
const Conf = @import("Conf.zig");
const Runtime = @import("Runtime.zig");
const TimestampsEncoder = @import("store/data/TimestampsEncoder.zig");
const CompressionPool = @import("store/compression/CompressionPool.zig");
const DecompressionPool = @import("store/compression/DecompressionPool.zig");
const LookupPool = @import("store/index/lookup/LookupPool.zig");
const QueryIndexCacheValue = @import("store/index/Index.zig").QueryIndexCacheValue;

pub const Store = @This();

/// lockFile is used to ensure only one instance is running
/// in order to prevent data corruption
lockFile: Io.File,

// TODO: review all the Mutex usage and replace to RwMutex if possible
// TODO: this collection rarely gets updated, it's better to use a sequence lock with retries
partitionsMx: Io.Mutex = .init,
partitions: std.ArrayList(*Partition) = .empty,
lruPartition: ?*Partition = null,

/// streamCache is a stream id cache for ingestion,
/// shared across all partitions, injected from a store to them
streamCache: *Cache(void),
indexMemBlocksCache: *Cache(*MemBlock),
indexQueryCache: *Cache(*QueryIndexCacheValue),
timestampsEncoders: *TimestampsEncoder.TimestampsEncoderPool,
compressionPool: *CompressionPool,
decompressionPool: *DecompressionPool,
lookupPool: *LookupPool,

threadPool: *xev.ThreadPool,

meter: StoreMeter,

timerLoop: *TimerLoop,
tickCtx: TaskCtx = undefined,
stopped: Stop = .{},

/// pathsBuf holds a garbage of created paths for partitions and it's tables
pathsBuf: std.ArrayList([]const u8) = .empty,

// runtime stats
runtime: *Runtime,
conf: *const Conf,

// 30 is a default retention
const retentionDays = 30;

const diskUsageSampleIntervalNs = 20 * std.time.ns_per_s;
const cacheCleanIntervalNs = 30 * std.time.ns_per_s;

const TaskCtx = struct {
    store: *Store,
    io: Io,
    alloc: Allocator,
};

// TODO: start partitions retention watcher
pub fn init(io: Io, alloc: Allocator, conf: *const Conf, runtime: *Runtime, layout: Layout) !Store {
    errdefer |err| {
        Logger.log(.err, "unexpected failure to init Store", .{ .err = err });
    }

    const file = try createLockFile(io, runtime.path);
    errdefer file.close(io);

    var partitions = try std.ArrayList(*Partition).initCapacity(alloc, retentionDays);
    errdefer {
        for (partitions.items) |partition| {
            partition.close(io);
        }
        partitions.deinit(alloc);
    }

    var streamCache = try Cache(void).init(io, alloc, .{ .meter = .{ .name = "stream" } });
    errdefer streamCache.deinit();

    const indexMemBlocksCache = try Cache(*MemBlock).init(io, alloc, .{ .meter = .{ .name = "mem_blocks" } });
    errdefer indexMemBlocksCache.deinit();

    const indexQueryCache = try Cache(*QueryIndexCacheValue).init(io, alloc, .{ .meter = .{ .name = "index_query" } });
    errdefer indexQueryCache.deinit();

    const timestampsEncoders = try TimestampsEncoder.TimestampsEncoderPool.init(alloc, runtime.cpus);
    errdefer timestampsEncoders.deinit(alloc);

    const compressionPool = try CompressionPool.init(alloc, runtime.cpus);
    errdefer compressionPool.deinit(alloc);

    const decompressionPool = try DecompressionPool.init(alloc, runtime.cpus);
    errdefer decompressionPool.deinit(alloc);

    const lookupPool = try LookupPool.init(alloc, runtime.cpus);
    errdefer lookupPool.deinit(io, alloc);

    const threadPool = try alloc.create(xev.ThreadPool);
    errdefer alloc.destroy(threadPool);
    threadPool.* = .init(.{ .max_threads = runtime.cpus });
    errdefer {
        threadPool.shutdown();
        threadPool.deinit();
    }

    var meter = try StoreMeter.init(io, alloc);
    errdefer meter.deinit();

    const timerLoop = try TimerLoop.init(alloc);
    errdefer timerLoop.deinit();

    var store: Store = .{
        .lockFile = file,
        .partitions = partitions,
        .streamCache = streamCache,
        .indexMemBlocksCache = indexMemBlocksCache,
        .indexQueryCache = indexQueryCache,
        .timestampsEncoders = timestampsEncoders,
        .compressionPool = compressionPool,
        .decompressionPool = decompressionPool,
        .lookupPool = lookupPool,
        .threadPool = threadPool,
        .meter = meter,
        .runtime = runtime,
        .conf = conf,
        .timerLoop = timerLoop,
    };

    // TODO: try making it parallel, it speed up start up time
    var it = layout.partitionsDir.iterate();
    // TODO: ban while loops via linter and set explicit loops
    while (try it.next(io)) |entry| {
        if (entry.kind != .directory and entry.kind != .sym_link) {
            Logger.log(.warn, "partitions folder has an unexpected file", .{
                .filename = entry.name,
                .kind = @tagName(entry.kind),
            });
            continue;
        }

        const partitionPath = try std.fs.path.join(alloc, &.{ layout.partitionsPath, entry.name });
        errdefer alloc.free(partitionPath);

        const day = try dayFromKey(io, entry.name);
        const indexPath = try std.fs.path.join(alloc, &.{ partitionPath, filenames.indexTables });
        errdefer alloc.free(indexPath);
        const dataPath = try std.fs.path.join(alloc, &.{ partitionPath, filenames.dataTables });
        errdefer alloc.free(dataPath);

        // discard partition because it appends it to the store state
        _ = try store.openPartition(io, alloc, partitionPath, indexPath, dataPath, day);
    }

    std.sort.pdq(*Partition, store.partitions.items, {}, Partition.lessThan);

    store.lruPartition = if (store.partitions.items.len > 0)
        store.partitions.items[store.partitions.items.len - 1]
    else
        null;

    return store;
}

pub fn start(self: *Store, io: Io, alloc: Allocator) !void {
    errdefer self.stopped.stop(io);

    self.tickCtx = .{ .store = self, .io = io, .alloc = alloc };

    try self.timerLoop.addTimer(diskUsageSampleIntervalNs, &self.tickCtx, diskUsageSamplerTick);
    try self.timerLoop.addTimer(cacheCleanIntervalNs, &self.tickCtx, cacheEvicterTick);
    try self.timerLoop.start();
}

pub fn deinit(self: *Store, io: Io, alloc: Allocator) void {
    self.stopped.stop(io);
    self.timerLoop.stop();
    self.timerLoop.join();
    self.timerLoop.deinit();

    for (self.partitions.items) |partition| {
        partition.release(io);
    }
    self.partitions.deinit(alloc);

    for (self.pathsBuf.items) |path| {
        alloc.free(path);
    }
    self.pathsBuf.deinit(alloc);

    self.streamCache.deinit();
    self.indexMemBlocksCache.deinit();
    self.indexQueryCache.deinit();

    self.meter.deinit();

    self.timestampsEncoders.deinit(alloc);
    self.compressionPool.deinit(alloc);
    self.decompressionPool.deinit(alloc);
    self.lookupPool.deinit(io, alloc);

    self.threadPool.shutdown();
    self.threadPool.deinit();
    alloc.destroy(self.threadPool);

    // close lock file later, it unlocks potentially another Ochi process
    self.lockFile.close(io);
    self.* = undefined;
}

// Store tracks disk usage so:
// - operators can alert before running out of space
// - eviction policy can rely on the current usage instead of static thresholds
// TODO: handle partitions eviction due to max usage config
// TODO: find a way to move the meter to an infra level,
// and make this meter watching max usage to guard the store to run out of space
fn diskUsageSamplerTick(ctx: *anyopaque) void {
    const taskCtx: *TaskCtx = @ptrCast(@alignCast(ctx));
    taskCtx.store.writeStoreMeter(taskCtx.io, taskCtx.alloc);
}

fn cacheEvicterTick(ctx: *anyopaque) void {
    const taskCtx: *TaskCtx = @ptrCast(@alignCast(ctx));
    taskCtx.store.streamCache.clean();
    taskCtx.store.indexMemBlocksCache.clean();
    taskCtx.store.indexQueryCache.clean();
}

fn writeStoreMeter(self: *Store, io: Io, alloc: Allocator) void {
    const usage = self.readStoreUsage(io, alloc) catch |err| switch (err) {
        error.FileNotFound => 0,
        else => {
            Logger.log(.err, "failed to read store disk usage", .{ .err = err });
            return;
        },
    };
    self.meter.diskUsage.set(usage);

    self.writeTableStats(io) catch |err| {
        Logger.log(.err, "failed to write tables stats", .{ .err = err });
        return;
    };
}

// TODO: the implementation watches the files stats, it's not efficient,
// because the existing partitions size never change and either the tables size,
// instead we must collect size stats from the partitions directly on opening the tables
fn readStoreUsage(self: *Store, io: Io, alloc: Allocator) !u64 {
    return readDirUsage(io, alloc, self.runtime.path);
}

fn readDirUsage(io: Io, alloc: Allocator, root: []const u8) !u64 {
    var stack: std.ArrayList([]u8) = try .initCapacity(alloc, 32);
    defer {
        for (stack.items) |path| alloc.free(path);
        stack.deinit(alloc);
    }

    const rootPath = try alloc.dupe(u8, root);
    stack.appendAssumeCapacity(rootPath);

    var total: u64 = 0;

    while (stack.pop()) |path| {
        defer alloc.free(path);

        var dir = if (std.fs.path.isAbsolute(path))
            try std.Io.Dir.openDirAbsolute(io, path, .{ .iterate = true })
        else
            try std.Io.Dir.cwd().openDir(io, path, .{ .iterate = true });
        defer dir.close(io);

        var it = dir.iterate();
        while (try it.next(io)) |entry| {
            switch (entry.kind) {
                .directory => {
                    const childPath = try std.fs.path.join(alloc, &.{ path, entry.name });
                    errdefer alloc.free(childPath);
                    try stack.append(alloc, childPath);
                },
                .file => {
                    total += (try dir.statFile(io, entry.name, .{})).size;
                },
                else => {},
            }
        }
    }

    return total;
}

fn writeTableStats(self: *Store, io: Io) !void {
    var partsBuf: [retentionDays]*Partition = undefined;
    const partsLen = self.getPartitions(io, 0, std.math.maxInt(u32), &partsBuf);
    const parts = partsBuf[0..partsLen];
    defer for (parts) |part| part.release(io);

    for (parts) |part| {
        try part.index.recorder.mxTables.lock(io);
        const openIndexMemTables: u32 = @intCast(part.index.recorder.memTables.items.len);
        const openIndexDiskTables: u32 = @intCast(part.index.recorder.diskTables.items.len);
        try self.meter.openTables.set(
            .{ .kind = "index", .partition = part.key, .residence = "mem" },
            openIndexMemTables,
        );
        try self.meter.openTables.set(
            .{ .kind = "index", .partition = part.key, .residence = "disk" },
            openIndexDiskTables,
        );
        part.index.recorder.mxTables.unlock(io);

        try part.data.mxTables.lock(io);
        const openDataMemTables: u32 = @intCast(part.data.memTables.items.len);
        const openDataDiskTables: u32 = @intCast(part.data.diskTables.items.len);
        try self.meter.openTables.set(
            .{ .kind = "data", .partition = part.key, .residence = "mem" },
            openDataMemTables,
        );
        try self.meter.openTables.set(
            .{ .kind = "data", .partition = part.key, .residence = "disk" },
            openDataDiskTables,
        );
        part.data.mxTables.unlock(io);
    }
}

pub fn addLines(
    self: *Store,
    io: Io,
    allocator: Allocator,
    lines: []Line,
    tags: []Field,
    encodedTags: []const u8,
    sid: SID,
) !void {
    const z = tracy.Zone.begin(.{
        .src = @src(),
        .name = "Store.addLines",
    });
    defer z.end();

    if (lines.len == 0) return;
    // TODO: make partition interval configurable
    // in order to being able to test shorter partitions: 1, 2, 3, 6, 12 hours
    const nowNs: u64 = @intCast(Io.Timestamp.now(io, .real).nanoseconds);
    const minDay = (nowNs - self.conf.app.storeRetentionNs()) / std.time.ns_per_day;
    // limit the incoming logs to now + 1 day,
    // in case an ingestor sends data with broken timezone or timestamp
    const maxDay = (nowNs + std.time.ns_per_day) / std.time.ns_per_day;

    var idx: usize = 0;
    // Hot path if all Lines belong to the same Partition
    hotpath: {
        const firstDay: u32 = @intCast(lines[0].timestampNs / std.time.ns_per_day);
        if (firstDay < minDay or firstDay > maxDay) break :hotpath;
        //skip first element
        idx = 1;

        while (idx < lines.len) : (idx += 1) {
            const day: u32 = @intCast(lines[idx].timestampNs / std.time.ns_per_day);
            if (day != firstDay) break;
        }
        const partition = blk: {
            self.partitionsMx.lockUncancelable(io);
            defer self.partitionsMx.unlock(io);

            break :blk try self.getPartitionOrLru(io, allocator, firstDay);
        };
        defer partition.release(io);

        var list = std.ArrayList(Line).initBuffer(lines[0..idx]);
        list.items.len = idx;
        try partition.addLines(io, allocator, list, tags, encodedTags, sid, self.indexMemBlocksCache, self.lookupPool);

        // Return early since all lines are added to the same Partition
        if (list.items.len == lines.len) return;
    }

    // If the Lines belong to different Partitions, continue where left off,
    // sort them by day, then bulk add
    var linesByInterval = std.AutoHashMap(u32, std.ArrayList(Line)).init(allocator);

    while (idx < lines.len) : (idx += 1) {
        const day: u32 = @intCast(lines[idx].timestampNs / std.time.ns_per_day);
        if (day < minDay) {
            Logger.log(.warn, "incoming log is out of the retention range", .{
                .range = "lower",
                .limit = nowNs - self.conf.app.storeRetentionNs(),
                .given = lines[idx].timestampNs,
            });
            continue;
        }
        if (day > maxDay) {
            Logger.log(.warn, "incoming log is out of the retention range", .{
                .range = "upper",
                .limit = nowNs + std.time.ns_per_day,
                .given = lines[idx].timestampNs,
            });
            continue;
        }

        const gop = try linesByInterval.getOrPut(day);
        if (gop.found_existing) {
            try gop.value_ptr.append(allocator, lines[idx]);
        } else {
            gop.value_ptr.* = .empty;
            try gop.value_ptr.append(allocator, lines[idx]);
        }
    }

    var linesIterator = linesByInterval.iterator();
    while (linesIterator.next()) |it| {
        const day = it.key_ptr.*;

        const partition = blk: {
            self.partitionsMx.lockUncancelable(io);
            defer self.partitionsMx.unlock(io);

            break :blk try self.getPartitionOrLru(io, allocator, day);
        };
        defer partition.release(io);

        try partition.addLines(io, allocator, it.value_ptr.*, tags, encodedTags, sid, self.indexMemBlocksCache, self.lookupPool);
    }
}

fn getPartitions(self: *Store, io: Io, minDay: u32, maxDay: u32, parts: []*Partition) u16 {
    self.partitionsMx.lockUncancelable(io);
    defer self.partitionsMx.unlock(io);

    const slice = selectPartitionsSliceInRange(self.partitions.items, minDay, maxDay);

    for (0..slice.len) |i| {
        const part = slice[i];
        part.retain();
        parts[i] = part;
    }

    // even 100 years retention won't overflow it
    return @intCast(slice.len);
}

pub fn queryLines(
    self: *Store,
    io: Io,
    requestArena: Allocator,
    alloc: Allocator,
    tenantID: u64,
    query: Query,
) !std.ArrayList(Line) {
    const minDay: u32 = @intCast(query.start / std.time.ns_per_day);
    const maxDay: u32 = @intCast(query.end / std.time.ns_per_day);

    var partsBuf: [retentionDays]*Partition = undefined;
    const partsLen = self.getPartitions(io, minDay, maxDay, &partsBuf);
    const parts = partsBuf[0..partsLen];
    defer for (parts) |part| part.release(io);

    var results = std.ArrayList(Line).empty;
    errdefer deinitLinesFull(requestArena, &results);
    for (parts) |part| {
        var partResults = try part.queryLines(
            io,
            requestArena,
            alloc,
            tenantID,
            query,
            self.indexMemBlocksCache,
            self.indexQueryCache,
        );
        defer partResults.deinit(requestArena);

        try results.appendSlice(requestArena, partResults.items);
    }

    // TODO: we need to make a real pagination, it's a plug not to overload the ui,
    // otherwise it fetches 10k lines and becomes unusable
    keepLatestLines(requestArena, &results, 2000);
    return results;
}

pub fn keepLatestLines(alloc: Allocator, lines: *std.ArrayList(Line), limit: usize) void {
    std.sort.pdq(Line, lines.items, {}, lineLatestFirst);
    if (lines.items.len <= limit) return;

    for (lines.items[limit..]) |line| {
        freeFields(alloc, line.fields);
    }
    lines.shrinkRetainingCapacity(limit);
}

pub fn queryStreamIDs(
    self: *Store,
    io: Io,
    requestArena: Allocator,
    alloc: Allocator,
    tenantID: u64,
    from: u64,
    to: u64,
) !std.AutoArrayHashMapUnmanaged(u128, void) {
    self.partitionsMx.lockUncancelable(io);

    const minDay: u32 = @intCast(from / std.time.ns_per_day);
    const maxDay: u32 = @intCast(to / std.time.ns_per_day);

    const slice = selectPartitionsSliceInRange(self.partitions.items, minDay, maxDay);
    var partsBuf: [retentionDays]*Partition = undefined;
    var parts = std.ArrayList(*Partition).initBuffer(&partsBuf);
    defer for (parts.items) |part| part.release(io);
    for (slice) |part| {
        part.retain();
        parts.appendAssumeCapacity(part);
    }

    self.partitionsMx.unlock(io);

    var streamIDs: std.AutoArrayHashMapUnmanaged(u128, void) = .empty;

    for (parts.items) |part| {
        var partStreamIDs = try part.queryStreamIDs(io, alloc, tenantID, self.indexMemBlocksCache, self.lookupPool);
        defer partStreamIDs.deinit(alloc);

        for (partStreamIDs.keys()) |sid| {
            try streamIDs.put(requestArena, sid, {});
        }
    }

    return streamIDs;
}

pub fn flush(self: *Store, io: Io, alloc: Allocator) !void {
    var parts = try std.ArrayList(*Partition).initCapacity(alloc, self.partitions.items.len);
    defer {
        for (parts.items) |part| part.release(io);
        parts.deinit(alloc);
    }

    self.partitionsMx.lockUncancelable(io);
    for (self.partitions.items) |part| {
        parts.appendAssumeCapacity(part);
        part.retain();
    }
    self.partitionsMx.unlock(io);

    for (self.partitions.items) |part| {
        try part.flushForce(io, alloc);
    }
}

pub fn selectPartitionsSliceInRange(partitions: []const *Partition, minDay: u32, maxDay: u32) []const *Partition {
    // Find first partition with day >= minDay
    const startIdx = std.sort.lowerBound(
        *Partition,
        partitions,
        minDay,
        orderPartitions,
    );

    // Find first partition with day > maxDay
    const slice = partitions[startIdx..];
    const end = std.sort.upperBound(
        *Partition,
        slice,
        maxDay,
        orderPartitions,
    );

    return slice[0..end];
}

fn getLruPartition(self: *Store) ?*Partition {
    if (self.lruPartition) |part| {
        part.retain();
        return part;
    }
    return null;
}

pub fn getPartition(self: *Store, io: Io, alloc: Allocator, day: u32) !*Partition {
    const n = std.sort.binarySearch(
        *Partition,
        self.partitions.items,
        day,
        orderPartitions,
    );
    if (n) |i| {
        const part = self.partitions.items[i];
        part.retain();

        self.lruPartition = part;
        return part;
    }

    // TODO: what if a partition is deleted, we might want to return null,
    // handle it outside, log a warning showing the partition is missing
    // due to being deprecated (identify whether it's out of the retention period)

    var partitionKey: [8]u8 = undefined;
    const partitionKeySlice = try partitionKeyBuf(io, &partitionKey, day);
    std.debug.assert(std.mem.eql(u8, partitionKeySlice, partitionKey[0..]));

    const partitionPath = try std.fs.path.join(alloc, &.{ self.runtime.path, filenames.partitions, partitionKeySlice });
    errdefer alloc.free(partitionPath);

    // TODO: don't allocate those paths, make it as computed properties,
    // then we can:
    // - remove pathsBuf
    // - make disk space cache rely on the store path
    const indexPath = try std.fs.path.join(alloc, &.{ partitionPath, filenames.indexTables });
    errdefer alloc.free(indexPath);
    const dataPath = try std.fs.path.join(alloc, &.{ partitionPath, filenames.dataTables });
    errdefer alloc.free(dataPath);

    Dir.accessAbsolute(io, partitionPath, .{ .read = true, .write = true }) catch |err| {
        switch (err) {
            error.FileNotFound => {
                try Partition.createDir(io, partitionPath, indexPath, dataPath);
            },
            else => return err,
        }
    };

    const part = try self.openPartition(io, alloc, partitionPath, indexPath, dataPath, day);
    part.retain();

    self.lruPartition = part;
    return part;
}

fn getPartitionOrLru(self: *Store, io: Io, alloc: Allocator, day: u32) !*Partition {
    if (self.getLruPartition()) |part|
        if (part.day == day)
            return part;

    return self.getPartition(io, alloc, day);
}

// TODO: when we get rid of pathsBuf we can path only partitions ref,
// therefore make store init returning the store in the end
fn openPartition(
    self: *Store,
    io: Io,
    alloc: Allocator,
    path: []const u8,
    indexPath: []const u8,
    dataPath: []const u8,
    day: u32,
) !*Partition {
    try self.partitions.ensureUnusedCapacity(alloc, 1);
    try self.pathsBuf.ensureUnusedCapacity(alloc, 3);

    const partition = try Partition.open(
        io,
        alloc,
        path,
        indexPath,
        dataPath,
        day,
        self.streamCache,
        self.runtime,
        self.timestampsEncoders,
        self.compressionPool,
        self.decompressionPool,
        self.threadPool,
        self.timerLoop,
    );

    self.pathsBuf.appendAssumeCapacity(path);
    self.pathsBuf.appendAssumeCapacity(indexPath);
    self.pathsBuf.appendAssumeCapacity(dataPath);
    self.partitions.appendAssumeCapacity(partition);

    return partition;
}

fn orderPartitions(day: u32, part: *Partition) std.math.Order {
    if (day < part.day) {
        return .lt;
    }
    if (day > part.day) {
        return .gt;
    }
    return .eq;
}

fn createLockFile(io: Io, path: []const u8) !Io.File {
    std.debug.assert(path[path.len - 1] != std.fs.path.sep);

    var lockFilePathBuf: [std.fs.max_path_bytes]u8 = undefined;
    const lockFilePath = try std.fmt.bufPrint(
        &lockFilePathBuf,
        "{s}{c}{s}",
        .{ path, std.fs.path.sep, filenames.lock },
    );

    // TODO: test locking mechanic carefully, perhaps we need to apply the statements below,
    // we must also test it with different devices: block, s3 fs, nfs (ceph), etc.
    // read "man flock" for details
    var file = try Dir.createFileAbsolute(io, lockFilePath, .{ .lock = .exclusive });
    errdefer file.close(io);

    // var i: u8 = 0;
    // while (i < 5) : (i += 1) {
    //     std.posix.flock(file.handle, std.posix.LOCK.EX | std.posix.LOCK.NB) catch |err| {
    //         switch (err) {
    //             // repeat once again
    //             error.WouldBlock => Io.sleep(std.time.ns_per_ms * 200),
    //             else => std.debug.panic(
    //                 "Failed to acquire lock on the store, another instance might be running, error: {s}",
    //                 .{@errorName(err)},
    //             ),
    //         }
    //     };
    // }

    return file;
}

pub fn partitionKeyBuf(io: Io, buf: []u8, day: u64) ![]u8 {
    const nowNs = day * std.time.ns_per_day;
    const inst = try zeit.instant(io, .{ .source = .{ .unix_nano = nowNs } });
    const time = inst.time();
    return std.fmt.bufPrint(buf, "{d:0>2}{d:0>2}{d:0>4}", .{ time.day, time.month, @as(u32, @intCast(time.year)) });
}

pub fn dayFromKey(io: Io, key: []const u8) !u32 {
    std.debug.assert(key.len == 8);

    const day = try std.fmt.parseInt(u64, key[0..2], 10);
    const month = try std.fmt.parseInt(u64, key[2..4], 10);
    const year = try std.fmt.parseInt(u64, key[4..8], 10);

    const monthEnum: zeit.Month = @enumFromInt(month);
    const inst = try zeit.instant(io, .{ .source = .{ .time = .{
        .day = @intCast(day),
        .month = monthEnum,
        .year = @intCast(year),
    } } });

    const ts: u64 = @intCast(inst.timestamp);
    return @intCast(ts / std.time.ns_per_day);
}
