const std = @import("std");
const Allocator = std.mem.Allocator;
const Io = std.Io;
const Dir = Io.Dir;

const zeit = @import("zeit");
const tracy = @import("tracy");

const fs = @import("fs.zig");
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

const Partition = @import("Partition.zig");
const MemBlock = @import("store/index/MemBlock.zig");
const filenames = @import("filenames.zig");
const Conf = @import("Conf.zig");
const Runtime = @import("Runtime.zig");
const TimestampsEncoder = @import("store/data/TimestampsEncoder.zig");
const CompressionPool = @import("store/compression/CompressionPool.zig");
const DecompressionPool = @import("store/compression/DecompressionPool.zig");

pub const Store = @This();

/// lockFile is used to ensure only one instance is running
/// in order to prevent data corruption
lockFile: Io.File,

// TODO: review all the Mutex usage and replace to RwMutex if possible
partitionsMx: Io.Mutex = .init,
partitions: std.ArrayList(*Partition) = .empty,
lruPartition: ?*Partition = null,

/// streamCache is a stream id cache for ingestion,
/// shared across all partitions, injected from a store to them
streamCache: *Cache(void),
memBlocksCache: *Cache(*MemBlock),
timestampsEncoders: *TimestampsEncoder.TimestampsEncoderPool,
compressionPool: *CompressionPool,
decompressionPool: *DecompressionPool,

meter: StoreMeter,

g: Io.Group = .init,
stopped: Stop = .{},

/// pathsBuf holds a garbage of created paths for partitions and it's tables
pathsBuf: std.ArrayList([]const u8) = .empty,

// runtime stats
runtime: *Runtime,
conf: *const Conf,

// TODO: start partitions retention watcher
pub fn init(io: Io, alloc: Allocator, conf: *const Conf, runtime: *Runtime, layout: Layout) !Store {
    errdefer |err| {
        Logger.log(.err, "unexpected failure to init Store", .{ .err = err });
    }

    const file = try createLockFile(io, runtime.path);
    errdefer file.close(io);

    var streamCache = try Cache(void).init(alloc);
    errdefer streamCache.deinit();

    // 30 is a default retention
    var partitions = try std.ArrayList(*Partition).initCapacity(alloc, 30);
    errdefer {
        for (partitions.items) |partition| {
            partition.close(io);
        }
        partitions.deinit(alloc);
    }

    const memBlocksCache = try Cache(*MemBlock).init(alloc);
    errdefer memBlocksCache.deinit();

    const timestampsEncoders = try TimestampsEncoder.TimestampsEncoderPool.init(alloc, runtime.cpus);
    errdefer timestampsEncoders.deinit(alloc);

    const compressionPool = try CompressionPool.init(alloc, runtime.cpus);
    errdefer compressionPool.deinit(alloc);

    const decompressionPool = try DecompressionPool.init(alloc, runtime.cpus);
    errdefer decompressionPool.deinit(alloc);

    const meter = StoreMeter.init();

    var store: Store = .{
        .lockFile = file,
        .partitions = partitions,
        .streamCache = streamCache,
        .memBlocksCache = memBlocksCache,
        .timestampsEncoders = timestampsEncoders,
        .compressionPool = compressionPool,
        .decompressionPool = decompressionPool,
        .meter = meter,
        .runtime = runtime,
        .conf = conf,
    };

    // TODO: try making it parallel, it speed up start up time
    var it = layout.partitionsDir.iterate();
    // TODO: ban while loops via linter and set explicit loops
    while (try it.next(io)) |entry| {
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

    try self.startDiskUsageSampler(io, alloc);
    try self.startCacheEvicter(io);
}

pub fn deinit(self: *Store, io: Io, allocator: Allocator) void {
    self.stopped.stop(io);
    self.g.await(io) catch |err| switch (err) {
        error.Canceled => {},
    };

    for (self.partitions.items) |partition| {
        partition.release(io);
    }
    self.partitions.deinit(allocator);

    for (self.pathsBuf.items) |path| {
        allocator.free(path);
    }
    self.pathsBuf.deinit(allocator);

    self.streamCache.deinit();
    self.memBlocksCache.deinit();
    self.g.cancel(io);
    self.timestampsEncoders.deinit(allocator);
    self.compressionPool.deinit(allocator);
    self.decompressionPool.deinit(allocator);

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
fn startDiskUsageSampler(self: *Store, io: Io, alloc: Allocator) !void {
    if (self.stopped.isStopped()) return;

    errdefer self.g.cancel(io);
    try self.g.concurrent(io, runDiskUsageSampler, .{ self, io, alloc });
}

fn runDiskUsageSampler(self: *Store, io: Io, alloc: Allocator) void {
    const sampleIntervalNs = 20 * std.time.ns_per_s;

    while (!self.stopped.isStopped()) {
        self.observeDiskUsage(io, alloc);
        self.stopped.sleepOrStop(io, sampleIntervalNs);
    }
}

fn startCacheEvicter(self: *Store, io: Io) !void {
    if (self.stopped.isStopped()) return;

    errdefer self.g.cancel(io);
    try self.g.concurrent(io, runCacheEvicter, .{ self, io });
}

fn runCacheEvicter(self: *Store, io: Io) void {
    const cleanIntervalNs = 5 * std.time.ns_per_s;

    while (!self.stopped.isStopped()) {
        self.stopped.sleepOrStop(io, cleanIntervalNs);

        self.streamCache.clean(io);
        self.memBlocksCache.clean(io);
    }
}

fn observeDiskUsage(self: *Store, io: Io, alloc: Allocator) void {
    const usage = self.readStoreUsage(io, alloc) catch |err| switch (err) {
        error.FileNotFound => 0,
        else => {
            Logger.log(.err, "failed to read store disk usage", .{ .err = err });
            return;
        },
    };
    self.meter.diskUsage.set(usage);
}

// TODO: the implementation watches the files stats, it's not efficient,
// because the existing partitions size never change and either the tables size,
// instead we must collect size stats from the partitions directly on opening the tables
fn readStoreUsage(self: *Store, io: Io, alloc: Allocator) !u64 {
    var childPathBuf: [std.fs.max_path_bytes]u8 = undefined;

    return readDirUsage(io, alloc, self.runtime.path, &childPathBuf);
}

fn readDirUsage(io: Io, alloc: Allocator, path: []const u8, childPathBuf: []u8) !u64 {
    var dir = if (std.fs.path.isAbsolute(path))
        try std.Io.Dir.openDirAbsolute(io, path, .{ .iterate = true })
    else
        try std.Io.Dir.cwd().openDir(io, path, .{ .iterate = true });
    defer dir.close(io);

    var childPathWriter = std.Io.Writer.fixed(childPathBuf);

    var total: u64 = 0;
    var it = dir.iterate();
    while (try it.next(io)) |entry| {
        switch (entry.kind) {
            .directory => {
                try std.fs.path.fmtJoin(&.{ path, entry.name }).format(&childPathWriter);
                total += try readDirUsage(io, alloc, childPathWriter.buffered(), childPathBuf);
                childPathWriter.end = 0;
            },
            .file => {
                var file = try dir.openFile(io, entry.name, .{});
                defer file.close(io);
                total += (try file.stat(io)).size;
            },
            else => {},
        }
    }

    return total;
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
    const minDay = (nowNs - self.conf.app.storeRetention) / std.time.ns_per_day;
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
        try partition.addLines(io, allocator, list, tags, encodedTags, sid, self.memBlocksCache);

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
                .limit = nowNs - self.conf.app.storeRetention,
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

        try partition.addLines(io, allocator, it.value_ptr.*, tags, encodedTags, sid, self.memBlocksCache);
    }
}

pub fn queryLines(
    self: *Store,
    io: Io,
    alloc: Allocator,
    longAlloc: Allocator,
    tenantID: u64,
    query: Query,
) !std.ArrayList(Line) {
    // TODO: query cancelation

    self.partitionsMx.lockUncancelable(io);

    const minDay: u32 = @intCast(query.start / std.time.ns_per_day);
    const maxDay: u32 = @intCast(query.end / std.time.ns_per_day);

    var fba = std.heap.stackFallback(64, alloc);
    const fbaAlloc = fba.get();

    const slice = selectPartitionsSliceInRange(self.partitions.items, minDay, maxDay);
    // copy partitions not to deal with the lock
    var parts = std.ArrayList(*Partition).initCapacity(fbaAlloc, slice.len) catch |err| {
        self.partitionsMx.unlock(io);
        return err;
    };
    defer {
        for (parts.items) |part| part.release(io);
        parts.deinit(fbaAlloc);
    }
    for (slice) |part| {
        part.retain();
        parts.appendAssumeCapacity(part);
    }
    self.partitionsMx.unlock(io);

    var results = std.ArrayList(Line).empty;
    errdefer deinitLinesFull(alloc, &results);
    for (parts.items) |part| {
        var partResults = try part.queryLines(io, alloc, longAlloc, tenantID, query, self.memBlocksCache);
        defer partResults.deinit(alloc);

        try results.appendSlice(alloc, partResults.items);
    }

    // TODO: we need to make a real pagination, it's a plug not to overload the ui,
    // otherwise it fetches 10k lines and becomes unusable
    keepLatestLines(alloc, &results, 200);
    return results;
}

fn keepLatestLines(alloc: Allocator, lines: *std.ArrayList(Line), limit: usize) void {
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
    alloc: Allocator,
    longAlloc: Allocator,
    tenantID: u64,
    from: u64,
    to: u64,
) !std.AutoArrayHashMapUnmanaged(u128, void) {
    self.partitionsMx.lockUncancelable(io);

    const minDay: u32 = @intCast(from / std.time.ns_per_day);
    const maxDay: u32 = @intCast(to / std.time.ns_per_day);

    var fba = std.heap.stackFallback(64, alloc);
    const fbaAlloc = fba.get();

    const slice = selectPartitionsSliceInRange(self.partitions.items, minDay, maxDay);
    var parts = std.ArrayList(*Partition).initCapacity(fbaAlloc, slice.len) catch |err| {
        self.partitionsMx.unlock(io);
        return err;
    };
    defer {
        for (parts.items) |part| part.release(io);
        parts.deinit(fbaAlloc);
    }
    for (slice) |part| {
        part.retain();
        parts.appendAssumeCapacity(part);
    }
    self.partitionsMx.unlock(io);

    var streamIDs: std.AutoArrayHashMapUnmanaged(u128, void) = .empty;

    for (parts.items) |part| {
        var partStreamIDs = try part.queryStreamIDs(io, alloc, longAlloc, tenantID, self.memBlocksCache);
        defer partStreamIDs.deinit(alloc);

        for (partStreamIDs.keys()) |sid| {
            try streamIDs.put(alloc, sid, {});
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

fn selectPartitionsSliceInRange(partitions: []const *Partition, minDay: u32, maxDay: u32) []const *Partition {
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

fn getPartition(self: *Store, io: Io, alloc: Allocator, day: u32) !*Partition {
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

const testing = std.testing;

fn partitionKeyBuf(io: Io, buf: []u8, day: u64) ![]u8 {
    const nowNs = day * std.time.ns_per_day;
    const inst = try zeit.instant(io, .{ .source = .{ .unix_nano = nowNs } });
    const time = inst.time();
    return std.fmt.bufPrint(buf, "{d:0>2}{d:0>2}{d:0>4}", .{ time.day, time.month, @as(u32, @intCast(time.year)) });
}

fn dayFromKey(io: Io, key: []const u8) !u32 {
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

test "partitionKeyFormat" {
    const io = testing.io;
    const inst = try zeit.instant(io, .{ .source = .{ .time = .{
        .day = 1,
        .month = .jan,
        .year = 2026,
    } } });
    var key: [8]u8 = undefined;
    const now: u64 = @intCast(inst.timestamp);
    const day = now / std.time.ns_per_day;
    const keySlice = try partitionKeyBuf(io, &key, @intCast(day));
    try testing.expectEqualStrings("01012026", key[0..]);
    try testing.expectEqualStrings("01012026", keySlice);
}

test "dayFromKey parses partition keys and roundtrips with partitionKeyBuf" {
    const io = testing.io;

    const Case = struct {
        key: []const u8,
        day: u5,
        month: zeit.Month,
        year: i32,
    };

    const cases = [_]Case{
        .{ .key = "01012026", .day = 1, .month = .jan, .year = 2026 },
        .{ .key = "29022024", .day = 29, .month = .feb, .year = 2024 },
        .{ .key = "31121999", .day = 31, .month = .dec, .year = 1999 },
    };

    for (cases) |case| {
        const parsedDay = try dayFromKey(io, case.key);
        const inst = try zeit.instant(io, .{ .source = .{ .time = .{
            .day = case.day,
            .month = case.month,
            .year = case.year,
        } } });
        const expectedTs: u64 = @intCast(inst.timestamp);
        const expectedDay = expectedTs / std.time.ns_per_day;
        try testing.expectEqual(expectedDay, parsedDay);

        var keyBuf: [8]u8 = undefined;
        const keySlice = try partitionKeyBuf(io, &keyBuf, parsedDay);
        try testing.expectEqualStrings(case.key, keySlice);
    }
}

test "init opens existing partitions, sorts them and sets lru" {
    const alloc = testing.allocator;
    const io = testing.io;

    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    const rootPath = try tmp.dir.realPathFileAlloc(io, ".", alloc);
    defer alloc.free(rootPath);

    var storePathBuf: [std.fs.max_path_bytes]u8 = undefined;
    var storePathWriter = std.Io.Writer.fixed(&storePathBuf);
    try std.fs.path.fmtJoin(&.{ rootPath, "store" }).format(&storePathWriter);
    const storePath = storePathWriter.buffered();
    try Dir.createDirAbsolute(io, storePath, .default_dir);

    var partitionsPathBuf: [std.fs.max_path_bytes]u8 = undefined;
    var partitionsPathWriter = std.Io.Writer.fixed(&partitionsPathBuf);
    try std.fs.path.fmtJoin(&.{ storePath, filenames.partitions }).format(&partitionsPathWriter);
    const partitionsPath = partitionsPathWriter.buffered();
    try Dir.createDirAbsolute(io, partitionsPath, .default_dir);

    const keys = [_][]const u8{ "31121999", "01012026", "29022024" };
    for (keys) |key| {
        var partitionPathBuf: [std.fs.max_path_bytes]u8 = undefined;
        var partitionPathWriter = std.Io.Writer.fixed(&partitionPathBuf);
        try std.fs.path.fmtJoin(&.{ partitionsPath, key }).format(&partitionPathWriter);
        const partitionPath = partitionPathWriter.buffered();

        var indexPathBuf: [std.fs.max_path_bytes]u8 = undefined;
        var indexPathWriter = std.Io.Writer.fixed(&indexPathBuf);
        try std.fs.path.fmtJoin(&.{ partitionPath, filenames.indexTables }).format(&indexPathWriter);
        const indexPath = indexPathWriter.buffered();

        var dataPathBuf: [std.fs.max_path_bytes]u8 = undefined;
        var dataPathWriter = std.Io.Writer.fixed(&dataPathBuf);
        try std.fs.path.fmtJoin(&.{ partitionPath, filenames.dataTables }).format(&dataPathWriter);
        const dataPath = dataPathWriter.buffered();

        try Dir.createDirAbsolute(io, partitionPath, .default_dir);
        try Dir.createDirAbsolute(io, indexPath, .default_dir);
        try Dir.createDirAbsolute(io, dataPath, .default_dir);
    }

    const conf = Conf.getConf();
    const runtime = try Runtime.init(io, alloc, storePath, conf.app.maxCachePortion);
    defer runtime.deinit(alloc);

    const layout = try Layout.make(io, storePath, &partitionsPathBuf);
    var store = try Store.init(io, alloc, &conf, runtime, layout);
    defer store.deinit(io, alloc);

    try testing.expectEqual(keys.len, store.partitions.items.len);

    const day0 = try dayFromKey(io, "31121999");
    const day1 = try dayFromKey(io, "29022024");
    const day2 = try dayFromKey(io, "01012026");

    try testing.expectEqualSlices(u32, &.{ day0, day1, day2 }, &.{
        store.partitions.items[0].day,
        store.partitions.items[1].day,
        store.partitions.items[2].day,
    });

    const lru = store.lruPartition orelse return error.TestExpectedPartition;
    try testing.expectEqual(day2, lru.day);
}

test "selectPartitionsSlice selects correct range and handles gaps" {
    const Case = struct {
        minDay: u32,
        maxDay: u32,
        expectedDays: []const u64,
    };

    const check = struct {
        fn run(partitions: []const *Partition, cases: []const Case) !void {
            for (cases) |case| {
                const slice = selectPartitionsSliceInRange(partitions, case.minDay, case.maxDay);
                try testing.expectEqual(case.expectedDays.len, slice.len);
                for (case.expectedDays, 0..) |day, i| {
                    try testing.expectEqual(day, slice[i].day);
                }
            }
        }
    }.run;
    const newPartition = struct {
        fn new(day: u32) Partition {
            const p = Partition{
                .alloc = undefined,
                .day = day,
                .path = "",
                .key = "",
                .index = undefined,
                .data = undefined,
                .streamCache = undefined,
            };
            return p;
        }
    }.new;

    // Empty list: any range returns nothing
    {
        const partitions = [_]*Partition{};
        try check(&partitions, &[_]Case{
            .{ .minDay = 0, .maxDay = 0, .expectedDays = &.{} },
            .{ .minDay = 0, .maxDay = 100, .expectedDays = &.{} },
            .{ .minDay = 5, .maxDay = 5, .expectedDays = &.{} },
        });
    }

    // Single partition at day 7
    {
        var p7 = newPartition(7);
        const partitions = [_]*Partition{&p7};
        try check(&partitions, &[_]Case{
            // Exact hit
            .{ .minDay = 7, .maxDay = 7, .expectedDays = &.{7} },
            // Range enclosing the partition
            .{ .minDay = 6, .maxDay = 8, .expectedDays = &.{7} },
            // Range entirely before the partition
            .{ .minDay = 0, .maxDay = 6, .expectedDays = &.{} },
            // Range entirely after the partition
            .{ .minDay = 8, .maxDay = 100, .expectedDays = &.{} },
        });
    }

    // Three sparse partitions with gaps at days 1, 3, 5
    {
        var p1 = newPartition(1);
        var p3 = newPartition(3);
        var p5 = newPartition(5);
        const partitions = [_]*Partition{ &p1, &p3, &p5 };
        try check(&partitions, &[_]Case{
            // Middle range: only day 3 falls in [2, 4]
            .{ .minDay = 2, .maxDay = 4, .expectedDays = &.{3} },
            // Full range: all partitions returned
            .{ .minDay = 0, .maxDay = 100, .expectedDays = &.{ 1, 3, 5 } },
            // Starts before first partition
            .{ .minDay = 0, .maxDay = 3, .expectedDays = &.{ 1, 3 } },
            // Ends after last partition
            .{ .minDay = 3, .maxDay = 100, .expectedDays = &.{ 3, 5 } },
            // Exact boundary match on both ends
            .{ .minDay = 1, .maxDay = 5, .expectedDays = &.{ 1, 3, 5 } },
            // Single partition: first
            .{ .minDay = 1, .maxDay = 2, .expectedDays = &.{1} },
            // Single partition: middle (minDay == maxDay == partition day)
            .{ .minDay = 3, .maxDay = 3, .expectedDays = &.{3} },
            // Single partition: last
            .{ .minDay = 4, .maxDay = 5, .expectedDays = &.{5} },
            // minDay == maxDay at exact first/last partition day
            .{ .minDay = 1, .maxDay = 1, .expectedDays = &.{1} },
            .{ .minDay = 5, .maxDay = 5, .expectedDays = &.{5} },
            // No match: range beyond last partition
            .{ .minDay = 6, .maxDay = 100, .expectedDays = &.{} },
            // No match: range before first partition
            .{ .minDay = 0, .maxDay = 0, .expectedDays = &.{} },
            // No match: range falls entirely in gap between 1 and 3
            .{ .minDay = 2, .maxDay = 2, .expectedDays = &.{} },
            // No match: range falls entirely in gap between 3 and 5
            .{ .minDay = 4, .maxDay = 4, .expectedDays = &.{} },
        });
    }

    // Five consecutive partitions at days 10–14
    {
        var p10 = newPartition(10);
        var p11 = newPartition(11);
        var p12 = newPartition(12);
        var p13 = newPartition(13);
        var p14 = newPartition(14);
        const partitions = [_]*Partition{ &p10, &p11, &p12, &p13, &p14 };
        try check(&partitions, &[_]Case{
            // Full range
            .{ .minDay = 10, .maxDay = 14, .expectedDays = &.{ 10, 11, 12, 13, 14 } },
            // Range wider than the partition set
            .{ .minDay = 9, .maxDay = 15, .expectedDays = &.{ 10, 11, 12, 13, 14 } },
            // Interior sub-range (no boundary touch)
            .{ .minDay = 11, .maxDay = 13, .expectedDays = &.{ 11, 12, 13 } },
            // Single day in the middle
            .{ .minDay = 12, .maxDay = 12, .expectedDays = &.{12} },
            // Overlap only at the start
            .{ .minDay = 0, .maxDay = 11, .expectedDays = &.{ 10, 11 } },
            // Overlap only at the end
            .{ .minDay = 13, .maxDay = 100, .expectedDays = &.{ 13, 14 } },
            // Before all
            .{ .minDay = 0, .maxDay = 9, .expectedDays = &.{} },
            // After all
            .{ .minDay = 15, .maxDay = 100, .expectedDays = &.{} },
        });
    }
}

test "keepLatestLines sorts by timestamp and trims old rows" {
    const alloc = testing.allocator;

    const makeLine = struct {
        fn run(allocator: Allocator, timestampNs: u64, value: []const u8) !Line {
            const fields = try allocator.alloc(Field, 1);
            errdefer allocator.free(fields);
            const key = try allocator.dupe(u8, "id");
            errdefer allocator.free(key);
            const copiedValue = try allocator.dupe(u8, value);
            fields[0] = .{ .key = key, .value = copiedValue };
            return .{ .timestampNs = timestampNs, .fields = fields };
        }
    }.run;
    const appendLine = struct {
        fn run(allocator: Allocator, dst: *std.ArrayList(Line), timestampNs: u64, value: []const u8) !void {
            const line = try makeLine(allocator, timestampNs, value);
            errdefer freeFields(allocator, line.fields);
            try dst.append(allocator, line);
        }
    }.run;

    var lines = std.ArrayList(Line).empty;
    defer deinitLinesFull(alloc, &lines);

    try appendLine(alloc, &lines, 10, "oldest");
    try appendLine(alloc, &lines, 50, "newest");
    try appendLine(alloc, &lines, 20, "old");
    try appendLine(alloc, &lines, 40, "newer");
    try appendLine(alloc, &lines, 30, "middle");

    keepLatestLines(alloc, &lines, 3);

    try testing.expectEqual(3, lines.items.len);
    try testing.expectEqual(50, lines.items[0].timestampNs);
    try testing.expectEqual(40, lines.items[1].timestampNs);
    try testing.expectEqual(30, lines.items[2].timestampNs);
    try testing.expectEqualStrings("newest", lines.items[0].fields[0].value);
    try testing.expectEqualStrings("newer", lines.items[1].fields[0].value);
    try testing.expectEqualStrings("middle", lines.items[2].fields[0].value);
}

test "getPartition reuses partition, updates lru, deinit closes partitions and recorders" {
    const alloc = testing.allocator;
    const io = testing.io;

    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    const rootPath = try tmp.dir.realPathFileAlloc(io, ".", alloc);
    defer alloc.free(rootPath);

    var partitionsRootBuf: [std.fs.max_path_bytes]u8 = undefined;
    var partitionsRootWriter = std.Io.Writer.fixed(&partitionsRootBuf);
    try std.fs.path.fmtJoin(&.{ rootPath, filenames.partitions }).format(&partitionsRootWriter);
    const partitionsRoot = partitionsRootWriter.buffered();
    try Dir.createDirAbsolute(io, partitionsRoot, .default_dir);

    const conf = Conf.getConf();
    const runtime = try Runtime.init(io, alloc, rootPath, conf.app.maxCachePortion);
    defer runtime.deinit(alloc);

    var partitionsPathBuf: [std.fs.max_path_bytes]u8 = undefined;
    const layout = try Layout.make(io, rootPath, &partitionsPathBuf);
    var store = try Store.init(io, alloc, &conf, runtime, layout);
    defer store.deinit(io, alloc);

    const dayOne: u64 = 10;
    const dayTwo: u64 = 11;

    try testing.expectEqual(0, store.partitions.items.len);

    const first = try store.getPartition(io, alloc, dayOne);
    defer first.release(io);
    try testing.expectEqual(1, store.partitions.items.len);
    try testing.expectEqual(first, store.partitions.items[0]);
    try testing.expectEqual(first, store.lruPartition.?);

    const firstAgain = try store.getPartition(io, alloc, dayOne);
    defer firstAgain.release(io);
    try testing.expectEqual(first, firstAgain);
    try testing.expectEqual(1, store.partitions.items.len);
    try testing.expectEqual(first, store.lruPartition.?);

    const second = try store.getPartition(io, alloc, dayTwo);
    defer second.release(io);
    try testing.expectEqual(2, store.partitions.items.len);
    try testing.expectEqual(second, store.partitions.items[1]);
    try testing.expectEqual(second, store.lruPartition.?);
}
