const std = @import("std");
const Allocator = std.mem.Allocator;

const zeit = @import("zeit");

const fs = @import("fs.zig");

const Cache = @import("stds/Cache.zig");
const Line = @import("store/lines.zig").Line;
const Field = @import("store/lines.zig").Field;
const Query = @import("store/query.zig").Query;

const Partition = @import("Partition.zig");
const filenames = @import("filenames.zig");
const Conf = @import("Conf.zig");
const Runtime = @import("Runtime.zig");

pub const Store = @This();

path: []const u8,
/// lockFile is used to ensure only one instance is running
/// in order to prevent data corruption
lockFile: std.fs.File,

partitionsMx: std.Thread.Mutex = .{},
partitions: std.ArrayList(*Partition) = .empty,
lruPartition: ?*Partition = null,

/// streamCache is a stream id cache for ingestion,
/// shared across all partitions, injected from a store to them
streamCache: *Cache.StreamCache,

/// pathsBuf holds a garbage of created paths for partitions and it's tables
pathsBuf: std.ArrayList([]const u8) = .empty,

// runtime stats
runtime: *Runtime,

// TODO: store must observe current disk space for 2 reasons:
// 1. expose it as a metric in order to alert to OPS to expand disk or remove the stale data
// 2. handle eviction policy based on the config (e.g. free GB or free % of the disk)
// TODO: start partitions retention watcher
pub fn init(alloc: Allocator, path: []const u8) !Store {
    std.debug.assert(std.fs.path.isAbsolute(path));
    std.debug.assert(path[path.len - 1] != std.fs.path.sep);

    const runtime = try Runtime.init(alloc, path, Conf.getConf().app.maxCachePortion);
    errdefer runtime.deinit(alloc);

    var buf: [std.fs.max_path_bytes]u8 = undefined;
    const partitionsPath = try std.fmt.bufPrint(
        &buf,
        "{s}{c}{s}",
        .{ path, std.fs.path.sep, filenames.partitions },
    );
    const partitionsDir = try createStoreDirIfNotExists(path, partitionsPath);

    const file = try createLockFile(path);
    errdefer file.close();

    var streamCache = try Cache.StreamCache.init(alloc);
    errdefer streamCache.deinit();

    // 30 is a default retention
    var partitions = try std.ArrayList(*Partition).initCapacity(alloc, 30);
    errdefer {
        for (partitions.items) |partition| {
            partition.close();
        }
        partitions.deinit(alloc);
    }

    var store: Store = .{
        .path = path,
        .lockFile = file,
        .partitions = partitions,
        .streamCache = streamCache,
        .runtime = runtime,
    };

    // TODO: try making it parallel, it speed up start up time
    var it = partitionsDir.iterate();
    while (try it.next()) |entry| {
        const partitionPath = try std.fs.path.join(alloc, &.{ partitionsPath, entry.name });
        errdefer alloc.free(partitionPath);

        const day = try dayFromKey(entry.name);
        const indexPath = try std.fs.path.join(alloc, &.{ partitionPath, filenames.indexTables });
        errdefer alloc.free(indexPath);
        const dataPath = try std.fs.path.join(alloc, &.{ partitionPath, filenames.dataTables });
        errdefer alloc.free(dataPath);

        // discard partition because it appends it to the store state
        _ = try store.openPartition(alloc, partitionPath, indexPath, dataPath, day);
    }

    std.mem.sortUnstable(*Partition, partitions.items, {}, Partition.lessThan);

    store.lruPartition = if (store.partitions.items.len > 0)
        store.partitions.items[store.partitions.items.len - 1]
    else
        null;
    return store;
}

pub fn deinit(self: *Store, allocator: Allocator) void {
    self.runtime.deinit(allocator);
    for (self.partitions.items) |partition| {
        partition.release();
    }
    self.partitions.deinit(allocator);

    for (self.pathsBuf.items) |path| {
        allocator.free(path);
    }
    self.pathsBuf.deinit(allocator);
    self.streamCache.deinit();

    self.lockFile.close();
}

pub fn createStoreDirIfNotExists(path: []const u8, partitionsPath: []const u8) !std.fs.Dir {
    std.fs.accessAbsolute(path, .{}) catch |err| {
        switch (err) {
            error.FileNotFound => {
                createDir(path, partitionsPath);
                return std.fs.openDirAbsolute(partitionsPath, .{ .iterate = true });
            },
            else => return err,
        }
    };
    std.fs.accessAbsolute(partitionsPath, .{}) catch |err| {
        switch (err) {
            error.FileNotFound => {
                fs.makeDirAssert(partitionsPath);
                fs.syncPathAndParentDir(path);
            },
            else => return err,
        }
    };

    return std.fs.openDirAbsolute(partitionsPath, .{ .iterate = true });
}

pub fn createDir(path: []const u8, partitionsPath: []const u8) void {
    fs.makeDirAssert(path);
    fs.makeDirAssert(partitionsPath);

    fs.syncPathAndParentDir(path);
}

// TODO: make it configurable
const retentionNs: u64 = 30 * std.time.ns_per_day;
pub fn addLines(
    self: *Store,
    allocator: Allocator,
    lines: []Line,
    tags: []Field,
    encodedTags: []const u8,
) !void {
    if (lines.len == 0) return;
    // TODO: make partition interval configurable
    // in order to being able to test shorter partitions: 1, 2, 3, 6, 12 hours
    const nowNs: u64 = @intCast(std.time.nanoTimestamp());
    const minDay = (nowNs - retentionNs) / std.time.ns_per_day;
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
            self.partitionsMx.lock();
            defer self.partitionsMx.unlock();

            break :blk try self.getPartitionOrLru(allocator, firstDay);
        };
        defer partition.release();

        var list = std.ArrayList(Line).initBuffer(lines[0..idx]);
        list.items.len = idx;
        try partition.addLines(allocator, list, tags, encodedTags);

        // Return early since all lines are added to the same Partition
        if (list.items.len == lines.len) return;
    }

    // If the Lines belong to different Partitions, continue where left off,
    // sort them by day, then bulk add
    var linesByInterval = std.AutoHashMap(u32, std.ArrayList(Line)).init(allocator);

    while (idx < lines.len) : (idx += 1) {
        const day: u32 = @intCast(lines[idx].timestampNs / std.time.ns_per_day);
        if (day < minDay) {
            std.debug.print(
                "[WARN] incoming log is out of the retentation range: lower range={d}, given={d}\n",
                .{ nowNs - retentionNs, lines[idx].timestampNs },
            );
            continue;
        }
        if (day > maxDay) {
            std.debug.print(
                "[WARN] incoming log is out of the retentation range: upper range={d}, given={d}\n",
                .{ nowNs + std.time.ns_per_day, lines[idx].timestampNs },
            );
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
            self.partitionsMx.lock();
            defer self.partitionsMx.unlock();

            break :blk try self.getPartitionOrLru(allocator, day);
        };
        defer partition.release();

        try partition.addLines(allocator, it.value_ptr.*, tags, encodedTags);
    }
}

pub fn queryLines(self: *Store, alloc: Allocator, tenantID: []const u8, query: Query) !std.ArrayList(Line) {
    // TODO: query cancelation

    self.partitionsMx.lock();

    const minDay: u32 = @intCast(query.start / std.time.ns_per_day);
    const maxDay: u32 = @intCast(query.end / std.time.ns_per_day);

    var fba = std.heap.stackFallback(64, alloc);
    const fbaAlloc = fba.get();

    const slice = selectPartitionsSliceInRange(self.partitions.items, minDay, maxDay);
    // copy partitions not to deal with the lock
    var parts = std.ArrayList(*Partition).initCapacity(fbaAlloc, slice.len) catch |err| {
        self.partitionsMx.unlock();
        return err;
    };
    defer {
        for (parts.items) |part| part.release();
        parts.deinit(fbaAlloc);
    }
    for (slice) |part| {
        part.retain();
        parts.appendAssumeCapacity(part);
    }
    self.partitionsMx.unlock();

    var results = std.ArrayList(Line).empty;
    for (parts.items) |part| {
        var partResults = try part.queryLines(fbaAlloc, tenantID, query);
        defer partResults.deinit(fbaAlloc);

        try results.appendSlice(alloc, partResults.items);
    }

    return results;
}

pub fn flush(self: *Store, alloc: Allocator) !void {
    var parts = try std.ArrayList(*Partition).initCapacity(alloc, self.partitions.items.len);
    defer {
        for (parts.items) |part| part.release();
        parts.deinit(alloc);
    }

    self.partitionsMx.lock();
    for (self.partitions.items) |part| {
        parts.appendAssumeCapacity(part);
        part.retain();
    }
    self.partitionsMx.unlock();

    for (self.partitions.items) |part| {
        try part.flushForce(alloc);
    }
}

fn selectPartitionsSliceInRange(partitions: []const *Partition, minDay: u32, maxDay: u32) []const *Partition {
    // Find first partition with day >= minDay
    const start = std.sort.lowerBound(
        *Partition,
        partitions,
        minDay,
        orderPartitions,
    );

    // Find first partition with day > maxDay
    const slice = partitions[start..];
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

fn getPartition(self: *Store, alloc: Allocator, day: u32) !*Partition {
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
    const partitionKeySlice = try partitionKeyBuf(&partitionKey, day);
    std.debug.assert(std.mem.eql(u8, partitionKeySlice, partitionKey[0..]));

    const partitionPath = try std.fs.path.join(alloc, &.{ self.path, filenames.partitions, partitionKeySlice });
    errdefer alloc.free(partitionPath);

    // TODO: don't allocate those paths, make it as computed properties,
    // then we can:
    // - remove pathsBuf
    // - make disk space cache rely on the store path
    const indexPath = try std.fs.path.join(alloc, &.{ partitionPath, filenames.indexTables });
    errdefer alloc.free(indexPath);
    const dataPath = try std.fs.path.join(alloc, &.{ partitionPath, filenames.dataTables });
    errdefer alloc.free(dataPath);

    std.fs.accessAbsolute(partitionPath, .{ .mode = .read_write }) catch |err| {
        switch (err) {
            error.FileNotFound => {
                Partition.createDir(partitionPath, indexPath, dataPath);
            },
            else => return err,
        }
    };

    const part = try self.openPartition(alloc, partitionPath, indexPath, dataPath, day);
    part.retain();

    self.lruPartition = part;
    return part;
}

fn getPartitionOrLru(self: *Store, alloc: Allocator, day: u32) !*Partition {
    if (self.getLruPartition()) |part|
        if (part.day == day)
            return part;

    return self.getPartition(alloc, day);
}

// TODO: when we get rid of pathsBuf we can path only partitions ref,
// therefore make store init returning the store in the end
fn openPartition(
    self: *Store,
    alloc: Allocator,
    path: []const u8,
    indexPath: []const u8,
    dataPath: []const u8,
    day: u32,
) !*Partition {
    try self.partitions.ensureUnusedCapacity(alloc, 1);
    try self.pathsBuf.ensureUnusedCapacity(alloc, 3);

    const partition = try Partition.open(alloc, path, indexPath, dataPath, day, self.streamCache, self.runtime);

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

fn partitionKeyBuf(buf: []u8, day: u64) ![]u8 {
    const nowNs = day * std.time.ns_per_day;
    const inst = try zeit.instant(.{ .source = .{ .unix_nano = nowNs } });
    const time = inst.time();
    return std.fmt.bufPrint(buf, "{d:0>2}{d:0>2}{d:0>4}", .{ time.day, time.month, @as(u32, @intCast(time.year)) });
}

fn dayFromKey(key: []const u8) !u32 {
    std.debug.assert(key.len == 8);

    const day = try std.fmt.parseInt(u64, key[0..2], 10);
    const month = try std.fmt.parseInt(u64, key[2..4], 10);
    const year = try std.fmt.parseInt(u64, key[4..8], 10);

    const monthEnum: zeit.Month = @enumFromInt(month);
    const inst = try zeit.instant(.{ .source = .{ .time = .{
        .day = @intCast(day),
        .month = monthEnum,
        .year = @intCast(year),
    } } });

    const ts: u64 = @intCast(inst.timestamp);
    return @intCast(ts / std.time.ns_per_day);
}

fn createLockFile(path: []const u8) !std.fs.File {
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
    var file = try std.fs.createFileAbsolute(lockFilePath, .{ .lock = .exclusive });
    errdefer file.close();

    // var i: u8 = 0;
    // while (i < 5) : (i += 1) {
    //     std.posix.flock(file.handle, std.posix.LOCK.EX | std.posix.LOCK.NB) catch |err| {
    //         switch (err) {
    //             // repeat once again
    //             error.WouldBlock => std.Thread.sleep(std.time.ns_per_ms * 200),
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
    const inst = try zeit.instant(.{ .source = .{ .time = .{
        .day = 1,
        .month = .jan,
        .year = 2026,
    } } });
    var key: [8]u8 = undefined;
    const now: u64 = @intCast(inst.timestamp);
    const day = now / std.time.ns_per_day;
    const keySlice = try partitionKeyBuf(&key, @intCast(day));
    try testing.expectEqualStrings("01012026", key[0..]);
    try testing.expectEqualStrings("01012026", keySlice);
}

test "dayFromKey parses partition keys and roundtrips with partitionKeyBuf" {
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
        const parsedDay = try dayFromKey(case.key);
        const inst = try zeit.instant(.{ .source = .{ .time = .{
            .day = case.day,
            .month = case.month,
            .year = case.year,
        } } });
        const expectedTs: u64 = @intCast(inst.timestamp);
        const expectedDay = expectedTs / std.time.ns_per_day;
        try testing.expectEqual(expectedDay, parsedDay);

        var keyBuf: [8]u8 = undefined;
        const keySlice = try partitionKeyBuf(&keyBuf, parsedDay);
        try testing.expectEqualStrings(case.key, keySlice);
    }
}

test "createStoreDirIfNotExists ensures store and partitions dirs exist" {
    const alloc = testing.allocator;

    const Case = struct {
        createStoreDir: bool,
        createPartitionsDir: bool,
    };

    const cases = [_]Case{
        .{ .createStoreDir = false, .createPartitionsDir = false },
        .{ .createStoreDir = true, .createPartitionsDir = false },
        .{ .createStoreDir = true, .createPartitionsDir = true },
    };

    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    const rootPath = try tmp.dir.realpathAlloc(alloc, ".");
    defer alloc.free(rootPath);

    for (cases, 0..) |case, i| {
        var storeNameBuf: [32]u8 = undefined;
        const storeName = try std.fmt.bufPrint(&storeNameBuf, "store-{d}", .{i});

        const storePath = try std.fs.path.join(alloc, &.{ rootPath, storeName });
        defer alloc.free(storePath);
        const partitionsPath = try std.fs.path.join(alloc, &.{ storePath, filenames.partitions });
        defer alloc.free(partitionsPath);

        if (case.createStoreDir) {
            try std.fs.makeDirAbsolute(storePath);
        }
        if (case.createPartitionsDir) {
            try std.fs.makeDirAbsolute(partitionsPath);
        }

        _ = try Store.createStoreDirIfNotExists(storePath, partitionsPath);

        try testing.expect(try fs.pathExists(storePath));
        try testing.expect(try fs.pathExists(partitionsPath));
    }
}

test "init opens existing partitions, sorts them and sets lru" {
    const alloc = testing.allocator;

    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    const rootPath = try tmp.dir.realpathAlloc(alloc, ".");
    defer alloc.free(rootPath);

    const storePath = try std.fs.path.join(alloc, &.{ rootPath, "store" });
    defer alloc.free(storePath);
    try std.fs.makeDirAbsolute(storePath);

    const partitionsPath = try std.fs.path.join(alloc, &.{ storePath, filenames.partitions });
    defer alloc.free(partitionsPath);
    try std.fs.makeDirAbsolute(partitionsPath);

    const keys = [_][]const u8{ "31121999", "01012026", "29022024" };
    for (keys) |key| {
        const partitionPath = try std.fs.path.join(alloc, &.{ partitionsPath, key });
        defer alloc.free(partitionPath);
        const indexPath = try std.fs.path.join(alloc, &.{ partitionPath, filenames.indexTables });
        defer alloc.free(indexPath);
        const dataPath = try std.fs.path.join(alloc, &.{ partitionPath, filenames.dataTables });
        defer alloc.free(dataPath);

        try std.fs.makeDirAbsolute(partitionPath);
        try std.fs.makeDirAbsolute(indexPath);
        try std.fs.makeDirAbsolute(dataPath);
    }

    var store = try Store.init(alloc, storePath);
    defer store.deinit(alloc);

    try testing.expectEqual(keys.len, store.partitions.items.len);

    const day0 = try dayFromKey("31121999");
    const day1 = try dayFromKey("29022024");
    const day2 = try dayFromKey("01012026");

    try testing.expectEqual(day0, store.partitions.items[0].day);
    try testing.expectEqual(day1, store.partitions.items[1].day);
    try testing.expectEqual(day2, store.partitions.items[2].day);

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

test "getPartition reuses partition, updates lru, deinit closes partitions and recorders" {
    const alloc = testing.allocator;

    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    const rootPath = try tmp.dir.realpathAlloc(alloc, ".");
    defer alloc.free(rootPath);

    const partitionsRoot = try std.fs.path.join(alloc, &.{ rootPath, filenames.partitions });
    defer alloc.free(partitionsRoot);
    try std.fs.makeDirAbsolute(partitionsRoot);

    var store = try Store.init(alloc, rootPath);
    defer store.deinit(alloc);

    const dayOne: u64 = 10;
    const dayTwo: u64 = 11;

    try testing.expectEqual(0, store.partitions.items.len);

    const first = try store.getPartition(alloc, dayOne);
    defer first.release();
    try testing.expectEqual(1, store.partitions.items.len);
    try testing.expectEqual(first, store.partitions.items[0]);
    try testing.expectEqual(first, store.lruPartition.?);

    const firstAgain = try store.getPartition(alloc, dayOne);
    defer firstAgain.release();
    try testing.expectEqual(first, firstAgain);
    try testing.expectEqual(1, store.partitions.items.len);
    try testing.expectEqual(first, store.lruPartition.?);

    const second = try store.getPartition(alloc, dayTwo);
    defer second.release();
    try testing.expectEqual(2, store.partitions.items.len);
    try testing.expectEqual(second, store.partitions.items[1]);
    try testing.expectEqual(second, store.lruPartition.?);
}
