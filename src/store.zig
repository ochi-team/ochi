const std = @import("std");
const Allocator = std.mem.Allocator;

const zeit = @import("zeit");

const Line = @import("store/lines.zig").Line;
const Field = @import("store/lines.zig").Field;

const Partition = @import("Partition.zig");
const Filenames = @import("Filenames.zig");
const Conf = @import("Conf.zig");

pub const Store = struct {
    path: []const u8,

    partitionsMx: std.Thread.Mutex = .{},
    partitions: std.ArrayList(*Partition) = .empty,
    lruPartition: ?*Partition = null,

    /// pathsBuf holds a garbage of created paths for partitions and it's tables
    pathsBuf: std.ArrayList([]const u8) = .empty,

    pub fn init(alloc: Allocator, path: []const u8) !*Store {
        const store = try alloc.create(Store);
        store.* = .{
            .path = path,
        };
        // truncate separator
        // TODO: do it outside
        if (path[store.path.len - 1] == std.fs.path.sep_str[0]) {
            store.path = path[0 .. path.len - 1];
        }
        return store;
    }

    pub fn deinit(self: *Store, allocator: Allocator) void {
        for (self.partitions.items) |partition| {
            partition.close(allocator);
        }
        self.partitions.deinit(allocator);

        for (self.pathsBuf.items) |path| {
            allocator.free(path);
        }
        self.pathsBuf.deinit(allocator);
        allocator.destroy(self);
    }

    pub fn addLines(
        self: *Store,
        allocator: Allocator,
        lines: std.AutoHashMap(u64, std.ArrayList(Line)),
        tags: []Field,
        encodedTags: []const u8,
    ) !void {
        var linesIterator = lines.iterator();
        while (linesIterator.next()) |it| {
            const day = it.key_ptr.*;

            // TODO: rework how we approach lru partition
            // 1. pass all the lines directly to the store,
            // iterate over saving the window width, if it fits - pass them all to the lru
            // 2. if it doesn't fit - make a map by intervals, retain it
            if (self.lruPartition) |part| if (part.day == day) {
                try part.addLines(allocator, it.value_ptr.*, tags, encodedTags);
                continue;
            };

            const partition = try self.getPartition(allocator, day);
            try partition.addLines(allocator, it.value_ptr.*, tags, encodedTags);
        }
    }

    fn getPartition(self: *Store, alloc: Allocator, day: u64) !*Partition {
        self.partitionsMx.lock();
        defer self.partitionsMx.unlock();

        const n = std.sort.binarySearch(
            *Partition,
            self.partitions.items,
            day,
            orderPartitions,
        );
        if (n) |i| {
            const part = self.partitions.items[i];
            self.lruPartition = part;
            return part;
        }

        // TODO: what if a partition is deleted, we might want to return null,
        // handle it outside, log a warning showing the partition is missing
        // due to being deprecated (identify whether it's out of the retention period)

        var partitionKey: [8]u8 = undefined;
        const partitionKeySlice = try partitionKeyBuf(&partitionKey, day);
        std.debug.assert(std.mem.eql(u8, partitionKeySlice, partitionKey[0..]));

        const partitionPath = try std.fs.path.join(alloc, &.{ self.path, Filenames.partitions, partitionKeySlice });
        errdefer alloc.free(partitionPath);

        // TODO: don't allocate those paths, make it as computed properties,
        // then we can:
        // - remove pathsBuf
        // - make disk space cache rely on the store path
        const indexPath = try std.fs.path.join(alloc, &.{ partitionPath, Filenames.indexTables });
        errdefer alloc.free(indexPath);
        const dataPath = try std.fs.path.join(alloc, &.{ partitionPath, Filenames.dataTables });
        errdefer alloc.free(dataPath);

        std.fs.accessAbsolute(partitionPath, .{ .mode = .read_write }) catch |err| {
            switch (err) {
                error.FileNotFound => {
                    Partition.createDir(partitionPath, indexPath, dataPath);
                },
                else => return err,
            }
        };

        const partition = try self.openPartition(alloc, partitionPath, indexPath, dataPath, day);
        return partition;
    }

    fn openPartition(
        self: *Store,
        alloc: Allocator,
        path: []const u8,
        indexPath: []const u8,
        dataPath: []const u8,
        day: u64,
    ) !*Partition {
        try self.partitions.ensureUnusedCapacity(alloc, 1);
        try self.pathsBuf.ensureUnusedCapacity(alloc, 3);

        const partition = try Partition.open(alloc, path, indexPath, dataPath, day);

        self.pathsBuf.appendAssumeCapacity(path);
        self.pathsBuf.appendAssumeCapacity(indexPath);
        self.pathsBuf.appendAssumeCapacity(dataPath);
        self.lruPartition = partition;
        self.partitions.appendAssumeCapacity(partition);

        return partition;
    }
};

fn orderPartitions(day: u64, part: *Partition) std.math.Order {
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

test "getPartition reuses partition, updates lru, deinit closes partitions and recorders" {
    const alloc = testing.allocator;
    _ = try Conf.default(alloc);
    defer Conf.deinit();

    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    const rootPath = try tmp.dir.realpathAlloc(alloc, ".");
    defer alloc.free(rootPath);

    const partitionsRoot = try std.fs.path.join(alloc, &.{ rootPath, Filenames.partitions });
    defer alloc.free(partitionsRoot);
    try std.fs.makeDirAbsolute(partitionsRoot);

    const store = try Store.init(alloc, rootPath);

    const dayOne: u64 = 10;
    const dayTwo: u64 = 11;

    try testing.expectEqual(0, store.partitions.items.len);

    const first = try store.getPartition(alloc, dayOne);
    try testing.expectEqual(1, store.partitions.items.len);
    try testing.expectEqual(first, store.partitions.items[0]);
    try testing.expectEqual(first, store.lruPartition.?);

    const firstAgain = try store.getPartition(alloc, dayOne);
    try testing.expectEqual(first, firstAgain);
    try testing.expectEqual(1, store.partitions.items.len);
    try testing.expectEqual(first, store.lruPartition.?);

    const second = try store.getPartition(alloc, dayTwo);
    try testing.expectEqual(2, store.partitions.items.len);
    try testing.expectEqual(second, store.partitions.items[1]);
    try testing.expectEqual(second, store.lruPartition.?);

    store.deinit(alloc);
}
