const std = @import("std");
const Allocator = std.mem.Allocator;

const Filenames = @import("Filenames.zig");
const DataRecorder = @import("DataRecorder.zig");
const Index = @import("store/index/Index.zig");
const IndexRecorder = @import("store/index/IndexRecorder.zig");
const Line = @import("store/lines.zig").Line;
const SID = @import("store/lines.zig").SID;
const Field = @import("store/lines.zig").Field;
const Cache = @import("stds/Cache.zig");

const Conf = @import("Conf.zig");

const Encoder = @import("encoding").Encoder;

fn streamIndexLess(lines: std.ArrayList(Line), i: u32, j: u32) bool {
    return lines.items[i].sid.lessThan(&lines.items[j].sid);
}

const partitionNameSize = 8;

pub const Partition = struct {
    day: u64,
    path: []const u8,
    name: []const u8,
    index: *Index,
    data: *DataRecorder,

    streamCache: *Cache.StreamCache,

    // TODO: meter how much it takes usually
    const bufSize = 1024;
    pub fn addLines(
        self: *Partition,
        allocator: Allocator,
        lines: std.ArrayList(Line),
        tags: []Field,
        encodedTags: []const u8,
    ) !void {
        var fallbackFba = std.heap.stackFallback(bufSize, allocator);
        const fba = fallbackFba.get();
        var streamsToCache = try std.ArrayList(u32).initCapacity(fba, bufSize / @sizeOf(u32));
        defer streamsToCache.deinit(fba);

        // detect not cached stream ids
        for (0..lines.items.len) |i| {
            const line = lines.items[i];
            if (self.isCached(line.sid)) {
                continue;
            }

            if (streamsToCache.items.len == 0) {
                try streamsToCache.append(fba, @intCast(i));
                continue;
            }

            const lineToCacheIdx = streamsToCache.items[streamsToCache.items.len - 1];
            const lineToCache = lines.items[lineToCacheIdx];
            if (!line.sid.eql(&lineToCache.sid)) {
                try streamsToCache.append(fba, @intCast(i));
            }
        }

        if (streamsToCache.items.len > 0) {
            // sort the stream ids,
            // it's necessary in case the incoming lines are mixed like [1, 3, 2],
            // so to make it [1, 2, 3]
            std.mem.sortUnstable(u32, streamsToCache.items, lines, streamIndexLess);
        }

        for (streamsToCache.items, 0..) |i, pos| {
            const sid = lines.items[i].sid;

            if (pos > 0 and lines.items[streamsToCache.items[pos - 1]].sid.eql(&sid)) continue;

            if (self.isCached(lines.items[i].sid)) continue;

            if (!try self.index.hasStream(allocator, sid)) {
                try self.index.indexStream(allocator, sid, tags, encodedTags);
            }
            try self.cache(sid);
        }

        try self.data.addLines(allocator, lines.items, 0);
        // FIXME: size calculation is not implemented
        unreachable;
    }

    fn isCached(self: *Partition, sid: SID) bool {
        var cacheKey: [SID.encodeBound + partitionNameSize]u8 = undefined;
        var enc = Encoder.init(&cacheKey);
        sid.encode(&enc);
        @memcpy(cacheKey[SID.encodeBound..], self.name);

        return self.streamCache.contains(cacheKey[0..]);
    }

    fn cache(self: *Partition, sid: SID) !void {
        var cacheKey: [SID.encodeBound + partitionNameSize]u8 = undefined;
        var enc = Encoder.init(&cacheKey);
        sid.encode(&enc);
        @memcpy(cacheKey[SID.encodeBound..], self.name);

        try self.streamCache.set(&cacheKey, {});
    }
};

pub const Store = struct {
    path: []const u8,

    partitions: std.ArrayList(*Partition),
    hot: ?*Partition,

    pub fn init(allocator: Allocator, path: []const u8) !*Store {
        const store = try allocator.create(Store);
        store.* = .{
            .path = path,
            .partitions = std.ArrayList(*Partition).empty,
            .hot = null,
        };
        // truncate separator
        if (path[store.path.len - 1] == std.fs.path.sep_str[0]) {
            store.path = path[0 .. path.len - 1];
        }
        return store;
    }

    pub fn deinit(self: *Store, allocator: Allocator) void {
        self.partitions.deinit(allocator);
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
            const partition = try self.getPartition(allocator, it.key_ptr.*);
            try partition.addLines(allocator, it.value_ptr.*, tags, encodedTags);
        }
    }

    fn getPartition(self: *Store, allocator: Allocator, day: u64) !*Partition {
        const n = std.sort.binarySearch(
            *Partition,
            self.partitions.items,
            day,
            orderPartitions,
        );
        if (n) |i| {
            const part = self.partitions.items[i];
            self.hot = part;
            return part;
        }

        var path_buf: [std.fs.max_path_bytes]u8 = undefined;
        const partitionPath = try std.fmt.bufPrint(
            &path_buf,
            "{s}{s}{s}{s}{d}",
            .{ self.path, std.fs.path.sep_str, Filenames.partitions, std.fs.path.sep_str, day },
        );

        const res = std.fs.accessAbsolute(partitionPath, .{ .mode = .read_write });
        if (res) |_| {
            // TODO: get a partition from existing folder or create missing files
            // missing files could be due to crash
            return error.PartitionUnavailble;
        } else |err| switch (err) {
            error.FileNotFound => {},
            else => return err,
        }

        // TODO: consider using fixed buffer allocator for dataFolderPath and indexFolderPath
        const dataFolderPath = try std.mem.concat(allocator, u8, &.{ partitionPath, Filenames.tableData });
        defer allocator.free(dataFolderPath);
        const indexFolderPath = try std.mem.concat(allocator, u8, &.{ partitionPath, Filenames.tableIndex });
        defer allocator.free(indexFolderPath);

        try createParitionFiles(allocator, dataFolderPath, indexFolderPath);
        const partition = try self.openPartition(allocator, partitionPath, day);

        return partition;
    }

    fn openPartition(self: *Store, allocator: Allocator, path: []const u8, day: u64) !*Partition {
        // FIXME: understand whether the path for index and data is the same,
        // if it is - make sure deinit doesn't remove disk space from the cache, but it does it based on the partition path or something
        const conf = Conf.getConf();
        const indexTable = try IndexRecorder.init(allocator, "", conf.server.pools.cpus);
        const index = try Index.init(allocator, indexTable);

        // TODO: replace abc to path from config file
        const data = try DataRecorder.init(allocator, "abc"[0..]);
        // TODO: remove unused parts directories

        const cache = try Cache.StreamCache.init(allocator);

        const partitionName = std.fs.path.basename(path);
        std.debug.assert(partitionName.len == partitionNameSize);

        const partition = try allocator.create(Partition);
        partition.* = Partition{
            .day = day,
            .path = path,
            .name = partitionName,
            .index = index,
            .data = data,
            .streamCache = cache,
        };
        self.hot = partition;
        try self.partitions.append(allocator, partition);

        return partition;
    }
};

fn createParitionFiles(allocator: Allocator, indexFolderPath: []const u8, dataFolderPath: []const u8) !void {
    try std.fs.makeDirAbsolute(indexFolderPath);
    try std.fs.makeDirAbsolute(dataFolderPath);

    // TODO: consider using static buffer allocator
    const partsFilePath = try std.mem.concat(allocator, u8, &.{ dataFolderPath, Filenames.tables });
    defer allocator.free(partsFilePath);
    const file = try std.fs.createFileAbsolute(partsFilePath, .{ .exclusive = true });
    file.close();
}

fn orderPartitions(day: u64, part: *Partition) std.math.Order {
    if (day < part.day) {
        return .lt;
    }
    if (day > part.day) {
        return .gt;
    }
    return .eq;
}
