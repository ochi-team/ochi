const std = @import("std");

const Data = @import("data.zig").Data;
const Index = @import("store/index/Index.zig");
const IndexRecorder = @import("store/index/IndexRecorder.zig");
const Line = @import("store/lines.zig").Line;
const SID = @import("store/lines.zig").SID;
const Field = @import("store/lines.zig").Field;
const Cache = @import("stds/Cache.zig");

const Encoder = @import("encoding").Encoder;

const partitionsFolderName = "partitions";
const dataFolderName = "data";
const indexFolderName = "index";

// holds parts names separated by \n
const partsFileName = "parts";

fn streamIndexLess(lines: std.ArrayList(*const Line), i: usize, j: usize) bool {
    return lines.items[i].sid.lessThan(&lines.items[j].sid);
}

pub const Partition = struct {
    day: u64,
    path: []const u8,
    index: *Index,
    data: *Data,

    streamCache: *Cache.StreamCache,

    const bufSize = 128;
    pub fn addLines(
        self: *Partition,
        allocator: std.mem.Allocator,
        lines: std.ArrayList(*const Line),
        tags: []Field,
        encodedTags: []const u8,
    ) !void {
        var fallbackFba = std.heap.stackFallback(bufSize, allocator);
        const fba = fallbackFba.get();
        var streamsToCache = try std.ArrayList(usize).initCapacity(fba, bufSize / @sizeOf(usize));
        defer streamsToCache.deinit(fba);

        // detect not cached stream ids
        for (0..lines.items.len) |i| {
            const line = lines.items[i];
            if (self.isCached(line)) {
                continue;
            }

            if (streamsToCache.items.len == 0 or line.sid.eql(&lines.items[streamsToCache.items.len - 1].sid)) {
                try streamsToCache.append(fba, i);
            }
        }

        if (streamsToCache.items.len > 0) {
            // sort the stream ids,
            // it's necessary in case the incoming lines are mixed like [1, 2, 1, 2],
            // so to make it [1, 1, 2, 2]
            std.mem.sortUnstable(usize, streamsToCache.items, lines, streamIndexLess);

            for (streamsToCache.items) |i| {
                const sid = lines.items[i].sid;

                if (i > 0 and lines.items[streamsToCache.items[i - 1]].sid.eql(&sid)) continue;

                if (!self.index.hasStream(sid)) {
                    try self.index.indexStream(allocator, sid, tags, encodedTags);
                }
                try self.cache(sid);
            }
        }

        self.data.addLines(allocator, lines);
    }

    fn isCached(self: *Partition, line: *const Line) bool {
        // TODO: consider using u256 keys (u128 tenant id and u128 sid)
        var buf: [SID.encodeBound]u8 = undefined;
        var enc = Encoder.init(&buf);
        line.sid.encode(&enc);

        return self.streamCache.contains(buf[0..]);
    }

    fn cache(self: *Partition, sid: SID) !void {
        var buf: [SID.encodeBound]u8 = undefined;
        var enc = Encoder.init(&buf);
        sid.encode(&enc);

        try self.streamCache.set(&buf, {});
    }
};

pub const Store = struct {
    path: []const u8,

    partitions: std.ArrayList(*Partition),
    hot: ?*Partition,

    backgroundAllocator: std.mem.Allocator,

    pub fn init(allocator: std.mem.Allocator, backgroundAllocator: std.mem.Allocator, path: []const u8) !*Store {
        const store = try allocator.create(Store);
        store.* = .{
            .path = path,
            .partitions = std.ArrayList(*Partition).empty,
            .hot = null,
            .backgroundAllocator = backgroundAllocator,
        };
        // truncate separator
        if (path[store.path.len - 1] == std.fs.path.sep_str[0]) {
            store.path = path[0 .. path.len - 1];
        }
        return store;
    }

    pub fn deinit(self: *Store, allocator: std.mem.Allocator) void {
        self.partitions.deinit(allocator);
        allocator.destroy(self);
    }

    pub fn addLines(
        self: *Store,
        allocator: std.mem.Allocator,
        lines: std.AutoHashMap(u64, std.ArrayList(*const Line)),
        tags: []Field,
        encodedTags: []const u8,
    ) !void {
        var linesIterator = lines.iterator();
        while (linesIterator.next()) |it| {
            const partition = try self.getPartition(allocator, it.key_ptr.*);
            try partition.addLines(allocator, it.value_ptr.*, tags, encodedTags);
        }
    }

    fn getPartition(self: *Store, allocator: std.mem.Allocator, day: u64) !*Partition {
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
            .{ self.path, std.fs.path.sep_str, partitionsFolderName, std.fs.path.sep_str, day },
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
        const dataFolderPath = try std.mem.concat(allocator, u8, &.{ partitionPath, dataFolderName });
        defer allocator.free(dataFolderPath);
        const indexFolderPath = try std.mem.concat(allocator, u8, &.{ partitionPath, indexFolderName });
        defer allocator.free(indexFolderName);

        try createParitionFiles(allocator, dataFolderPath, indexFolderPath);
        const partition = try self.openPartition(allocator, partitionPath, day);

        return partition;
    }

    fn openPartition(self: *Store, allocator: std.mem.Allocator, path: []const u8, day: u64) !*Partition {
        const indexTable = try IndexRecorder.init(allocator, "");
        const index = try Index.init(allocator, indexTable);

        // TODO: replace abc to path from config file
        const data = try Data.init(allocator, self.backgroundAllocator, "abc"[0..]);
        // TODO: remove unused parts directories

        const cache = try Cache.StreamCache.init(allocator);

        const partition = try allocator.create(Partition);
        partition.* = Partition{
            .day = day,
            .path = path,
            .index = index,
            .data = data,
            .streamCache = cache,
        };
        self.hot = partition;
        try self.partitions.append(allocator, partition);

        return partition;
    }
};

fn createParitionFiles(allocator: std.mem.Allocator, indexFolderPath: []const u8, dataFolderPath: []const u8) !void {
    try std.fs.makeDirAbsolute(indexFolderPath);
    try std.fs.makeDirAbsolute(dataFolderPath);

    // TODO: consider using static buffer allocator
    const partsFilePath = try std.mem.concat(allocator, u8, &.{ dataFolderPath, partsFileName });
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
