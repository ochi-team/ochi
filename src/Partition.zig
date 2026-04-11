const std = @import("std");
const Allocator = std.mem.Allocator;

const Encoder = @import("encoding").Encoder;

const DataRecorder = @import("DataRecorder.zig");
const IndexRecorder = @import("store/index/IndexRecorder.zig");
const Index = @import("store/index/Index.zig");

const Cache = @import("stds/Cache.zig");
const Line = @import("store/lines.zig").Line;
const SID = @import("store/lines.zig").SID;
const Field = @import("store/lines.zig").Field;

const Conf = @import("Conf.zig");
const Filenames = @import("Filenames.zig");
const fs = @import("fs.zig");

fn streamIndexLess(lines: std.ArrayList(Line), i: u32, j: u32) bool {
    return lines.items[i].sid.lessThan(&lines.items[j].sid);
}

const partitionKeySize = 8;

pub const Partition = @This();

/// path is an absolute path to the partition
path: []const u8,
/// day is an internal key for the partition, represents as a day since epoch time,
day: u64,
/// key is a human readable representation of the partition,
/// it's also used as a folder name for both index and data tables,
/// it's derived from the day
key: []const u8,
index: *Index,
data: *DataRecorder,

/// stream cache for ingestion, shared across all partitions,
/// so the partition doesn't own it
streamCache: *Cache.StreamCache,

pub fn open(
    alloc: Allocator,
    path: []const u8,
    indexPath: []const u8,
    dataPath: []const u8,
    day: u64,
    streamCache: *Cache.StreamCache,
) !*Partition {
    std.debug.assert(std.fs.path.isAbsolute(path));
    std.debug.assert(path[path.len - 1] != std.fs.path.sep);

    const conf = Conf.getConf();

    const partitionKey = std.fs.path.basename(path);
    std.debug.assert(partitionKey.len == partitionKeySize);

    const indexExists = try fs.pathExists(indexPath);
    const dataExists = try fs.pathExists(dataPath);

    if (!indexExists or !dataExists) {
        // TODO: if we write index first it might happen that index exists, but data doesn't,
        // therefore makes sense to have that state recoverable
        if (indexExists != dataExists) {
            // TODO: add a config option not to fail here
            std.debug.panic("data partition exists but index partition doesn't, " ++
                "data is corrupted, path: {s}", .{path});
        }

        std.debug.print("partition doesn't exist due to recent crash or a data loss," ++
            " creating missing partition, path: {s}\n", .{path});
        IndexRecorder.createDir(indexPath);
        DataRecorder.createDir(dataPath);
    }

    const indexRecorder = try IndexRecorder.init(alloc, indexPath, conf.server.pools.cpus);
    errdefer indexRecorder.deinit(alloc);

    const index = try Index.init(alloc, indexRecorder);
    errdefer index.deinit(alloc);

    const data = try DataRecorder.init(alloc, dataPath, conf.server.pools.cpus);
    errdefer data.deinit(alloc);

    const partition = try alloc.create(Partition);
    partition.* = Partition{
        .day = day,
        .path = path,
        .key = partitionKey,
        .index = index,
        .data = data,
        .streamCache = streamCache,
    };

    return partition;
}

pub fn close(self: *Partition, allocator: Allocator) void {
    self.index.deinit(allocator);

    self.data.stop(allocator) catch |err| {
        std.debug.panic("failed to stop data recorder in partition close: {s}", .{@errorName(err)});
    };
    allocator.destroy(self);
}

pub fn createDir(path: []const u8, indexPath: []const u8, dataPath: []const u8) void {
    fs.makeDirAssert(path);

    IndexRecorder.createDir(indexPath);
    DataRecorder.createDir(dataPath);

    fs.syncPathAndParentDir(path);
}

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

    // TODO: revisit when we understand the lines life cycle
    var size: u32 = 0;
    for (lines.items) |line| {
        size += line.rawSize();
    }
    try self.data.addLines(allocator, lines.items, size);
}

fn isCached(self: *Partition, sid: SID) bool {
    var cacheKey: [SID.encodeBound + partitionKeySize]u8 = undefined;
    var enc = Encoder.init(&cacheKey);
    sid.encode(&enc);
    @memcpy(cacheKey[SID.encodeBound..], self.key);

    return self.streamCache.contains(cacheKey[0..]);
}

fn cache(self: *Partition, sid: SID) !void {
    var cacheKey: [SID.encodeBound + partitionKeySize]u8 = undefined;
    var enc = Encoder.init(&cacheKey);
    sid.encode(&enc);
    @memcpy(cacheKey[SID.encodeBound..], self.key);

    try self.streamCache.set(&cacheKey, {});
}
