const std = @import("std");
const Allocator = std.mem.Allocator;
const Io = std.Io;

const Encoder = @import("encoding").Encoder;
const QuerySIDsResult = @import("store/index/Index.zig").QuerySIDsResult;

const DataRecorder = @import("DataRecorder.zig");
const IndexRecorder = @import("store/index/IndexRecorder.zig");
const Index = @import("store/index/Index.zig");
const MemBlock = @import("store/index/MemBlock.zig");

const Cache = @import("stds/Cache.zig").Cache;
const Line = @import("store/lines.zig").Line;
const SID = @import("store/lines.zig").SID;
const Field = @import("store/lines.zig").Field;
const Query = @import("query/Query.zig");

const Runtime = @import("Runtime.zig");
const fs = @import("fs.zig");
const Logger = @import("logging");

const partitionKeySize = 8;

// TODO: redesign a partition range to rely not on the time, but the size:
// 1. limit the max amount of tables to 32
// 2. limit the max table size to 100gb
// 3. reaching the limit the partition become a read only:
// a computed property returning enum state == .full
// 4. the flag must tell to the store to attempt to create new partition
// if the store itself is not read only
// 5. a partition has a range: u32 instead of a day service timestamp in seconds the data begins at: [range, ...)
// 6. it requires reshape how the partitions are stored and open in the store
pub const Partition = @This();

/// path is an absolute path to the partition
path: []const u8,
/// day is an internal key for the partition, represents as a day since epoch time,
day: u32,
/// key is a human readable representation of the partition,
/// it's also used as a folder name for both index and data tables,
/// it's derived from the day
key: []const u8,
index: Index,
data: *DataRecorder,

/// stream cache for ingestion, shared across all partitions,
/// so the partition doesn't own it
streamCache: *Cache(void),

refCounter: std.atomic.Value(u32) = .init(1),

/// Partition is a managed resource by a ref counter,
/// therefore it requires an allocator
alloc: Allocator,

pub fn open(
    io: Io,
    alloc: Allocator,
    path: []const u8,
    indexPath: []const u8,
    dataPath: []const u8,
    day: u32,
    streamCache: *Cache(void),
    runtime: *Runtime,
) !*Partition {
    std.debug.assert(std.fs.path.isAbsolute(path));
    std.debug.assert(path[path.len - 1] != std.fs.path.sep);

    const partitionKey = std.fs.path.basename(path);
    std.debug.assert(partitionKey.len == partitionKeySize);

    const indexExists = try fs.pathExists(io, indexPath);
    const dataExists = try fs.pathExists(io, dataPath);

    if (!indexExists or !dataExists) {
        if (indexExists != dataExists) {
            std.debug.panic("data partition exists but index partition doesn't, " ++
                "data is corrupted, path: {s}", .{path});
        }

        Logger.log(.warn, "partition does not exist, creating missing partition", .{ .path = path });
        try IndexRecorder.createDir(io, indexPath);
        try DataRecorder.createDir(io, dataPath);
    }

    const indexRecorder = try IndexRecorder.init(io, alloc, indexPath, runtime);
    errdefer indexRecorder.stop(io, alloc) catch |err| {
        Logger.log(.err, "failed to stop index recorder in partition opening", .{ .err = err });
    };
    try indexRecorder.start(io, alloc);
    const index = Index.init(indexRecorder);

    const data = try DataRecorder.init(io, alloc, dataPath, runtime);
    errdefer data.stop(io, alloc) catch |err| {
        Logger.log(.err, "failed to stop data recorder in partition opening", .{ .err = err });
    };
    try data.start(io, alloc);

    const partition = try alloc.create(Partition);
    errdefer alloc.destroy(partition);
    partition.* = Partition{
        .alloc = alloc,
        .day = day,
        .path = path,
        .key = partitionKey,
        .index = index,
        .data = data,
        .streamCache = streamCache,
    };

    return partition;
}

pub fn retain(self: *Partition) void {
    // TODO: review all the atomics ordering
    _ = self.refCounter.fetchAdd(1, .acquire);
}

pub fn release(self: *Partition, io: Io) void {
    const prev = self.refCounter.fetchSub(1, .acq_rel);
    std.debug.assert(prev > 0);

    if (prev != 1) return;

    self.close(io);

    // TODO: deletion of the partition due to retention or eviction policy
    // read table release implementation as a sample
}

pub fn close(
    self: *Partition,
    io: Io,
) void {
    self.index.recorder.stop(io, self.alloc) catch |err| {
        std.debug.panic("failed to stop index recorder in partition close: {s}", .{@errorName(err)});
    };
    self.data.stop(io, self.alloc) catch |err| {
        std.debug.panic("failed to stop data recorder in partition close: {s}", .{@errorName(err)});
    };
    self.alloc.destroy(self);
}

pub fn createDir(io: Io, path: []const u8, indexPath: []const u8, dataPath: []const u8) !void {
    try fs.createDirAssert(io, path);

    try IndexRecorder.createDir(io, indexPath);
    try DataRecorder.createDir(io, dataPath);

    try fs.syncPathAndParentDir(io, path);
}

pub fn addLines(
    self: *Partition,
    io: Io,
    allocator: Allocator,
    lines: std.ArrayList(Line),
    tags: []Field,
    encodedTags: []const u8,
    sid: SID,
    blocksCache: *Cache(*MemBlock),
) !void {
    if (!self.isCached(io, sid)) {
        if (!try self.index.hasStream(io, allocator, sid, blocksCache)) {
            try self.index.indexStream(io, allocator, sid, tags, encodedTags);
        }
        try self.cache(io, sid);
    }

    try self.data.addLines(io, allocator, lines.items, sid);
}

// TODO: this api is not used, but if we want to collect more sids in the processor
// we can ressurect it back
// const bufSize = 1024;
// pub fn addLinesWithSidChunks(
//     self: *Partition,
//     io: Io,
//     allocator: Allocator,
//     lines: std.ArrayList(Line),
//     tags: []Field,
//     encodedTags: []const u8,
//     sids: []SID,
//     blocksCache: *Cache(*MemBlock),
// ) !void {
//     var fallbackFba = std.heap.stackFallback(bufSize, allocator);
//     const fba = fallbackFba.get();
//     var streamsToCache = try std.ArrayList(u32).initCapacity(fba, bufSize / @sizeOf(u32));
//     defer streamsToCache.deinit(fba);
//
//     // detect not cached stream ids
//     for (0..lines.items.len) |i| {
//         const line = lines.items[i];
//         if (self.isCached(io, line.sid)) {
//             continue;
//         }
//
//         if (streamsToCache.items.len == 0) {
//             try streamsToCache.append(fba, @intCast(i));
//             continue;
//         }
//
//         const lineToCacheIdx = streamsToCache.items[streamsToCache.items.len - 1];
//         const lineToCache = lines.items[lineToCacheIdx];
//         if (!line.sid.eql(&lineToCache.sid)) {
//             try streamsToCache.append(fba, @intCast(i));
//         }
//     }
//
//     if (streamsToCache.items.len > 0) {
//         // sort the stream ids,
//         // it's necessary in case the incoming lines are mixed like [1, 3, 2],
//         // so to make it [1, 2, 3]
//         std.sort.pdq(u32, streamsToCache.items, lines, streamIndexLess);
//     }
//
//     for (streamsToCache.items, 0..) |i, pos| {
//         const sid = lines.items[i].sid;
//
//         if (pos > 0 and lines.items[streamsToCache.items[pos - 1]].sid.eql(&sid)) continue;
//
//         if (self.isCached(io, lines.items[i].sid)) continue;
//
//         if (!try self.index.hasStream(io, allocator, sid, blocksCache)) {
//             try self.index.indexStream(io, allocator, sid, tags, encodedTags);
//         }
//         try self.cache(io, sid);
//     }
//
//     try self.data.addLines(io, allocator, lines.items);
// }

// TODO: experiment with scan sharing,
// we could hash the query for a very short time (3s)
// and if the query hash matches we could bypass index lookup and data blocks discovery,
// but it has to come after table cache eviction implementation
pub fn queryLines(
    self: *Partition,
    io: Io,
    alloc: Allocator,
    longAlloc: Allocator,
    tenantID: u64,
    query: Query,
    memBlocksCache: *Cache(*MemBlock),
) !std.ArrayList(Line) {
    // TODO: query cancelation

    var sidsRes: QuerySIDsResult = sids: {
        // if streamIDs are passed in a query we don't need to query them,
        // just sort and join with tenant
        if (query.streamIDs) |streamIDs| {
            var sids = try std.ArrayList(SID).initCapacity(alloc, streamIDs.len);

            for (streamIDs) |streamID| {
                // TODO: this is a signal of idiotism, we have to split stream and tenants
                // in query path
                sids.appendAssumeCapacity(.{
                    .tenantID = tenantID,
                    .id = streamID,
                });
            }

            std.sort.pdq(SID, sids.items, {}, SID.sortLessThan);

            break :sids .{ .sids = sids, .cutOff = false };
        } else {
            if (query.tagsExpr) |tags| {
                break :sids try self.index.querySIDs(io, alloc, longAlloc, tenantID, tags, memBlocksCache);
            }

            break :sids .{ .cutOff = false, .sids = .empty };
        }
    };
    defer sidsRes.sids.deinit(alloc);

    if (sidsRes.cutOff) {
        // TODO: add a message to a response
        // so a user could narrow a query
        var tagsBuf: [128]u8 = undefined;
        const tagsN = if (query.tagsExpr) |tagsExpr| tagsExpr.stringifyLimited(&tagsBuf) else 0;
        Logger.log(.warn, "query is cut off by index", .{
            .tenantID = tenantID,
            .partition = self.key,
            .query = tagsBuf[0..tagsN],
        });
    }

    return self.data.queryLines(io, alloc, sidsRes.sids.items, query);
}

pub fn queryStreamIDs(
    self: *Partition,
    io: Io,
    alloc: Allocator,
    longAlloc: Allocator,
    tenantID: u64,
    memBlocksCache: *Cache(*MemBlock),
) !std.AutoArrayHashMapUnmanaged(u128, void) {
    const res = try self.index.queryAllStreamIDs(io, alloc, longAlloc, tenantID, memBlocksCache);
    return res.streamIDs;
}

pub fn flushForce(self: *Partition, io: Io, alloc: Allocator) !void {
    try self.index.recorder.flushForce(io, alloc);
    try self.data.flushForce(io, alloc);
}

fn isCached(self: *Partition, io: Io, sid: SID) bool {
    var cacheKey: [SID.encodeBound + partitionKeySize]u8 = undefined;
    var enc = Encoder.init(&cacheKey);
    sid.encode(&enc);
    @memcpy(cacheKey[SID.encodeBound..], self.key);

    return self.streamCache.contains(io, cacheKey[0..]);
}

fn cache(self: *Partition, io: Io, sid: SID) !void {
    var cacheKey: [SID.encodeBound + partitionKeySize]u8 = undefined;
    var enc = Encoder.init(&cacheKey);
    sid.encode(&enc);
    @memcpy(cacheKey[SID.encodeBound..], self.key);

    try self.streamCache.put(io, &cacheKey, {});
}

pub fn lessThan(_: void, one: *Partition, another: *Partition) bool {
    return one.day < another.day;
}
