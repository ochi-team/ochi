const std = @import("std");
const Allocator = std.mem.Allocator;
const Io = std.Io;

const SID = @import("../lines.zig").SID;
const Field = @import("../lines.zig").Field;
const Cache = @import("../../stds/Cache.zig").Cache;
const IndexRecorder = @import("IndexRecorder.zig");
const MemBlock = @import("MemBlock.zig");
const FilterExpression = @import("../../query/Query.zig").FilterExpression;
const FilterPredicate = @import("../../query/Query.zig").FilterPredicate;
const TagRecordsParser = @import("TagRecordsParser.zig");

pub const tracy = @import("tracy");

const maxQueryLength = @import("../../query/Loql.zig").maxQueryLength;
const partitionKeySize = @import("../../Partition.zig").partitionKeySize;

const Lookup = @import("lookup/Lookup.zig");
const LookupPool = @import("lookup/LookupPool.zig");
const StreamIDsByPrefixesResult = Lookup.StreamIDsByPrefixesResult;

const Encoder = @import("encoding").Encoder;

pub const IndexKind = enum(u8) {
    // tenant:stream, to writes the key exists
    sid = 0,
    // tenant:stream => tags
    sidToTags = 1,
    // tenant:key:value => streams,
    // inverted index to find streams with the given tag
    tagToSids = 2,
};

comptime {
    if (@typeInfo(IndexKind).@"enum".fields.len != 3) {
        @compileError("fix IndexKind usage in IndexTable.mergeTagsRecords if you update the enum");
    }
}

const Self = @This();

recorder: *IndexRecorder,

pub fn init(recorder: *IndexRecorder) Self {
    return .{
        .recorder = recorder,
    };
}

pub fn hasStream(
    self: *Self,
    io: Io,
    alloc: Allocator,
    sid: SID,
    blocksCache: *Cache(*MemBlock),
    lookupPool: *LookupPool,
) !bool {
    const z = tracy.Zone.begin(.{
        .src = @src(),
        .name = "index.hasStream",
    });
    defer z.end();

    const lookup = lookupPool.next();
    lookup.mx.lockUncancelable(io);
    defer lookup.mx.unlock(io);

    // TODO: remove lookup pool, useless, better to use arenas
    try lookup.val.setup(io, alloc, alloc, self.recorder, blocksCache);
    defer lookup.val.reset(io, alloc);

    const sidBuf = try alloc.alloc(u8, 1 + SID.encodeBound);
    defer alloc.free(sidBuf);
    var enc = Encoder.init(sidBuf);
    sid.encodeTenantWithPrefix(&enc, @intFromEnum(IndexKind.sid));
    enc.writeInt(u128, sid.id);

    const maybeItem = try lookup.val.findFirstByPrefix(io, alloc, sidBuf);
    if (maybeItem) |item| {
        return item.len == sidBuf.len;
    }

    return false;
}

pub fn queryAllStreamIDs(
    self: *Self,
    io: Io,
    alloc: Allocator,
    tenantID: u64,
    memBlocksCache: *Cache(*MemBlock),
    lookupPool: *LookupPool,
) !StreamIDsByPrefixesResult {
    const lookup = lookupPool.next();
    lookup.mx.lockUncancelable(io);
    defer lookup.mx.unlock(io);

    try lookup.val.setup(io, alloc, alloc, self.recorder, memBlocksCache);
    defer lookup.val.reset(io, alloc);

    const suffixLen: usize = 1 + @sizeOf(u64);
    var tenantPrefix: [suffixLen]u8 = undefined;
    var enc = Encoder.init(&tenantPrefix);
    enc.writeInt(u8, @intFromEnum(IndexKind.sid));
    enc.writeInt(u64, tenantID);

    return lookup.val.findAllStreamIDsByPrefixes(
        io,
        alloc,
        &.{&tenantPrefix},
    );
}

pub fn indexStream(self: *Self, io: Io, alloc: Allocator, sid: SID, tags: []Field, encodedTags: []const u8) !void {
    const z = tracy.Zone.begin(.{
        .src = @src(),
        .name = "Index.indexStream",
    });
    defer z.end();

    var entries = try alloc.alloc([]const u8, 2 + tags.len);
    var ei: usize = 0;
    defer {
        for (0..ei) |i| alloc.free(entries[i]);
        alloc.free(entries);
    }

    // index stream existence
    const sidBuf = try alloc.alloc(u8, 1 + SID.encodeBound);

    var enc = Encoder.init(sidBuf);
    sid.encodeTenantWithPrefix(&enc, @intFromEnum(IndexKind.sid));
    enc.writeInt(u128, sid.id);

    entries[ei] = sidBuf;
    ei += 1;

    const tenantID = enc.buf[1..9];
    const streamID = enc.buf[9..];

    // index stream -> tags
    // it's stored in index instead of data
    // in order not to duplicate the tags data in every block
    var sidTagsBuf = try alloc.alloc(u8, 1 + SID.encodeBound + encodedTags.len);

    sidTagsBuf[0] = @intFromEnum(IndexKind.sidToTags);
    @memcpy(sidTagsBuf[1..25], enc.buf[1..25]);
    @memcpy(sidTagsBuf[25..], encodedTags);
    entries[ei] = sidTagsBuf;
    ei += 1;

    // index inverted tag -> stream
    for (tags) |tag| {
        const bufSize = 1 + SID.encodeBound + tag.encodeIndexTagBound();
        const tagSidsBuf = try alloc.alloc(u8, bufSize);

        tagSidsBuf[0] = @intFromEnum(IndexKind.tagToSids);
        @memcpy(tagSidsBuf[1..9], tenantID);
        const offset = tag.encodeIndexTag(tagSidsBuf[9..]);
        @memcpy(tagSidsBuf[9 + offset ..], streamID);

        entries[ei] = tagSidsBuf;
        ei += 1;
    }

    try self.recorder.add(io, alloc, entries);
}

// max query + 8 partitionKey + version
const queryIndexCacheBufferSize = maxQueryLength + partitionKeySize + @sizeOf(u64) + @sizeOf(u32);
const QueryIndexCacheKey = struct {
    partition: []const u8,
    tenantID: u64,
    version: u32,
    tagsFilter: []const u8,

    fn encode(self: *const QueryIndexCacheKey, buf: []u8) usize {
        var enc = Encoder.init(buf);
        enc.writeString(self.partition);
        enc.writeInt(u64, self.tenantID);
        enc.writeInt(u32, self.version);
        enc.writeString(self.tagsFilter);
        return enc.offset;
    }
};
pub const QueryIndexCacheValue = struct {
    sids: []const SID,

    pub fn init(alloc: Allocator, sids: []const SID) !*QueryIndexCacheValue {
        const self = try alloc.create(QueryIndexCacheValue);
        errdefer alloc.destroy(self);

        const sidsCopy = try alloc.dupe(SID, sids);
        errdefer alloc.free(sidsCopy);

        self.* = .{
            .sids = sidsCopy,
        };

        return self;
    }

    pub fn deinit(self: *QueryIndexCacheValue, alloc: Allocator) void {
        alloc.free(self.sids);
        alloc.destroy(self);
    }
};

pub const QuerySIDsResult = struct { sids: std.ArrayList(SID) };
pub fn querySIDs(
    self: *Self,
    io: Io,
    requestArena: Allocator,
    alloc: Allocator,
    tenantID: u64,
    partitionKey: []const u8,
    tags: *const FilterExpression,
    indexMemBlocksCache: *Cache(*MemBlock),
    indexQueryCache: *Cache(*QueryIndexCacheValue),
) !QuerySIDsResult {
    var tagsKeyBuf: [maxQueryLength]u8 = undefined;
    const tagsKeyLen = tags.encodeCacheKey(&tagsKeyBuf);
    const indexCacheVersion = self.recorder.indexCacheKeyVersion.load(.acquire);
    const cacheKey: QueryIndexCacheKey = .{
        .partition = partitionKey,
        .tenantID = tenantID,
        .tagsFilter = tagsKeyBuf[0..tagsKeyLen],
        .version = indexCacheVersion,
    };

    var cacheKeyBuf: [queryIndexCacheBufferSize]u8 = undefined;
    const cacheKeyLen = cacheKey.encode(&cacheKeyBuf);

    const maybeStreams = indexQueryCache.get(io, cacheKeyBuf[0..cacheKeyLen]);
    if (maybeStreams) |streams| {
        var sids: std.ArrayList(SID) = try .initCapacity(requestArena, streams.sids.len);
        for (streams.sids) |s| {
            sids.appendAssumeCapacity(s);
        }
        return .{ .sids = sids };
    }

    var lookup = try Lookup.init(io, requestArena, alloc, self.recorder, indexMemBlocksCache);
    defer lookup.deinit(io, requestArena);

    var result = try querySIDsFromExpr(io, requestArena, &lookup, tenantID, tags);
    defer result.streamIDs.deinit(requestArena);

    const streamKeys = result.streamIDs.keys();

    var sids: std.ArrayList(SID) = try .initCapacity(requestArena, streamKeys.len);
    for (result.streamIDs.keys()) |s| {
        // TODO: ideally we look only for streams, the tenant is known in advance,
        // we must design the API to return only Array(streams)
        sids.appendAssumeCapacity(.{ .id = s, .tenantID = tenantID });
    }

    // important to sort it since the data query expected sorted set of streams
    std.sort.pdq(SID, sids.items, {}, SID.sortLessThan);

    const cachedObj = try QueryIndexCacheValue.init(alloc, sids.items);
    errdefer cachedObj.deinit(alloc);

    _ = try indexQueryCache.put(io, cacheKeyBuf[0..cacheKeyLen], cachedObj);

    return .{ .sids = sids };
}

// TODO: pass destination AutoArrayHashMapUnmanaged to collect the keys,
// it allows not to allocate on Or operation
fn querySIDsFromExpr(
    io: Io,
    alloc: Allocator,
    lookup: *Lookup,
    tenantID: u64,
    expr: *const FilterExpression,
) !StreamIDsByPrefixesResult {
    switch (expr.*) {
        // TODO: having only 2 predicates per bool operation gives no flexibility to collect the data
        // and increases recursion, so worth having them as a slice
        // TODO: consider or document the opposite if we apply condition mat ching at the scaning stage,
        // most of the time the keys comparison is the same, so pushing them down to scan make it executing less often,
        // it requires a sorted list of prefixes/keys/predicates and:
        // 1. split them into groups so we know if they share the same block/prefix
        // 2. if they ordered in .seek call we can skip previous block and continue from the current position
        // or find the next block via binary search

        .predicate => |p| return querySIDsFromPredicate(io, alloc, lookup, tenantID, p),
        .andOp => |ops| {
            var left = try querySIDsFromExpr(io, alloc, lookup, tenantID, ops[0]);
            defer left.streamIDs.deinit(alloc);

            if (left.streamIDs.keys().len == 0)
                return .{ .streamIDs = .empty };

            var right = try querySIDsFromExpr(io, alloc, lookup, tenantID, ops[1]);
            defer right.streamIDs.deinit(alloc);

            var intersection: std.AutoArrayHashMapUnmanaged(u128, void) = .empty;
            errdefer intersection.deinit(alloc);
            for (left.streamIDs.keys()) |sid| {
                if (right.streamIDs.contains(sid)) {
                    try intersection.put(alloc, sid, {});
                }
            }
            return .{ .streamIDs = intersection };
        },
        .orOp => |ops| {
            var left = try querySIDsFromExpr(io, alloc, lookup, tenantID, ops[0]);
            errdefer left.streamIDs.deinit(alloc);

            var right = try querySIDsFromExpr(io, alloc, lookup, tenantID, ops[1]);
            defer right.streamIDs.deinit(alloc);

            for (right.streamIDs.keys()) |sid| {
                try left.streamIDs.put(alloc, sid, {});
            }
            return .{ .streamIDs = left.streamIDs };
        },
    }
}

fn querySIDsFromPredicate(
    io: Io,
    alloc: Allocator,
    lookup: *Lookup,
    tenantID: u64,
    p: FilterPredicate,
) !StreamIDsByPrefixesResult {
    const tag = Field{ .key = p.key, .value = p.value };
    switch (p.op) {
        .equal => {
            const prefix = try alloc.alloc(u8, TagRecordsParser.encodePrefixBound(tag));
            defer alloc.free(prefix);
            TagRecordsParser.encodePrefix(prefix, tenantID, tag);
            return lookup.findAllStreamIDsByPrefixes(io, alloc, &[_][]const u8{prefix});
        },
        else => return error.QueryMatchOperationNotImplemented,
    }
}
