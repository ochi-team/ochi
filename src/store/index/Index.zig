const std = @import("std");
const Allocator = std.mem.Allocator;
const Io = std.Io;

const SID = @import("../lines.zig").SID;
const Field = @import("../lines.zig").Field;
const IndexRecorder = @import("IndexRecorder.zig");
const FilterExpression = @import("../../query/Query.zig").FilterExpression;
const FilterPredicate = @import("../../query/Query.zig").FilterPredicate;
const TagRecordsParser = @import("TagRecordsParser.zig");

pub const tracy = @import("tracy");

const Lookup = @import("lookup/Lookup.zig");
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
        @compileError("fix IndexKind usage in IndexTable.mergeTagsRecords");
    }
}

const Self = @This();

recorder: *IndexRecorder,

pub fn init(allocator: std.mem.Allocator, recorder: *IndexRecorder) !*Self {
    const i = try allocator.create(Self);
    i.* = .{
        .recorder = recorder,
    };
    return i;
}

pub fn deinit(self: *Self, io: Io, allocator: Allocator) void {
    self.recorder.stop(io, allocator) catch |err| {
        std.debug.print("failed to stop index recorder in partition close: {s}", .{@errorName(err)});
    };

    allocator.destroy(self);
}

pub fn hasStream(self: *Self, io: Io, alloc: Allocator, sid: SID) !bool {
    var lookup = try Lookup.init(io, alloc, self.recorder);
    defer lookup.deinit(io, alloc);

    const sidBuf = try alloc.alloc(u8, 1 + SID.encodeBound);
    defer alloc.free(sidBuf);
    var enc = Encoder.init(sidBuf);
    sid.encodeTenantWithPrefix(&enc, @intFromEnum(IndexKind.sid));
    enc.writeInt(u128, sid.id);

    const maybeItem = try lookup.findFirstByPrefix(io, alloc, sidBuf);
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
) !StreamIDsByPrefixesResult {
    var lookup = try Lookup.init(io, alloc, self.recorder);
    defer lookup.deinit(io, alloc);

    const suffixLen: usize = 1 + @sizeOf(u64);
    var tenantPrefix: [suffixLen]u8 = undefined;
    var enc = Encoder.init(&tenantPrefix);
    enc.writeInt(u8, @intFromEnum(IndexKind.sid));
    enc.writeInt(u64, tenantID);

    return lookup.findAllStreamIDsByPrefixes(
        io,
        alloc,
        &.{&tenantPrefix},
    );
}

pub fn indexStream(self: *Self, io: Io, alloc: Allocator, sid: SID, tags: []Field, encodedTags: []const u8) !void {
    const z = tracy.Zone.begin(.{
        .src = @src(),
        .name = ".Index.indexStream",
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

// TODO: this bool flag under big question, redesign it later after solid query design
pub const QuerySIDsResult = struct { sids: std.ArrayList(SID), cutOff: bool };
pub fn querySIDs(
    self: *Self,
    io: Io,
    alloc: Allocator,
    tenantID: u64,
    tags: *const FilterExpression,
) !QuerySIDsResult {
    // TODO: cache query => stream
    var lookup = try Lookup.init(io, alloc, self.recorder);
    defer lookup.deinit(io, alloc);

    var result = try querySIDsFromExpr(io, alloc, &lookup, tenantID, tags);
    defer result.streamIDs.deinit(alloc);

    if (result.streamIDs.keys().len == 0)
        return .{ .sids = .empty, .cutOff = false };

    var sids: std.ArrayList(SID) = try .initCapacity(alloc, result.streamIDs.keys().len);
    for (result.streamIDs.keys()) |s| {
        // TODO: ideally we look only for streams, the tenant is known in advance,
        // we must design the API to return only Array(streams)
        sids.appendAssumeCapacity(.{ .id = s, .tenantID = tenantID });
    }

    // import to sort it since the data query expected sorted set of streams
    std.sort.pdq(SID, sids.items, {}, SidLessThan);

    return .{ .sids = sids, .cutOff = result.cutOff };
}

// TODO: pass destination AutoArrayHashMapUnmanaged to collect the keys
fn querySIDsFromExpr(
    io: Io,
    alloc: Allocator,
    lookup: *Lookup,
    tenantID: u64,
    expr: *const FilterExpression,
) !StreamIDsByPrefixesResult {
    switch (expr.*) {
        .predicate => |p| return querySIDsFromPredicate(io, alloc, lookup, tenantID, p),
        .andOp => |ops| {
            var left = try querySIDsFromExpr(io, alloc, lookup, tenantID, ops[0]);
            defer left.streamIDs.deinit(alloc);

            if (left.streamIDs.keys().len == 0)
                return .{ .streamIDs = .empty, .cutOff = left.cutOff };

            var right = try querySIDsFromExpr(io, alloc, lookup, tenantID, ops[1]);
            defer right.streamIDs.deinit(alloc);

            var intersection: std.AutoArrayHashMapUnmanaged(u128, void) = .empty;
            errdefer intersection.deinit(alloc);
            for (left.streamIDs.keys()) |sid| {
                if (right.streamIDs.contains(sid)) {
                    try intersection.put(alloc, sid, {});
                }
            }
            return .{ .streamIDs = intersection, .cutOff = left.cutOff or right.cutOff };
        },
        .orOp => |ops| {
            var left = try querySIDsFromExpr(io, alloc, lookup, tenantID, ops[0]);
            errdefer left.streamIDs.deinit(alloc);

            var right = try querySIDsFromExpr(io, alloc, lookup, tenantID, ops[1]);
            defer right.streamIDs.deinit(alloc);

            for (right.streamIDs.keys()) |sid| {
                try left.streamIDs.put(alloc, sid, {});
            }
            return .{ .streamIDs = left.streamIDs, .cutOff = left.cutOff or right.cutOff };
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

// TODO: when we collect only streams we can sort them without tenant,
// and we can remove this function
pub fn SidLessThan(_: void, self: SID, another: SID) bool {
    return self.tenantID < another.tenantID or
        (self.tenantID == another.tenantID and self.id < another.id);
}
