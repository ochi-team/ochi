const std = @import("std");
const Allocator = std.mem.Allocator;

const SID = @import("../lines.zig").SID;
const Field = @import("../lines.zig").Field;
const IndexRecorder = @import("IndexRecorder.zig");
const Query = @import("../query.zig").Query;
const TagRecordsParseState = @import("TagRecordsParseState.zig");

const Lookup = @import("lookup/Lookup.zig");

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

pub fn deinit(self: *Self, allocator: Allocator) void {
    self.recorder.stop(allocator) catch |err| {
        std.debug.panic("failed to stop index recorder in partition close: {s}", .{@errorName(err)});
    };

    allocator.destroy(self);
}

pub fn hasStream(self: *Self, alloc: Allocator, sid: SID) !bool {
    var lookup = try Lookup.init(alloc, self.recorder);
    defer lookup.deinit(alloc);

    const sidBuf = try alloc.alloc(u8, 1 + SID.encodeBound);
    defer alloc.free(sidBuf);
    var enc = Encoder.init(sidBuf);
    sid.encodeTenantWithPrefix(&enc, @intFromEnum(IndexKind.sid));
    enc.writeInt(u128, sid.id);

    const maybeItem = try lookup.findFirstByPrefix(alloc, sidBuf);
    if (maybeItem) |item| {
        return item.len == sidBuf.len;
    }

    return false;
}

pub fn indexStream(self: *Self, alloc: Allocator, sid: SID, tags: []Field, encodedTags: []const u8) !void {
    var entries = try alloc.alloc([]const u8, 2 + tags.len);
    var ei: usize = 0;
    defer {
        for (ei..0) |i| alloc.free(entries[i]);
        alloc.free(entries);
    }

    // index stream existence
    const sidBuf = try alloc.alloc(u8, 1 + SID.encodeBound);
    defer alloc.free(sidBuf);

    var enc = Encoder.init(sidBuf);
    sid.encodeTenantWithPrefix(&enc, @intFromEnum(IndexKind.sid));
    enc.writeInt(u128, sid.id);

    entries[ei] = sidBuf;
    ei += 1;

    const tenantID = enc.buf[1..17];
    const streamID = enc.buf[17..];

    // index stream -> tags
    // it's stored in index instead of data
    // in order not to duplicate the tags data in every block
    var sidTagsBuf = try alloc.alloc(u8, 1 + SID.encodeBound + encodedTags.len);
    defer alloc.free(sidTagsBuf);

    sidTagsBuf[0] = @intFromEnum(IndexKind.sidToTags);
    @memcpy(sidTagsBuf[1..33], enc.buf[0..]);
    @memcpy(sidTagsBuf[33..], encodedTags);
    entries[ei] = sidTagsBuf;
    ei += 1;

    // index inverted tag -> stream
    for (tags) |tag| {
        const bufSize = 1 + SID.encodeBound + tag.encodeIndexTagBound();
        const tagSidsBuf = try alloc.alloc(u8, bufSize);

        tagSidsBuf[0] = @intFromEnum(IndexKind.tagToSids);
        @memcpy(tagSidsBuf[1..17], tenantID);
        const offset = tag.encodeIndexTag(tagSidsBuf[17..]);
        @memcpy(tagSidsBuf[17 + offset ..], streamID);

        entries[ei] = tagSidsBuf;
        ei += 1;
    }

    try self.recorder.add(alloc, entries);
}

const QueryStreamsResult = struct { streamIDs: std.ArrayList(SID), cutOff: bool };
pub fn queryStreams(self: *Self, alloc: Allocator, tenantID: []const u8, tags: []const Field) !QueryStreamsResult {
    // TODO: cache query => stream
    var lookup = try Lookup.init(alloc, self.recorder);
    defer lookup.deinit(alloc);

    var prefixes: std.ArrayList([]const u8) = try .initCapacity(alloc, tags.len);
    defer {
        for (prefixes.items) |p| {
            alloc.free(p);
        }
        prefixes.deinit(alloc);
    }

    var state: TagRecordsParseState = .{};
    defer state.deinit(alloc);

    for (tags) |tag| {
        const prefix = try alloc.alloc(u8, TagRecordsParseState.encodePrefixBound(tag));

        TagRecordsParseState.encodePrefix(prefix, tenantID, tag);

        prefixes.appendAssumeCapacity(prefix);
    }

    const items =
        try lookup.findAllByPrefixes(alloc, prefixes.items) orelse
        return .{ .streamIDs = .empty, .cutOff = false };
    defer {
        for (items.result) |i| {
            alloc.free(i);
        }
        alloc.free(items.result);
    }

    var sids: std.ArrayList(SID) = .empty;

    for (items.result) |i| {
        // TODO: we can setup it from the tail, not the full entry and save a bit on the parsing,
        // the tail is like i[prefix.len..]
        try state.setup(i);

        try state.parseStreamIDs(alloc);

        try sids.ensureUnusedCapacity(alloc, state.streamIDs.items.len);

        for (state.streamIDs.items) |s|
            // TODO: ideally we look only for streams, the tenant is known in advance,
            // we must design the API to return only Array(streams)
            sids.appendAssumeCapacity(.{ .id = s, .tenantID = tenantID });
    }

    return .{ .streamIDs = sids, .cutOff = items.cutOff };
}
