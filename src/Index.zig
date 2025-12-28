const std = @import("std");
const Allocator = std.mem.Allocator;

const SID = @import("store/lines.zig").SID;
const Field = @import("store/lines.zig").Field;
const IndexTable = @import("IndexTable.zig");

const Encoder = @import("encoding").Encoder;

const ControlChar = enum(u8) {
    escape = 0,
    tagTerminator = 1,
};

fn encodeTag(alloc: Allocator, tag: Field) ![]u8 {
    var encodedKey = try escapeTag(alloc, tag.key);
    defer encodedKey.deinit(alloc);

    var encodedValue = try escapeTag(alloc, tag.value);
    defer encodedValue.deinit(alloc);

    var result = try alloc.alloc(u8, encodedKey.items.len + encodedValue.items.len);
    @memcpy(result[0..encodedKey.items.len], encodedKey.items);
    @memcpy(result[encodedKey.items.len..], encodedValue.items);

    return result;
}

fn escapeTag(alloc: Allocator, buf: []const u8) !std.ArrayList(u8) {
    var res = try std.ArrayList(u8).initCapacity(alloc, buf.len);

    var last: usize = 0;
    for (0..buf.len) |i| {
        switch (buf[i]) {
            @intFromEnum(ControlChar.escape) => {
                try res.appendSlice(alloc, buf[last .. i + 1]);
                try res.append(alloc, '0');
                last = i + 1;
            },
            @intFromEnum(ControlChar.tagTerminator) => {
                try res.appendSlice(alloc, buf[last .. i + 1]);
                try res.append(alloc, '1');
                last = i + 1;
            },
            else => {},
        }
    }

    if (last < buf.len) {
        try res.appendSlice(alloc, buf[last..]);
    }
    return res;
}

const IndexKind = enum(u8) {
    sid = 0,
    sidToTags = 1,
    tagToSids = 2,
};

const Self = @This();

table: *IndexTable,

pub fn init(allocator: std.mem.Allocator, table: *IndexTable) !*Self {
    const i = try allocator.create(Self);
    i.* = .{
        .table = table,
    };
    return i;
}

pub fn hasStream(self: *Self, sid: SID) bool {
    _ = self;
    _ = sid;
    unreachable;
}
pub fn indexStream(self: *Self, alloc: Allocator, sid: SID, tags: []Field, encodedTags: []const u8) !void {
    var entries = try alloc.alloc([]const u8, 2 + tags.len);
    alloc.free(entries);
    var ei: usize = 0;

    // index stream existence
    var sidBuf = try alloc.alloc(u8, 1 + SID.encodeBound);
    sidBuf[0] = @intFromEnum(IndexKind.sid);
    var enc = Encoder.init(sidBuf[1..]);
    sid.encode(&enc);
    entries[ei] = sidBuf;
    ei += 1;

    // index stream -> tags
    // it's stored in index instead of data
    // in order not to duplicate the tags data in every block
    var sidTagsBuf = try alloc.alloc(u8, 1 + SID.encodeBound + encodedTags.len);
    sidTagsBuf[0] = @intFromEnum(IndexKind.sidToTags);
    @memcpy(sidTagsBuf[1..33], enc.buf[0..]);
    @memcpy(sidTagsBuf[33..], encodedTags);
    entries[ei] = sidTagsBuf;
    ei += 1;

    var stackFba = std.heap.stackFallback(128, alloc);
    var fba = stackFba.get();
    // index inverted tag -> stream
    for (tags) |tag| {
        const bufSize = 1 + SID.encodeBound + tag.key.len + tag.value.len;
        const tagSidsBuf = try alloc.alloc(u8, bufSize);

        tagSidsBuf[0] = @intFromEnum(IndexKind.tagToSids);
        @memcpy(tagSidsBuf[1..17], enc.buf[0..16]);
        const encodedTag = try encodeTag(fba, tag);
        @memcpy(tagSidsBuf[17..], encodedTag);
        const offset = 17 + tag.key.len + tag.value.len;
        @memcpy(tagSidsBuf[offset..], enc.buf[16..]);

        fba.free(encodedTag);
        entries[ei] = tagSidsBuf;
        ei += 1;
    }

    try self.table.add(alloc, entries);
}
