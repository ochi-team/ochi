const std = @import("std");

const Field = @import("../lines.zig").Field;

const TagRecordsParser = @import("TagRecordsParser.zig");

const testing = std.testing;

test "setup parses tag record" {
    const alloc = testing.allocator;
    var state: TagRecordsParser = .{};
    defer state.deinit(alloc);

    const tag = Field{ .key = "env", .value = "prod" };
    const streamIDs = &[_]u128{ 100, 200 };
    const bufSize = TagRecordsParser.encodeRecordBound(tag, streamIDs.len);
    const buf = try alloc.alloc(u8, bufSize);
    defer alloc.free(buf);
    const totalLen = TagRecordsParser.encodeRecord(buf, 42, tag, streamIDs);

    try state.setup(buf[0..totalLen]);

    try testing.expectEqual(@as(u64, 42), state.tenantID);
    try testing.expectEqualStrings("env", state.tag.key);
    try testing.expectEqualStrings("prod", state.tag.value);
    try testing.expectEqual(@as(usize, 2), state.streamsLen());

    // Test parsing stream IDs
    try state.parseStreamIDs(alloc);

    try testing.expectEqual(@as(usize, 2), state.streamIDs.items.len);
    try testing.expectEqual(@as(u128, 100), state.streamIDs.items[0]);
    try testing.expectEqual(@as(u128, 200), state.streamIDs.items[1]);

    // Test encodePrefix
    const prefixLen = TagRecordsParser.encodePrefixBound(state.tag);
    var outBuf: [128]u8 = undefined;
    TagRecordsParser.encodePrefix(&outBuf, state.tenantID, state.tag);

    try testing.expectEqualSlices(u8, buf[0..prefixLen], outBuf[0..prefixLen]);
}

test "parseStreamIDs empty" {
    const alloc = testing.allocator;
    var state: TagRecordsParser = .{};
    defer state.deinit(alloc);

    const tag = Field{ .key = "k", .value = "v" };
    const streamIDs = &[_]u128{};
    const bufSize = TagRecordsParser.encodeRecordBound(tag, streamIDs.len);
    const buf = try alloc.alloc(u8, bufSize);
    defer alloc.free(buf);
    const totalLen = TagRecordsParser.encodeRecord(buf, 1, tag, streamIDs);

    try state.setup(buf[0..totalLen]);
    try state.parseStreamIDs(alloc);

    try testing.expectEqual(@as(usize, 0), state.streamIDs.items.len);
}

test "setup resets parsed stream ids" {
    const alloc = testing.allocator;
    var state: TagRecordsParser = .{};
    defer state.deinit(alloc);

    const tag = Field{ .key = "k", .value = "v" };
    const firstBuf = try alloc.alloc(u8, TagRecordsParser.encodeRecordBound(tag, 3));
    const first = firstBuf[0..TagRecordsParser.encodeRecord(firstBuf, 42, tag, &[_]u128{ 1, 2, 3 })];
    defer alloc.free(first);
    const secondBuf = try alloc.alloc(u8, TagRecordsParser.encodeRecordBound(tag, 1));
    const second = secondBuf[0..TagRecordsParser.encodeRecord(secondBuf, 42, tag, &[_]u128{9})];
    defer alloc.free(second);

    try state.setup(first);
    try state.parseStreamIDs(alloc);
    try testing.expectEqualSlices(u128, &[_]u128{ 1, 2, 3 }, state.streamIDs.items);
    try testing.expectEqualDeep(tag, state.tag);

    try state.setup(second);
    try state.parseStreamIDs(alloc);
    try testing.expectEqualSlices(u128, &[_]u128{9}, state.streamIDs.items);
    try testing.expectEqualDeep(tag, state.tag);
}

test "setupStreamsRaw resets parsed stream ids" {
    const alloc = testing.allocator;
    var state: TagRecordsParser = .{};
    defer state.deinit(alloc);

    var tag = Field{ .key = "k", .value = "v" };

    const firstBuf = try alloc.alloc(u8, TagRecordsParser.encodeRecordBound(tag, 3));
    const first = firstBuf[0..TagRecordsParser.encodeRecord(firstBuf, 42, tag, &[_]u128{ 1, 2, 3 })];
    defer alloc.free(first);

    const secondBuf = try alloc.alloc(u8, TagRecordsParser.encodeRecordBound(tag, 1));
    const second = secondBuf[0..TagRecordsParser.encodeRecord(secondBuf, 42, tag, &[_]u128{9})];
    defer alloc.free(second);

    const tenantOffset = 1 + @sizeOf(u64);
    const tagPortion = first[tenantOffset..];
    const offset = tag.decodeIndexTag(tagPortion);

    // make a slice of the streams part only
    const firstStreamsRaw = first[tenantOffset + offset ..];
    const secondStreamsRaw = second[tenantOffset + offset ..];

    try state.setupStreamsRaw(firstStreamsRaw);
    try state.parseStreamIDs(alloc);
    try testing.expectEqualSlices(u128, &[_]u128{ 1, 2, 3 }, state.streamIDs.items);

    try state.setupStreamsRaw(secondStreamsRaw);
    try state.parseStreamIDs(alloc);
    try testing.expectEqualSlices(u128, &[_]u128{9}, state.streamIDs.items);
}
