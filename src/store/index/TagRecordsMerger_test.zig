const std = @import("std");
const Allocator = std.mem.Allocator;

const TagRecordsParser = @import("TagRecordsParser.zig");
const MemBlock = @import("MemBlock.zig");

const TagRecordsMerger = @import("TagRecordsMerger.zig");

const testing = std.testing;

test "removeDuplicatedStreams" {
    const Case = struct {
        input: []const u128,
        expected: []const u128,
    };
    const cases = [_]Case{
        .{
            .input = &.{ 42, 42 },
            .expected = &.{42},
        },
        .{
            .input = &.{ 1, 42 },
            .expected = &.{ 1, 42 },
        },
        .{
            .input = &.{ 1, 2, 2, 3, 3, 3 },
            .expected = &.{ 1, 2, 3 },
        },
    };

    for (cases) |case| {
        const alloc = testing.allocator;
        var m: TagRecordsMerger = .{};
        defer m.deinit(alloc);
        try m.streamIDs.appendSlice(alloc, case.input);

        m.removeDuplicatedStreams();

        try testing.expectEqualSlices(u128, m.streamIDs.items, case.expected);
    }
}

const Field = @import("../lines.zig").Field;

pub fn createTagRecord(
    alloc: Allocator,
    tenantID: u64,
    tag: Field,
    streamIDs: []const u128,
) ![]u8 {
    const bufSize = TagRecordsParser.encodeRecordBound(tag, streamIDs.len);
    const buf = try alloc.alloc(u8, bufSize);
    const recordLen = TagRecordsParser.encodeRecord(buf, tenantID, tag, streamIDs);
    return buf[0..recordLen];
}

test "statesPrefixEqual" {
    const Case = struct {
        tenantA: u64,
        tenantB: u64,
        tagA: Field,
        tagB: Field,
        expected: bool,
    };
    const cases = [_]Case{
        .{
            .tenantA = 1,
            .tenantB = 1,
            .tagA = .{ .key = "env", .value = "prod" },
            .tagB = .{ .key = "env", .value = "prod" },
            .expected = true,
        },
        .{
            .tenantA = 1,
            .tenantB = 2,
            .tagA = .{ .key = "env", .value = "prod" },
            .tagB = .{ .key = "env", .value = "prod" },
            .expected = false,
        },
        .{
            .tenantA = 1,
            .tenantB = 1,
            .tagA = .{ .key = "env", .value = "prod" },
            .tagB = .{ .key = "env", .value = "dev" },
            .expected = false,
        },
    };

    for (cases) |case| {
        const alloc = testing.allocator;
        var m: TagRecordsMerger = .{};
        defer m.deinit(alloc);

        const record1 = try createTagRecord(alloc, case.tenantA, case.tagA, &[_]u128{100});
        defer alloc.free(record1);
        const record2 = try createTagRecord(alloc, case.tenantB, case.tagB, &[_]u128{200});
        defer alloc.free(record2);

        try m.state.setup(record1);
        try m.prevState.setup(record2);

        try testing.expectEqual(case.expected, m.statesPrefixEqual());
    }
}

test "moveParsedState" {
    const alloc = testing.allocator;
    var m: TagRecordsMerger = .{};
    defer m.deinit(alloc);

    const tag = Field{ .key = "app", .value = "web" };
    const streamIDs = &[_]u128{ 100, 200, 300 };
    const record = try createTagRecord(alloc, 1, tag, streamIDs);
    defer alloc.free(record);

    try m.state.setup(record);
    try m.state.parseStreamIDs(alloc);

    const origState = m.state;
    const origPrevState = m.prevState;

    try m.moveParsedState(alloc);

    // streamIDs should be moved to merger
    try testing.expectEqual(@as(usize, 3), m.streamIDs.items.len);
    try testing.expectEqualSlices(u128, streamIDs, m.streamIDs.items);

    // states should be swapped
    try testing.expectEqual(origPrevState, m.state);
    try testing.expectEqual(origState, m.prevState);
}

test "writeState empty" {
    const alloc = testing.allocator;
    var m: TagRecordsMerger = .{};
    defer m.deinit(alloc);

    var target = try MemBlock.init(alloc, .{
        .maxMemBlockSize = 64,
        .blocksCountHint = 1,
    });
    defer target.deinit(alloc);

    try m.writeState(alloc, target);

    try testing.expectEqual(@as(usize, 0), target.memEntries.items.len);
}

test "writeState" {
    const alloc = testing.allocator;

    const Case = struct {
        tag: Field,
        recordStreamIDs: []const u128,
        initial: []const u128,
        expected: []const u128,
    };

    const cases = [_]Case{
        .{
            .tag = Field{ .key = "region", .value = "eu" },
            .recordStreamIDs = &[_]u128{ 300, 100, 200 },
            .initial = &[_]u128{ 300, 100, 200 },
            .expected = &[_]u128{ 100, 200, 300 },
        },
        .{
            .tag = Field{ .key = "env", .value = "prod" },
            .recordStreamIDs = &[_]u128{1},
            .initial = &[_]u128{ 50, 10, 10, 30, 50, 20 },
            .expected = &[_]u128{ 10, 20, 30, 50 },
        },
    };

    for (cases) |case| {
        var m: TagRecordsMerger = .{};
        defer m.deinit(alloc);

        const record = try createTagRecord(alloc, 1, case.tag, case.recordStreamIDs);
        defer alloc.free(record);

        try m.prevState.setup(record);
        try m.streamIDs.appendSlice(alloc, case.initial);

        var target = try MemBlock.init(alloc, .{
            .maxMemBlockSize = 256,
            .blocksCountHint = 1,
        });
        defer target.deinit(alloc);

        try m.writeState(alloc, target);

        try testing.expectEqual(@as(usize, 1), target.memEntries.items.len);
        try testing.expectEqual(@as(usize, 0), m.streamIDs.items.len);

        var verifyState: TagRecordsParser = .{};
        defer verifyState.deinit(alloc);

        try verifyState.setup(@constCast(target.get(0)));
        try verifyState.parseStreamIDs(alloc);

        try testing.expectEqualSlices(u128, case.expected, verifyState.streamIDs.items);
    }
}
