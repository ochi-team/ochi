/// TagRecordsMerger: Merges consecutive tagToSids index records with the same prefix.
///
/// Use cases:
/// - Compacting inverted index entries: multiple (tenant:tag -> streamID) records
///   are merged into a single (tenant:tag -> [streamIDs]) record
/// - Reducing index size by deduplicating stream IDs
///
/// Constraints:
/// - Operates on sorted input; records must be processed in order
/// - Uses two TagRecordsParseState instances for comparing consecutive records
/// - Output streamIDs are sorted and deduplicated
/// - Caller must call writeState() before switching to a different prefix
const std = @import("std");
const Allocator = std.mem.Allocator;

const TagRecordsParseState = @import("TagRecordsParseState.zig");

const Self = @This();

streamIDs: std.ArrayList(u128) = .empty,
state: *TagRecordsParseState,
prevState: *TagRecordsParseState,

pub fn init(alloc: Allocator) !Self {
    const state = try TagRecordsParseState.init(alloc);
    errdefer state.deinit(alloc);
    const prevState = try TagRecordsParseState.init(alloc);
    errdefer prevState.deinit(alloc);

    return .{
        .state = state,
        .prevState = prevState,
    };
}

pub fn deinit(self: *Self, alloc: Allocator) void {
    self.streamIDs.deinit(alloc);
    self.state.deinit(alloc);
    self.prevState.deinit(alloc);
}

pub fn writeState(self: *Self, alloc: Allocator, buf: *std.ArrayList(u8), target: *std.ArrayList([]const u8)) !void {
    if (self.streamIDs.items.len == 0) {
        return;
    }

    std.mem.sortUnstable(u128, self.streamIDs.items, {}, std.sort.asc(u128));
    self.removeDuplicatedStreams();

    const bound = TagRecordsParseState.encodeRecordBound(self.prevState.tag, self.streamIDs.items.len);
    try buf.ensureUnusedCapacity(alloc, bound);
    const slice = buf.unusedCapacitySlice();
    const recordLen = TagRecordsParseState.encodeRecord(
        slice,
        self.prevState.tenantID,
        self.prevState.tag,
        self.streamIDs.items,
    );
    buf.items.len += recordLen;

    self.streamIDs.clearRetainingCapacity();
    try target.append(alloc, slice[0..recordLen]);
}

fn removeDuplicatedStreams(self: *Self) void {
    if (self.streamIDs.items.len < 2) return;

    var write: usize = 1;
    var prev = self.streamIDs.items[0];

    var i: usize = 1;
    while (i < self.streamIDs.items.len) : (i += 1) {
        const v = self.streamIDs.items[i];
        if (v != prev) {
            self.streamIDs.items[write] = v;
            write += 1;
            prev = v;
        }
    }
    self.streamIDs.items.len = write;
}

pub fn statesPrefixEqual(self: *const Self) bool {
    if (!std.mem.eql(u8, self.state.tenantID, self.prevState.tenantID)) return false;

    if (!self.state.tag.eql(self.prevState.tag)) return false;

    return true;
}

pub fn moveParsedState(self: *Self, alloc: Allocator) !void {
    try self.streamIDs.appendSlice(alloc, self.state.streamIDs.items);
    const tmp = self.state;
    self.state = self.prevState;
    self.prevState = tmp;
}

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
        var m = try Self.init(alloc);
        defer m.deinit(alloc);
        try m.streamIDs.appendSlice(alloc, case.input);

        m.removeDuplicatedStreams();

        try testing.expectEqualSlices(u128, m.streamIDs.items, case.expected);
    }
}

const Field = @import("../lines.zig").Field;

pub fn createTagRecord(
    alloc: Allocator,
    tenantID: []const u8,
    tag: Field,
    streamIDs: []const u128,
) ![]const u8 {
    const bufSize = TagRecordsParseState.encodeRecordBound(tag, streamIDs.len);
    const buf = try alloc.alloc(u8, bufSize);
    const recordLen = TagRecordsParseState.encodeRecord(buf, tenantID, tag, streamIDs);
    return buf[0..recordLen];
}

test "statesPrefixEqual" {
    const Case = struct {
        tenantA: []const u8,
        tenantB: []const u8,
        tagA: Field,
        tagB: Field,
        expected: bool,
    };
    const cases = [_]Case{
        .{
            .tenantA = "tenant1",
            .tenantB = "tenant1",
            .tagA = .{ .key = "env", .value = "prod" },
            .tagB = .{ .key = "env", .value = "prod" },
            .expected = true,
        },
        .{
            .tenantA = "tenant1",
            .tenantB = "tenant2",
            .tagA = .{ .key = "env", .value = "prod" },
            .tagB = .{ .key = "env", .value = "prod" },
            .expected = false,
        },
        .{
            .tenantA = "tenant1",
            .tenantB = "tenant1",
            .tagA = .{ .key = "env", .value = "prod" },
            .tagB = .{ .key = "env", .value = "dev" },
            .expected = false,
        },
    };

    for (cases) |case| {
        const alloc = testing.allocator;
        var m = try Self.init(alloc);
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
    var m = try Self.init(alloc);
    defer m.deinit(alloc);

    const tag = Field{ .key = "app", .value = "web" };
    const streamIDs = &[_]u128{ 100, 200, 300 };
    const record = try createTagRecord(alloc, "tenant1", tag, streamIDs);
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
    var m = try Self.init(alloc);
    defer m.deinit(alloc);

    var buf = std.ArrayList(u8){};
    defer buf.deinit(alloc);
    var target = std.ArrayList([]const u8){};
    defer target.deinit(alloc);

    try m.writeState(alloc, &buf, &target);

    try testing.expectEqual(@as(usize, 0), target.items.len);
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
        var m = try Self.init(alloc);
        defer m.deinit(alloc);

        const record = try createTagRecord(alloc, "tenant1", case.tag, case.recordStreamIDs);
        defer alloc.free(record);

        try m.prevState.setup(record);
        try m.streamIDs.appendSlice(alloc, case.initial);

        var buf = std.ArrayList(u8){};
        defer buf.deinit(alloc);
        var target = std.ArrayList([]const u8){};
        defer target.deinit(alloc);

        try m.writeState(alloc, &buf, &target);

        try testing.expectEqual(@as(usize, 1), target.items.len);
        try testing.expectEqual(@as(usize, 0), m.streamIDs.items.len);

        var verifyState = try TagRecordsParseState.init(alloc);
        defer verifyState.deinit(alloc);
        try verifyState.setup(target.items[0]);
        try verifyState.parseStreamIDs(alloc);

        try testing.expectEqualSlices(u128, case.expected, verifyState.streamIDs.items);
    }
}
