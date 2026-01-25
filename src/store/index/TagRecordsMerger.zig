const std = @import("std");
const Allocator = std.mem.Allocator;

const encoding = @import("encoding");
const Encoder = encoding.Encoder;

const maxTenantIDLen = @import("../lines.zig").maxTenantIDLen;

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

    const encodePrefixBound = self.prevState.encodePrefixBound();
    const bound = encodePrefixBound + self.streamIDs.items.len * maxTenantIDLen;
    try buf.ensureUnusedCapacity(alloc, bound);
    const slice = buf.unusedCapacitySlice();
    self.prevState.encodePrefix(slice);
    var enc = Encoder.init(slice[encodePrefixBound..]);
    for (self.streamIDs.items) |sid| {
        enc.writeInt(u128, sid);
    }
    buf.items.len += bound;

    self.streamIDs.clearRetainingCapacity();
    try target.append(alloc, slice[0..bound]);
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
