const std = @import("std");
const Allocator = std.mem.Allocator;

pub const zeit = @import("zeit");

const Query = @import("Query.zig");
const TimeRangeExpression = @import("Parser.zig").TimeRangeExpression;
const TimeValue = @import("Parser.zig").TimeValue;
const QuerySet = @import("Parser.zig").QuerySet;

pub const TranslateError = error{
    InvalidDuration,
    OverflowDuration,
    InvalidTimestamp,
    InvalidRange,
};

const Translator = @This();

pub fn query(self: *Translator, allocator: Allocator, qset: QuerySet, nowNs: u64) TranslateError!Query {
    _ = self;
    _ = allocator;

    const range = try translateRange(qset.timeRange, nowNs);
    const q: Query = .{
        .startTimeNs = range.startTimeNs,
        .endTimeNs = range.endTimeNs,
        .tagsExpr = null,
        .fieldsExpr = null,
    };

    if (q.startTimeNs >= q.endTimeNs) {
        return TranslateError.InvalidRange;
    }

    return q;
}

fn translateRange(range: TimeRangeExpression, nowNs: u64) TranslateError!struct { startTimeNs: u64, endTimeNs: u64 } {
    return .{
        .startTimeNs = try translateTimeValue(range[0], nowNs),
        .endTimeNs = try translateTimeValue(range[1], nowNs),
    };
}

fn translateTimeValue(value: TimeValue, nowNs: u64) TranslateError!u64 {
    switch (value) {
        .now => return nowNs,
        .duration => |d| {
            if (d.len == 0) {
                return TranslateError.InvalidDuration;
            }

            if (d[0] == '-') {
                const dur = try translateTimeDuration(d[1..]);
                return std.math.sub(u64, nowNs, dur) catch TranslateError.OverflowDuration;
            } else {
                const dur = try translateTimeDuration(d);
                return std.math.add(u64, nowNs, dur) catch TranslateError.InvalidDuration;
            }
        },
        .timestamp => |ts| {
            const timestamp = zeit.Time.fromISO8601(ts) catch return TranslateError.InvalidTimestamp;
            const ns = timestamp.instant().timestamp;
            if (ns <= 0) {
                return TranslateError.InvalidTimestamp;
            }
            return @intCast(ns);
        },
    }
}

fn translateTimeDuration(raw: []const u8) TranslateError!u64 {
    if (raw.len < 2) {
        return TranslateError.InvalidDuration;
    }

    const unit = raw[raw.len - 1];
    const valueRaw = raw[0 .. raw.len - 1];
    const value = std.fmt.parseInt(u64, valueRaw, 10) catch return TranslateError.InvalidDuration;

    const multiplier: u64 = switch (unit) {
        's' => std.time.ns_per_s,
        'm' => std.time.ns_per_min,
        'h' => std.time.ns_per_hour,
        'd' => std.time.ns_per_day,
        else => return TranslateError.InvalidDuration,
    };

    return std.math.mul(u64, value, multiplier) catch TranslateError.InvalidDuration;
}

const testing = std.testing;

test "Translator.query translates query range values" {
    const allocator = testing.allocator;
    const nowNs: u64 = 10 * std.time.ns_per_min;

    var translator: Translator = .{};

    const tagExpr: @import("Parser.zig").Expression = .{ .literal = "env" };

    const querySet: QuerySet = .{
        .timeRange = .{ .{ .duration = "-5m" }, .{ .duration = "+30s" } },
        .tags = tagExpr,
        .query = null,
        .pipes = .empty,
    };

    const got = try translator.query(allocator, querySet, nowNs);
    const want: Query = .{
        .startTimeNs = nowNs - (5 * std.time.ns_per_min),
        .endTimeNs = nowNs + (30 * std.time.ns_per_s),
        .tagsExpr = null,
        .fieldsExpr = null,
    };

    try testing.expectEqualDeep(want, got);
}

test "Translator.query translates ISO8601 timestamp bounds" {
    const allocator = testing.allocator;

    var translator: Translator = .{};

    const startTs = "2024-01-10T00:00:00Z";
    const endTs = "2024-01-10T01:00:00Z";

    const tagExpr: @import("Parser.zig").Expression = .{ .literal = "service" };
    const querySet: QuerySet = .{
        .timeRange = .{ .{ .timestamp = startTs }, .{ .timestamp = endTs } },
        .tags = tagExpr,
        .query = null,
        .pipes = .empty,
    };

    const expectedStart: u64 = @intCast((try zeit.Time.fromISO8601(startTs)).instant().timestamp);
    const expectedEnd: u64 = @intCast((try zeit.Time.fromISO8601(endTs)).instant().timestamp);

    const got = try translator.query(allocator, querySet, 0);
    const want: Query = .{
        .startTimeNs = expectedStart,
        .endTimeNs = expectedEnd,
        .tagsExpr = null,
        .fieldsExpr = null,
    };

    try testing.expectEqualDeep(want, got);
}

test "Translator.query returns translate errors for invalid inputs" {
    const allocator = testing.allocator;

    const Case = struct {
        qset: QuerySet,
        nowNs: u64,
        expectedErr: TranslateError,
    };

    const tagExpr: @import("Parser.zig").Expression = .{ .literal = "env" };
    const cases = [_]Case{
        .{
            .qset = .{
                .timeRange = .{ .{ .duration = "-5x" }, .{ .now = {} } },
                .tags = tagExpr,
                .query = null,
                .pipes = .empty,
            },
            .nowNs = 100,
            .expectedErr = TranslateError.InvalidDuration,
        },
        .{
            .qset = .{
                .timeRange = .{ .{ .timestamp = "not-a-ts" }, .{ .now = {} } },
                .tags = tagExpr,
                .query = null,
                .pipes = .empty,
            },
            .nowNs = 100,
            .expectedErr = TranslateError.InvalidTimestamp,
        },
        .{
            .qset = .{
                .timeRange = .{ .{ .now = {} }, .{ .duration = "-1m" } },
                .tags = tagExpr,
                .query = null,
                .pipes = .empty,
            },
            .nowNs = 10 * std.time.ns_per_min,
            .expectedErr = TranslateError.InvalidRange,
        },
    };

    var translator: Translator = .{};
    for (cases) |case| {
        try testing.expectError(case.expectedErr, translator.query(allocator, case.qset, case.nowNs));
    }
}
