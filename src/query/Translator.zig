const std = @import("std");
const Allocator = std.mem.Allocator;

pub const zeit = @import("zeit");

const Query = @import("Query.zig");
const TimeRangeExpression = @import("Parser.zig").TimeRangeExpression;
const TimeValue = @import("Parser.zig").TimeValue;
const QuerySet = @import("Parser.zig").QuerySet;
const Expression = @import("Parser.zig").Expression;

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

test "Translator.query" {
    const allocator = testing.allocator;
    const now: u64 = 100;

    const startTs = "2024-01-10T00:00:00Z";
    const endTs = "2024-01-10T01:00:00Z";
    const expectedStart: u64 = @intCast((try zeit.Time.fromISO8601(startTs)).instant().timestamp);
    const expectedEnd: u64 = @intCast((try zeit.Time.fromISO8601(endTs)).instant().timestamp);

    const Case = struct {
        qset: QuerySet,
        nowNs: u64,
        expectedQuery: ?Query = null,
        expectedErr: ?TranslateError = null,
    };

    const cases = [_]Case{
        .{
            // translates relative duration range
            .qset = .{
                .timeRange = .{ .{ .duration = "-5m" }, .{ .duration = "30s" } },
                .tags = .{ .literal = "env" },
                .query = null,
                .pipes = .empty,
            },
            .nowNs = now,
            .expectedQuery = .{
                .startTimeNs = now - (5 * std.time.ns_per_min),
                .endTimeNs = now + (30 * std.time.ns_per_s),
                .tagsExpr = null,
                .fieldsExpr = null,
            },
        },
        .{
            // translates ISO8601 timestamp bounds
            .qset = .{
                .timeRange = .{ .{ .timestamp = startTs }, .{ .timestamp = endTs } },
                .tags = .{ .literal = "env" },
                .query = null,
                .pipes = .empty,
            },
            .nowNs = now,
            .expectedQuery = .{
                .startTimeNs = expectedStart,
                .endTimeNs = expectedEnd,
                .tagsExpr = null,
                .fieldsExpr = null,
            },
        },
        .{
            // returns invalid duration for unknown unit
            .qset = .{
                .timeRange = .{ .{ .duration = "-5x" }, .{ .now = {} } },
                .tags = .{ .literal = "env" },
                .query = null,
                .pipes = .empty,
            },
            .nowNs = now,
            .expectedErr = TranslateError.InvalidDuration,
        },
        .{
            // returns invalid timestamp for malformed input
            .qset = .{
                .timeRange = .{ .{ .timestamp = "not-a-ts" }, .{ .now = {} } },
                .tags = .{ .literal = "env" },
                .query = null,
                .pipes = .empty,
            },
            .nowNs = now,
            .expectedErr = TranslateError.InvalidTimestamp,
        },
        .{
            // returns invalid range when start is not before end
            .qset = .{
                .timeRange = .{ .{ .now = {} }, .{ .duration = "-1m" } },
                .tags = .{ .literal = "env" },
                .query = null,
                .pipes = .empty,
            },
            .nowNs = now,
            .expectedErr = TranslateError.InvalidRange,
        },
    };

    var translator: Translator = .{};
    for (cases) |case| {
        if (case.expectedErr) |expectedErr| {
            try testing.expectError(expectedErr, translator.query(allocator, case.qset, case.nowNs));
            continue;
        }

        const got = try translator.query(allocator, case.qset, case.nowNs);
        try testing.expectEqualDeep(case.expectedQuery.?, got);
    }
}
