const std = @import("std");
const Allocator = std.mem.Allocator;

pub const zeit = @import("zeit");

const Query = @import("Query.zig");
const TimeRangeExpression = @import("Parser.zig").TimeRangeExpression;
const TimeValue = @import("Parser.zig").TimeValue;
const QuerySet = @import("Parser.zig").QuerySet;

pub const TranslateError = error{
    InvalidDuration,
    InvalidTimestamp,
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

    return q;
}

fn translateRange(range: TimeRangeExpression, nowNs: u64) TranslateError!struct { startTimeNs: u64, endTimeNs: u64 } {
    _ = range;
    _ = nowNs;
    return .{ .startTimeNs = 0, .endTimeNs = 0 };
}

fn translateTimeValue(value: TimeValue, nowNs: u64) TranslateError!u64 {
    switch (value) {
        .now => return nowNs,
        .duration => |d| {
            if (d[0] == '-') {
                return nowNs - std.time.parseDuration(d[1..]);
            } else {
                return nowNs + std.time.parseDuration(d);
            }
        },
        .timestamp => |ts| {
            const timestamp = zeit.Time.fromISO8601(ts) catch return TranslateError.InvalidTimestamp;
            return @intCast(timestamp.instant().timestamp);
        },
    }

    return 0;
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
