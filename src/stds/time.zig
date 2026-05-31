const std = @import("std");

const zeit = @import("zeit");

pub const ParserDurationError = error{
    InvalidDuration,
};

pub const TimestampError = error{
    InvalidTimestamp,
};

pub fn parseDurationNs(raw: []const u8) ParserDurationError!u64 {
    if (raw.len < 2) {
        return ParserDurationError.InvalidDuration;
    }

    const unit = raw[raw.len - 1];
    const valueRaw = raw[0 .. raw.len - 1];
    const value = std.fmt.parseInt(u64, valueRaw, 10) catch return ParserDurationError.InvalidDuration;

    const multiplier: u64 = switch (unit) {
        's' => std.time.ns_per_s,
        'm' => std.time.ns_per_min,
        'h' => std.time.ns_per_hour,
        'd' => std.time.ns_per_day,
        else => return ParserDurationError.InvalidDuration,
    };

    return std.math.mul(u64, value, multiplier) catch ParserDurationError.InvalidDuration;
}

pub fn parseTimestamp(ts: []const u8) TimestampError!u64 {
    const timestamp = zeit.Time.fromISO8601(ts) catch return error.InvalidTimestamp;
    const ns = timestamp.instant().timestamp;
    if (ns <= 0) {
        return error.InvalidTimestamp;
    }
    if (ns > std.math.maxInt(u64)) {
        return error.InvalidTimestamp;
    }
    return @intCast(ns);
}

const testing = std.testing;

test "parseDurationNs" {
    const Case = struct {
        raw: []const u8,
        expected: u64 = 0,
        expectedErr: ?ParserDurationError = null,
    };

    const cases = [_]Case{
        .{ .raw = "1s", .expected = std.time.ns_per_s },
        .{ .raw = "2m", .expected = 2 * std.time.ns_per_min },
        .{ .raw = "3h", .expected = 3 * std.time.ns_per_hour },
        .{ .raw = "4d", .expected = 4 * std.time.ns_per_day },
        .{ .raw = "0s", .expected = 0 },
        .{ .raw = "18446744073s", .expected = 18446744073000000000 },
        .{ .raw = "18446744074s", .expectedErr = ParserDurationError.InvalidDuration },
        .{ .raw = "", .expectedErr = ParserDurationError.InvalidDuration },
        .{ .raw = "9", .expectedErr = ParserDurationError.InvalidDuration },
        .{ .raw = "s", .expectedErr = ParserDurationError.InvalidDuration },
        .{ .raw = "12x", .expectedErr = ParserDurationError.InvalidDuration },
        .{ .raw = "1ms", .expectedErr = ParserDurationError.InvalidDuration },
    };

    for (cases) |case| {
        if (case.expectedErr) |err| {
            try testing.expectError(err, parseDurationNs(case.raw));
            continue;
        }

        const actual = try parseDurationNs(case.raw);
        try testing.expectEqual(case.expected, actual);
    }
}
