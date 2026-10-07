const std = @import("std");

const zeit = @import("zeit");

pub const ParserDurationError = error{
    InvalidDuration,
    DurationLimit,
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
    const value = std.fmt.parseInt(u64, valueRaw, 10) catch |err| switch (err) {
        error.Overflow => return ParserDurationError.DurationLimit,
        else => return ParserDurationError.InvalidDuration,
    };

    const multiplier: u64 = switch (unit) {
        's' => std.time.ns_per_s,
        'm' => std.time.ns_per_min,
        'h' => std.time.ns_per_hour,
        'd' => std.time.ns_per_day,
        else => return ParserDurationError.InvalidDuration,
    };

    return std.math.mul(u64, value, multiplier) catch ParserDurationError.DurationLimit;
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

pub fn parseTimestampISO8601(v: []const u8) ?i64 {
    // TODO: we should rather do parsing in-place instead of validating and then parsing,
    // it simply blows the amount of code
    if (!hasCompleteTimestampISO8601Shape(v)) return null;

    const time = zeit.Time.fromISO8601(v) catch return null;
    return std.math.cast(i64, time.instant().timestamp);
}

fn hasCompleteTimestampISO8601Shape(v: []const u8) bool {
    if (v.len >= 19 and
        isDigits(v[0..4]) and
        v[4] == '-' and
        isDigits(v[5..7]) and
        v[7] == '-' and
        isDigits(v[8..10]) and
        isDateTimeSeparator(v[10]) and
        isDigits(v[11..13]) and
        v[13] == ':' and
        isDigits(v[14..16]) and
        v[16] == ':' and
        isDigits(v[17..19]))
    {
        return hasTimezoneSuffix(v[19..]);
    }

    if (v.len >= 15 and
        isDigits(v[0..8]) and
        isDateTimeSeparator(v[8]) and
        isDigits(v[9..15]))
    {
        return hasTimezoneSuffix(v[15..]);
    }

    return false;
}

fn hasTimezoneSuffix(v: []const u8) bool {
    if (v.len == 0) return true;

    switch (v[0]) {
        'Z' => return v.len == 1,
        '+', '-' => return hasTimezoneOffset(v),
        '.' => {
            var i: usize = 1;
            while (i < v.len and std.ascii.isDigit(v[i])) : (i += 1) {}
            const fracLen = i - 1;
            if (fracLen == 0 or fracLen > 9) return false;
            if (i == v.len) return true;
            if (v[i] == 'Z') return i + 1 == v.len;
            if (v[i] == '+' or v[i] == '-') return hasTimezoneOffset(v[i..]);
            return false;
        },
        else => return false,
    }
}

fn hasTimezoneOffset(v: []const u8) bool {
    return (v.len == 6 and isSign(v[0]) and isDigits(v[1..3]) and v[3] == ':' and isDigits(v[4..6])) or
        (v.len == 5 and isSign(v[0]) and isDigits(v[1..5]));
}

fn isDateTimeSeparator(ch: u8) bool {
    return ch == 'T' or ch == ' ';
}

fn isSign(ch: u8) bool {
    return ch == '+' or ch == '-';
}

fn isDigits(v: []const u8) bool {
    for (v) |ch| {
        if (!std.ascii.isDigit(ch)) return false;
    }
    return true;
}

const testing = std.testing;
