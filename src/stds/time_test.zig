const std = @import("std");

pub const parseDurationNs = @import("time.zig").parseDurationNs;
pub const ParserDurationError = @import("time.zig").ParserDurationError;
pub const parseTimestampISO8601 = @import("time.zig").parseTimestampISO8601;

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
        .{ .raw = "", .expectedErr = ParserDurationError.InvalidDuration },
        .{ .raw = "9", .expectedErr = ParserDurationError.InvalidDuration },
        .{ .raw = "s", .expectedErr = ParserDurationError.InvalidDuration },
        .{ .raw = "12x", .expectedErr = ParserDurationError.InvalidDuration },
        .{ .raw = "1ms", .expectedErr = ParserDurationError.InvalidDuration },
        .{ .raw = "18446744073s", .expected = 18446744073000000000 },
        .{ .raw = "18446744074s", .expectedErr = ParserDurationError.DurationLimit },
        .{ .raw = "307445734m", .expected = 18446744040000000000 },
        .{ .raw = "307445735m", .expectedErr = ParserDurationError.DurationLimit },
        .{ .raw = "5124095h", .expected = 18446742000000000000 },
        .{ .raw = "5124096h", .expectedErr = ParserDurationError.DurationLimit },
        .{ .raw = "213503d", .expected = 18446659200000000000 },
        .{ .raw = "213504d", .expectedErr = ParserDurationError.DurationLimit },
        .{ .raw = "18446744073709551615s", .expectedErr = ParserDurationError.DurationLimit },
        .{ .raw = "18446744073709551616s", .expectedErr = ParserDurationError.DurationLimit },
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

test "parseTimestampISO8601 requires complete in-range timestamps" {
    const Case = struct {
        value: []const u8,
        expected: ?i64,
    };

    const cases = [_]Case{
        .{ .value = "389", .expected = null },
        .{ .value = "2011", .expected = null },
        .{ .value = "2011-04-19", .expected = null },
        .{ .value = "2011-04-19T03:44", .expected = null },
        .{ .value = "3890-01-01T00:00:00Z", .expected = null },
        .{ .value = "2011-04-19T03:44:01Z", .expected = 1303184641000000000 },
        .{ .value = "2011-04-19T03:44:01.123456789Z", .expected = 1303184641123456789 },
        .{ .value = "20240224T154944Z", .expected = 1708789784000000000 },
    };

    for (cases) |case| {
        const actual = parseTimestampISO8601(case.value);
        if (case.expected) |expected| {
            try testing.expectEqual(expected, actual.?);
        } else {
            try testing.expectEqual(null, actual);
        }
    }
}
