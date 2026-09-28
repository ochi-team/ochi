const std = @import("std");

const zeit = @import("zeit");

const Query = @import("Query.zig");
const QuerySet = @import("Parser.zig").QuerySet;
const Translator = @import("Translator.zig");
const TranslateError = Translator.TranslateError;
const translateTimeValue = Translator.translateTimeValue;

const testing = std.testing;

test "translateTimeValue distinguishes duration parse limits from arithmetic overflow" {
    const Case = struct {
        raw: []const u8,
        nowNs: u64,
        expected: ?u64 = null,
        expectedErr: ?TranslateError = null,
    };
    const nowNs: u64 = 1_000_000_000_000;

    const cases = [_]Case{
        .{ .raw = "18446744073s", .nowNs = 0, .expected = 18446744073000000000 },
        .{ .raw = "18446744074s", .nowNs = 0, .expectedErr = TranslateError.InvalidDuration },
        .{ .raw = "307445734m", .nowNs = 0, .expected = 18446744040000000000 },
        .{ .raw = "307445735m", .nowNs = 0, .expectedErr = TranslateError.InvalidDuration },
        .{ .raw = "5124095h", .nowNs = 0, .expected = 18446742000000000000 },
        .{ .raw = "5124096h", .nowNs = 0, .expectedErr = TranslateError.InvalidDuration },
        .{ .raw = "213503d", .nowNs = 0, .expected = 18446659200000000000 },
        .{ .raw = "213504d", .nowNs = 0, .expectedErr = TranslateError.InvalidDuration },
        .{ .raw = "18446744073709551615s", .nowNs = 0, .expectedErr = TranslateError.InvalidDuration },
        .{ .raw = "18446744073709551616s", .nowNs = 0, .expectedErr = TranslateError.InvalidDuration },
        .{ .raw = "-18446744242s", .nowNs = nowNs, .expectedErr = TranslateError.InvalidDuration },
        .{ .raw = "1s", .nowNs = std.math.maxInt(u64), .expectedErr = TranslateError.OverflowDuration },
        .{ .raw = "-1s", .nowNs = 0, .expectedErr = TranslateError.DurationBeforeUnixEpoch },
        .{ .raw = "1s", .nowNs = nowNs, .expected = nowNs + std.time.ns_per_s },
        .{ .raw = "2m", .nowNs = nowNs, .expected = nowNs + 2 * std.time.ns_per_min },
        .{ .raw = "3h", .nowNs = nowNs, .expected = nowNs + 3 * std.time.ns_per_hour },
        .{ .raw = "4d", .nowNs = nowNs, .expected = nowNs + 4 * std.time.ns_per_day },
        .{ .raw = "0s", .nowNs = nowNs, .expected = nowNs },
        .{ .raw = "-1s", .nowNs = nowNs, .expected = nowNs - std.time.ns_per_s },
        .{ .raw = "", .nowNs = nowNs, .expectedErr = TranslateError.InvalidDuration },
        .{ .raw = "-", .nowNs = nowNs, .expectedErr = TranslateError.InvalidDuration },
        .{ .raw = "9", .nowNs = nowNs, .expectedErr = TranslateError.InvalidDuration },
        .{ .raw = "s", .nowNs = nowNs, .expectedErr = TranslateError.InvalidDuration },
        .{ .raw = "12x", .nowNs = nowNs, .expectedErr = TranslateError.InvalidDuration },
        .{ .raw = "1ms", .nowNs = nowNs, .expectedErr = TranslateError.InvalidDuration },
    };

    for (cases) |case| {
        const actual = translateTimeValue(.{ .duration = case.raw }, case.nowNs);
        if (case.expectedErr) |err| {
            try testing.expectError(err, actual);
            continue;
        }

        try testing.expectEqual(case.expected.?, try actual);
    }
}

test "Translator.query" {
    const allocator = testing.allocator;
    const now: u64 = 1_000_000_000_000;

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
                .tags = .{ .equalOp = .{ &.{ .literal = "env" }, &.{ .literal = "prod" } } },
                .query = null,
                .pipes = .empty,
            },
            .nowNs = now,
            .expectedQuery = .{
                .start = now - (5 * std.time.ns_per_min),
                .end = now + (30 * std.time.ns_per_s),
                .tagsExpr = &.{
                    .predicate = .{
                        .key = "env",
                        .value = "prod",
                        .op = .equal,
                    },
                },
                .fieldsExpr = null,
            },
        },
        .{
            // translates absolute timestamp 1970
            .qset = .{
                .timeRange = .{ .{ .timestamp = "1970-01-01T00:00:00.000Z" }, .now },
                .tags = .{ .equalOp = .{ &.{ .literal = "env" }, &.{ .literal = "prod" } } },
                .query = null,
                .pipes = .empty,
            },
            .nowNs = now,
            .expectedQuery = .{
                // 1970 year is 0
                .start = 0,
                .end = now,
                .tagsExpr = &.{
                    .predicate = .{
                        .key = "env",
                        .value = "prod",
                        .op = .equal,
                    },
                },
                .fieldsExpr = null,
            },
        },
        .{
            // translates ISO8601 timestamp bounds
            .qset = .{
                .timeRange = .{ .{ .timestamp = startTs }, .{ .timestamp = endTs } },
                .tags = .{ .equalOp = .{ &.{ .literal = "env" }, &.{ .literal = "prod" } } },
                .query = null,
                .pipes = .empty,
            },
            .nowNs = now,
            .expectedQuery = .{
                .start = expectedStart,
                .end = expectedEnd,
                .tagsExpr = &.{
                    .predicate = .{
                        .key = "env",
                        .value = "prod",
                        .op = .equal,
                    },
                },
                .fieldsExpr = null,
            },
        },
        .{
            // translates complex tags and query filter expressions
            .qset = .{
                .timeRange = .{ .{ .duration = "-1m" }, .{ .now = {} } },
                .tags = .{ .grouping = &.{ .andOp = .{
                    &.{ .equalOp = .{ &.{ .literal = "env" }, &.{ .literal = "prod" } } },
                    &.{ .notEqualOp = .{ &.{ .literal = "service" }, &.{ .literal = "api" } } },
                } } },
                .query = .{ .orOp = .{
                    &.{ .matchRegexOp = .{ &.{ .literal = "message" }, &.{ .literal = "err.*" } } },
                    &.{ .andOp = .{
                        &.{ .notMatchRegexOp = .{ &.{ .literal = "path" }, &.{ .literal = "/health" } } },
                        &.{ .equalOp = .{ &.{ .literal = "status" }, &.{ .literal = "500" } } },
                    } },
                } },
                .pipes = .empty,
            },
            .nowNs = now,
            .expectedQuery = .{
                .start = now - std.time.ns_per_min,
                .end = now,
                .tagsExpr = &.{ .andOp = .{
                    &.{ .predicate = .{ .key = "env", .value = "prod", .op = .equal } },
                    &.{ .predicate = .{ .key = "service", .value = "api", .op = .notEqual } },
                } },
                .fieldsExpr = &.{ .orOp = .{
                    &.{ .predicate = .{ .key = "message", .value = "err.*", .op = .matchRegex } },
                    &.{ .andOp = .{
                        &.{ .predicate = .{ .key = "path", .value = "/health", .op = .notMatchRegex } },
                        &.{ .predicate = .{ .key = "status", .value = "500", .op = .equal } },
                    } },
                } },
            },
        },
        // nullable tags
        .{
            .qset = .{
                .timeRange = .{ .{ .duration = "-1m" }, .{ .now = {} } },
                .tags = null,
                .query = .{ .orOp = .{
                    &.{ .matchRegexOp = .{ &.{ .literal = "message" }, &.{ .literal = "err.*" } } },
                    &.{ .andOp = .{
                        &.{ .notMatchRegexOp = .{ &.{ .literal = "path" }, &.{ .literal = "/health" } } },
                        &.{ .equalOp = .{ &.{ .literal = "status" }, &.{ .literal = "500" } } },
                    } },
                } },
                .pipes = .empty,
            },
            .nowNs = now,
            .expectedQuery = .{
                .start = now - std.time.ns_per_min,
                .end = now,
                .tagsExpr = null,
                .fieldsExpr = &.{ .orOp = .{
                    &.{ .predicate = .{ .key = "message", .value = "err.*", .op = .matchRegex } },
                    &.{ .andOp = .{
                        &.{ .predicate = .{ .key = "path", .value = "/health", .op = .notMatchRegex } },
                        &.{ .predicate = .{ .key = "status", .value = "500", .op = .equal } },
                    } },
                } },
            },
        },
        .{
            // returns invalid duration for unknown unit
            .qset = .{
                .timeRange = .{ .{ .duration = "-5x" }, .{ .now = {} } },
                .tags = .{ .equalOp = .{ &.{ .literal = "env" }, &.{ .literal = "prod" } } },
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
                .tags = .{ .equalOp = .{ &.{ .literal = "env" }, &.{ .literal = "prod" } } },
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
                .tags = .{ .equalOp = .{ &.{ .literal = "env" }, &.{ .literal = "prod" } } },
                .query = null,
                .pipes = .empty,
            },
            .nowNs = now,
            .expectedErr = TranslateError.InvalidRange,
        },
        .{
            // returns expected literal when predicate left side is not literal
            .qset = .{
                .timeRange = .{ .{ .duration = "-5m" }, .{ .now = {} } },
                .tags = .{ .equalOp = .{ &.{ .grouping = &.{ .literal = "env" } }, &.{ .literal = "prod" } } },
                .query = null,
                .pipes = .empty,
            },
            .nowNs = now,
            .expectedErr = TranslateError.ExpectedLiteral,
        },
        .{
            // returns expected literal when predicate right side is not literal
            .qset = .{
                .timeRange = .{ .{ .duration = "-5m" }, .{ .now = {} } },
                .tags = .{ .equalOp = .{ &.{ .literal = "env" }, &.{ .literal = "prod" } } },
                .query = .{ .equalOp = .{ &.{ .literal = "service" }, &.{ .grouping = &.{ .literal = "api" } } } },
                .pipes = .empty,
            },
            .nowNs = now,
            .expectedErr = TranslateError.ExpectedLiteral,
        },
        .{
            // returns unexpected tag expression when tags root is not a filter expression
            .qset = .{
                .timeRange = .{ .{ .duration = "-5m" }, .{ .now = {} } },
                .tags = .{ .literal = "env" },
                .query = null,
                .pipes = .empty,
            },
            .nowNs = now,
            .expectedErr = TranslateError.UnexpectedTagExpression,
        },
        .{
            // timestamp overflow
            .qset = .{
                .timeRange = .{ .{ .timestamp = "560" }, .{ .now = {} } },
                .tags = .{ .equalOp = .{ &.{ .literal = "env" }, &.{ .literal = "prod" } } },
                .query = null,
                .pipes = .empty,
            },
            .nowNs = now,
            .expectedErr = TranslateError.InvalidTimestamp,
        },
    };

    for (cases) |case| {
        var translator: Translator = .{};
        defer translator.deinit(allocator);

        const got = translator.query(allocator, case.qset, case.nowNs);
        if (case.expectedErr) |expectedErr| {
            try testing.expectError(expectedErr, got);
        } else {
            try testing.expectEqualDeep(case.expectedQuery.?, try got);
        }
    }
}
