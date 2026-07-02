/// Logql is a Logs Ochi Query Language,
/// translates a query to ochi query API
const std = @import("std");
const Allocator = std.mem.Allocator;
const Io = std.Io;

const zeit = @import("zeit");

const Scanner = @import("Scanner.zig");
const Parser = @import("Parser.zig");
const Translator = @import("Translator.zig");
const ErrorReporter = @import("ErrorReporter.zig");

const Query = @import("Query.zig");

pub const QueryError = error{
    EmptyQuery,
    QueryTooLong,
};

const Loql = @This();

scanner: Scanner = .{},
parser: Parser = .{},
translator: Translator = .{},

pub fn deinit(self: *Loql, allocator: Allocator) void {
    self.scanner.deinit(allocator);
    self.parser.deinit(allocator);
    self.translator.deinit(allocator);
}

pub const maxQueryLength = 2048;
// -128 for timestamps, in case they are passed as short duration,
// but we need to parse as full timestamps (-64)
// and a little gap for safety (-64)
pub const maxQueryBodyLength = maxQueryLength - 128;
// TODO: it must accept a reader probably, not a query string
pub fn translateQuery(
    self: *Loql,
    allocator: Allocator,
    reporter: *ErrorReporter,
    fullQueryStr: []const u8,
    nowNs: u64,
) !Query {
    const trimmedQueryStr = std.mem.trim(u8, fullQueryStr, " \n\t");
    if (trimmedQueryStr.len == 0) {
        return error.EmptyQuery;
    }
    if (trimmedQueryStr.len > maxQueryLength) {
        return error.QueryTooLong;
    }

    // TODO: validate whether it's a syntax error and return report content
    // or unknown error
    try self.scanner.scan(allocator, trimmedQueryStr, reporter);
    const expr = try self.parser.querySet(allocator, self.scanner.tokens.items, reporter);
    const query = try self.translator.query(allocator, expr, nowNs);

    return query;
}

const testing = std.testing;

// TODO: setup benchmarks, it requires:
// 1. add to a test runner prefix :benchmark to run them on flag
// 2. setup a first bench using zbench
// 3. report benchmarks as json
// 4. translate into BMF: https://bencher.dev/docs/reference/bencher-metric-format/
// 5. git them and compare during bench runs
// follow zig-otel for potential implementation: https://github.com/zig-o11y/opentelemetry-sdk/issues/76
test "translateQuery" {
    const allocator = testing.allocator;
    const io = testing.io;

    const now: u64 = @intCast(Io.Timestamp.now(io, .real).nanoseconds);
    const fiveMinutesAgo = now - 5 * std.time.ns_per_min;
    const fixedTs = "2024-01-10T00:00:00Z";
    const fixedTsNs: u64 = @intCast((try zeit.Time.fromISO8601(fixedTs)).instant().timestamp);

    const absoluteStartStr = "2026-06-26T06:41:09.924Z";
    const absoluteEndStr = "2026-06-26T09:41:09.924Z";
    const absoluteStart: u64 = @intCast((try zeit.Time.fromISO8601(absoluteStartStr)).instant().timestamp);
    const absoluteEnd: u64 = @intCast((try zeit.Time.fromISO8601(absoluteEndStr)).instant().timestamp);

    const Case = struct {
        query: []const u8,
        expected: ?Query,
        expectedErr: ?anyerror = null,
        expectedReports: []const ErrorReporter.SyntaxError,
    };

    const cases = [_]Case{
        .{
            .query = "[-5m,now] {env=prod}",
            .expected = .{
                .start = fiveMinutesAgo,
                .end = now,
                .tagsExpr = &.{ .predicate = .{ .key = "env", .value = "prod", .op = .equal } },
                .fieldsExpr = null,
            },
            .expectedReports = &.{},
        },
        .{
            .query = "[-2h,] {env=prod AND (service=one or service=two)}",
            .expected = .{
                .start = now - 2 * std.time.ns_per_hour,
                .end = now,
                .tagsExpr = &.{
                    .andOp = .{
                        &.{ .predicate = .{ .key = "env", .value = "prod", .op = .equal } },
                        &.{
                            .orOp = .{
                                &.{ .predicate = .{ .key = "service", .value = "one", .op = .equal } },
                                &.{ .predicate = .{ .key = "service", .value = "two", .op = .equal } },
                            },
                        },
                    },
                },
                .fieldsExpr = null,
            },
            .expectedReports = &.{},
        },
        .{
            .query = "\n\t[-10m,now] {env=prod and service!=api}\t\n",
            .expected = .{
                .start = now - 10 * std.time.ns_per_min,
                .end = now,
                .tagsExpr = &.{
                    .andOp = .{
                        &.{ .predicate = .{ .key = "env", .value = "prod", .op = .equal } },
                        &.{ .predicate = .{ .key = "service", .value = "api", .op = .notEqual } },
                    },
                },
                .fieldsExpr = null,
            },
            .expectedReports = &.{},
        },
        .{
            .query = "[-15m,now] {ENV=prod OR (service=api and host=web)} message=timeout",
            .expected = .{
                .start = now - 15 * std.time.ns_per_min,
                .end = now,
                .tagsExpr = &.{
                    .orOp = .{
                        &.{ .predicate = .{ .key = "ENV", .value = "prod", .op = .equal } },
                        &.{ .andOp = .{
                            &.{ .predicate = .{ .key = "service", .value = "api", .op = .equal } },
                            &.{ .predicate = .{ .key = "host", .value = "web", .op = .equal } },
                        } },
                    },
                },
                .fieldsExpr = &.{ .predicate = .{ .key = "message", .value = "timeout", .op = .equal } },
            },
            .expectedReports = &.{},
        },
        .{
            .query = "[2024-01-10T00:00:00Z,now] {env=prod}",
            .expected = .{
                .start = fixedTsNs,
                .end = now,
                .tagsExpr = &.{ .predicate = .{ .key = "env", .value = "prod", .op = .equal } },
                .fieldsExpr = null,
            },
            .expectedReports = &.{},
        },
        .{
            .query = "[-5m,now] {env=prod} message~err",
            .expected = .{
                .start = fiveMinutesAgo,
                .end = now,
                .tagsExpr = &.{ .predicate = .{ .key = "env", .value = "prod", .op = .equal } },
                .fieldsExpr = &.{ .predicate = .{ .key = "message", .value = "err", .op = .matchRegex } },
            },
            .expectedReports = &.{},
        },
        // tags are optional
        .{
            .query = "[-5m,now] env=prod AND message=err",
            .expected = .{
                .start = fiveMinutesAgo,
                .end = now,
                .fieldsExpr = &.{ .andOp = .{
                    &.{ .predicate = .{ .key = "env", .value = "prod", .op = .equal } },
                    &.{ .predicate = .{ .key = "message", .value = "err", .op = .equal } },
                } },
            },
            .expectedReports = &.{},
        },
        // AND is implicit
        .{
            .query = "[-5m,now] env=prod message=err",
            .expected = .{
                .start = fiveMinutesAgo,
                .end = now,
                .fieldsExpr = &.{ .andOp = .{
                    &.{ .predicate = .{ .key = "env", .value = "prod", .op = .equal } },
                    &.{ .predicate = .{ .key = "message", .value = "err", .op = .equal } },
                } },
            },
            .expectedReports = &.{},
        },
        .{
            .query = "[-2d,30m] {env=prod and service!=api} (message~timeout and path!~health)",
            .expected = .{
                .start = now - 2 * std.time.ns_per_day,
                .end = now + 30 * std.time.ns_per_min,
                .tagsExpr = &.{ .andOp = .{
                    &.{ .predicate = .{ .key = "env", .value = "prod", .op = .equal } },
                    &.{ .predicate = .{ .key = "service", .value = "api", .op = .notEqual } },
                } },
                .fieldsExpr = &.{ .andOp = .{
                    &.{ .predicate = .{ .key = "message", .value = "timeout", .op = .matchRegex } },
                    &.{ .predicate = .{ .key = "path", .value = "health", .op = .notMatchRegex } },
                } },
            },
            .expectedReports = &.{},
        },
        .{
            .query = "[-30s,now] {env=prod and (service=api or host=edge)} (status=500 or message~panic and endpoint!~health)",
            .expected = .{
                .start = now - 30 * std.time.ns_per_s,
                .end = now,
                .tagsExpr = &.{ .andOp = .{
                    &.{ .predicate = .{ .key = "env", .value = "prod", .op = .equal } },
                    &.{ .orOp = .{
                        &.{ .predicate = .{ .key = "service", .value = "api", .op = .equal } },
                        &.{ .predicate = .{ .key = "host", .value = "edge", .op = .equal } },
                    } },
                } },
                .fieldsExpr = &.{ .orOp = .{
                    &.{ .predicate = .{ .key = "status", .value = "500", .op = .equal } },
                    &.{ .andOp = .{
                        &.{ .predicate = .{ .key = "message", .value = "panic", .op = .matchRegex } },
                        &.{ .predicate = .{ .key = "endpoint", .value = "health", .op = .notMatchRegex } },
                    } },
                } },
            },
            .expectedReports = &.{},
        },
        .{
            .query = "[-30s,now] {env=prod and (service=api or host=edge)} (status=500 or message~panic) and endpoint!~health",
            .expected = .{
                .start = now - 30 * std.time.ns_per_s,
                .end = now,
                .tagsExpr = &.{ .andOp = .{
                    &.{ .predicate = .{ .key = "env", .value = "prod", .op = .equal } },
                    &.{ .orOp = .{
                        &.{ .predicate = .{ .key = "service", .value = "api", .op = .equal } },
                        &.{ .predicate = .{ .key = "host", .value = "edge", .op = .equal } },
                    } },
                } },
                .fieldsExpr = &.{ .andOp = .{
                    &.{ .orOp = .{
                        &.{ .predicate = .{ .key = "status", .value = "500", .op = .equal } },
                        &.{ .predicate = .{ .key = "message", .value = "panic", .op = .matchRegex } },
                    } },
                    &.{ .predicate = .{ .key = "endpoint", .value = "health", .op = .notMatchRegex } },
                } },
            },
            .expectedReports = &.{},
        },
        .{
            .query = "[-30s,now] {env=prod and (service=api or host=edge)} status=500 or (message~panic and endpoint!~health)",
            .expected = .{
                .start = now - 30 * std.time.ns_per_s,
                .end = now,
                .tagsExpr = &.{ .andOp = .{
                    &.{ .predicate = .{ .key = "env", .value = "prod", .op = .equal } },
                    &.{ .orOp = .{
                        &.{ .predicate = .{ .key = "service", .value = "api", .op = .equal } },
                        &.{ .predicate = .{ .key = "host", .value = "edge", .op = .equal } },
                    } },
                } },
                .fieldsExpr = &.{ .orOp = .{
                    &.{ .predicate = .{ .key = "status", .value = "500", .op = .equal } },
                    &.{ .andOp = .{
                        &.{ .predicate = .{ .key = "message", .value = "panic", .op = .matchRegex } },
                        &.{ .predicate = .{ .key = "endpoint", .value = "health", .op = .notMatchRegex } },
                    } },
                } },
            },
            .expectedReports = &.{},
        },
        // message single quoted
        .{
            .query = "[-5m,now] {env=prod} message='@ts=1782479674646 @l=ERROR msg=\"failed to handle request\" err=FailedToParse'",
            .expected = .{
                .start = fiveMinutesAgo,
                .end = now,
                .tagsExpr = &.{ .predicate = .{ .key = "env", .value = "prod", .op = .equal } },
                .fieldsExpr = &.{ .predicate = .{ .key = "message", .value = "@ts=1782479674646 @l=ERROR msg=\"failed to handle request\" err=FailedToParse", .op = .equal } },
            },
            .expectedReports = &.{},
        },
        // message double quoted, inner keys are escaped
        .{
            .query = "[-5m,now] {env=prod} message=\"@ts=1782479674646 @l=ERROR msg=\\\"failed to handle request\\\" err=FailedToParse\"",
            .expected = .{
                .start = fiveMinutesAgo,
                .end = now,
                .tagsExpr = &.{ .predicate = .{ .key = "env", .value = "prod", .op = .equal } },
                .fieldsExpr = &.{ .predicate = .{ .key = "message", .value = "@ts=1782479674646 @l=ERROR msg=\"failed to handle request\" err=FailedToParse", .op = .equal } },
            },
            .expectedReports = &.{},
        },
        // message double quoted, inner keys are single quoted
        .{
            .query = "[-5m,now] {env=prod} message=\"@ts=1782479674646 @l=ERROR msg='failed to handle request' err=FailedToParse\"",
            .expected = .{
                .start = fiveMinutesAgo,
                .end = now,
                .tagsExpr = &.{ .predicate = .{ .key = "env", .value = "prod", .op = .equal } },
                .fieldsExpr = &.{ .predicate = .{ .key = "message", .value = "@ts=1782479674646 @l=ERROR msg='failed to handle request' err=FailedToParse", .op = .equal } },
            },
            .expectedReports = &.{},
        },
        // brackets surrounded quoted literal
        .{
            .query = "[-5m,now] {job=local} (_msg=\"@ts=1782478454763 @l=INFO msg=\\\"Ochi in mono mode starting\\\" port=9014 version=\\\"main-027a8736\")",
            .expected = .{
                .start = fiveMinutesAgo,
                .end = now,
                .tagsExpr = &.{ .predicate = .{ .key = "job", .value = "local", .op = .equal } },
                .fieldsExpr = &.{ .predicate = .{ .key = "_msg", .value = "@ts=1782478454763 @l=INFO msg=\"Ochi in mono mode starting\" port=9014 version=\"main-027a8736", .op = .equal } },
            },
            .expectedReports = &.{},
        },
        .{
            .query = "[" ++ absoluteStartStr ++ "," ++ absoluteEndStr ++ "] {env=prod}",
            .expected = .{
                .start = absoluteStart,
                .end = absoluteEnd,
                .tagsExpr = &.{ .predicate = .{ .key = "env", .value = "prod", .op = .equal } },
            },
            .expectedReports = &.{},
        },
        .{
            .query = "[now,now] {env=prod}",
            .expected = null,
            .expectedErr = error.InvalidRange,
            .expectedReports = &.{},
        },
        .{
            .query = "[,] {env=prod}",
            .expected = null,
            .expectedErr = error.SyntaxError,
            .expectedReports = &.{
                .{ .line = 1, .col = 5, .message = "At least one of the time range values must be specified." },
            },
        },
        .{
            .query = "[not-a-tz,now] {env=prod}",
            .expected = null,
            .expectedErr = error.InvalidTimestamp,
            .expectedReports = &.{},
        },
        .{
            .query = "[-xm,now] {env=prod}",
            .expected = null,
            .expectedErr = error.InvalidDuration,
            .expectedReports = &.{},
        },
        .{
            .query = "[-5m,now] {env~prod}",
            .expected = null,
            .expectedErr = error.SyntaxError,
            .expectedReports = &.{
                .{ .line = 1, .col = 15, .message = "Expect '}' after tags." },
            },
        },
        .{
            .query = "[-5m,now] {env=prod} message=",
            .expected = null,
            .expectedErr = error.SyntaxError,
            .expectedReports = &.{
                .{ .line = 1, .col = 29, .message = "Expect expression." },
            },
        },
        .{
            .query = "[-5m, now] {v}",
            .expected = null,
            .expectedErr = error.SyntaxError,
            .expectedReports = &.{
                .{ .line = 1, .col = 13, .message = "Expect expression." },
            },
        },
        .{
            .query = "[-5m, now] 42",
            .expected = null,
            .expectedErr = error.SyntaxError,
            .expectedReports = &.{
                .{ .line = 1, .col = 12, .message = "Expect expression." },
            },
        },
        .{
            .query = "[-5m,now] {env=prod} (message=timeout",
            .expected = null,
            .expectedErr = error.SyntaxError,
            .expectedReports = &.{
                .{ .line = 1, .col = 31, .message = "Expect ')' after expression." },
            },
        },
        .{
            .query = "[560,now] {env=prod} (message=timeout",
            .expected = null,
            .expectedErr = error.SyntaxError,
            .expectedReports = &.{
                .{ .line = 1, .col = 31, .message = "Expect ')' after expression." },
            },
        },
    };

    for (cases) |case| {
        var reporter = ErrorReporter{};
        var loql: Loql = .{};
        defer loql.deinit(allocator);

        const filter = loql.translateQuery(allocator, &reporter, case.query, now);
        if (case.expectedErr) |err| {
            try std.testing.expectError(err, filter);
        }
        if (case.expected) |expected| {
            try std.testing.expectEqualDeep(try filter, expected);
        }
        try std.testing.expectEqualSlices(ErrorReporter.SyntaxError, reporter.syntaxErrors(), case.expectedReports);
    }
}
