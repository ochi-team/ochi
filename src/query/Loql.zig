/// Logql is a Logs Ochi Query Language,
/// translates a query to ochi query API
const std = @import("std");
const Allocator = std.mem.Allocator;
const Io = std.Io;

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

const maxQueryLength = 4096;
// TODO: it must accept a reader, not a query string
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
    // TODO: most likely scanner has a state, we must reset it
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
    const fixedTsNs: u64 = @intCast((try Translator.zeit.Time.fromISO8601(fixedTs)).instant().timestamp);

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
