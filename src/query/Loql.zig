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

const Loql = @This();

scanner: Scanner = .{},
parser: Parser = .{},
translator: Translator = .{},

pub fn deinit(self: *Loql, allocator: Allocator) void {
    self.scanner.deinit(allocator);
    self.parser.deinit(allocator);
    self.translator.deinit(allocator);
}

// TODO: it must accept a reader, not a query string
pub fn translateQuery(
    self: *Loql,
    allocator: Allocator,
    reporter: *ErrorReporter,
    fullQueryStr: []const u8,
    nowNs: u64,
) !Query {
    const trimmedQueryStr = std.mem.trim(u8, fullQueryStr, " \n\t");

    // TODO: validate whether it's a syntax error and return report content
    // or unknown error
    // TODO: most likely scanner has a state, we must reset it
    try self.scanner.scan(allocator, trimmedQueryStr, reporter);
    const expr = try self.parser.querySet(allocator, self.scanner.tokens.items, reporter);
    const query = try self.translator.query(allocator, expr, nowNs);

    return query;
}

const testing = std.testing;

test "translateQuery" {
    const allocator = testing.allocator;
    const io = testing.io;

    const now: u64 = @intCast(Io.Timestamp.now(io, .real).nanoseconds);
    const fiveMinutesAgo = now - 5 * std.time.ns_per_min;

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
                .startTimeNs = fiveMinutesAgo,
                .endTimeNs = now,
                .tagsExpr = &.{ .predicate = .{ .key = "env", .value = "prod", .op = .equal } },
                .fieldsExpr = null,
            },
            .expectedReports = &.{},
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
