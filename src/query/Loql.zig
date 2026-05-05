/// Logql is a Logs Ochi Query Language,
/// translates a query to ochi query API
const std = @import("std");
const Allocator = std.mem.Allocator;

const Scanner = @import("Scanner.zig");
const Parser = @import("Parser.zig");
const Translator = @import("Translator.zig");
const ErrorReporter = @import("ErrorReporter.zig");

const Query = @import("Query.zig");

const testing = std.testing;

test "validateQuery" {
    const Case = struct {
        query: Query,
        expectedErr: ?anyerror = null,
    };

    const orExpr: Query.FilterExpression = .{
        .orOp = .{
            &.{ .predicate = .{ .key = "env", .value = "prod", .op = .equal } },
            &.{ .predicate = .{ .key = "service", .value = "worker", .op = .notEq } },
        },
    };
    const regex: Query.FilterExpression = .{ .predicate = .{ .key = "env", .value = "prod.*", .op = .matchRegex } };

    const cases = [_]Case{
        .{
            // valid time range and no tags
            .query = .{
                .startTimeNs = 10,
                .endTimeNs = 20,
                .tagsExpr = null,
                .fieldsExpr = null,
            },
        },
        .{
            // valid time range and supported tag operators
            .query = .{
                .startTimeNs = 10,
                .endTimeNs = 20,
                .tagsExpr = &orExpr,
                .fieldsExpr = null,
            },
        },
        .{
            // invalid time range when equal
            .query = .{
                .startTimeNs = 20,
                .endTimeNs = 20,
                .tagsExpr = null,
                .fieldsExpr = null,
            },
            .expectedErr = error.InvalidTimeRange,
        },
        .{
            // invalid time range when start is after end
            .query = .{
                .startTimeNs = 21,
                .endTimeNs = 20,
                .tagsExpr = null,
                .fieldsExpr = null,
            },
            .expectedErr = error.InvalidTimeRange,
        },
        .{
            // unsupported tag operator
            .query = .{
                .startTimeNs = 10,
                .endTimeNs = 20,
                .tagsExpr = &regex,
                .fieldsExpr = null,
            },
            .expectedErr = error.UnsupportedTagOperator,
        },
    };

    for (cases) |case| {
        if (case.expectedErr) |expectedErr| {
            try testing.expectError(expectedErr, case.query.validate());
        } else {
            try case.query.validate();
        }
    }
}

const Loql = @This();

scanner: Scanner = .{},
parser: Parser = .{},
translator: Translator = .{},

// TODO: it must accept a reader, not a query string
pub fn translateQuery(
    self: *Loql,
    allocator: Allocator,
    reporter: *ErrorReporter.ErrorReporter,
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

    try query.validate();
    return query;
}
