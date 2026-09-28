const std = @import("std");

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
            &.{ .predicate = .{ .key = "service", .value = "worker", .op = .notEqual } },
        },
    };
    const regex: Query.FilterExpression = .{ .predicate = .{ .key = "env", .value = "prod.*", .op = .matchRegex } };

    const cases = [_]Case{
        .{
            // valid time range and no tags
            .query = .{
                .start = 10,
                .end = 20,
                .tagsExpr = &orExpr,
                .fieldsExpr = null,
            },
        },
        .{
            // valid time range and supported tag operators
            .query = .{
                .start = 10,
                .end = 20,
                .tagsExpr = &orExpr,
                .fieldsExpr = null,
            },
        },
        .{
            // invalid time range when equal
            .query = .{
                .start = 20,
                .end = 20,
                .tagsExpr = &orExpr,
                .fieldsExpr = null,
            },
            .expectedErr = error.InvalidTimeRange,
        },
        .{
            // invalid time range when start is after end
            .query = .{
                .start = 21,
                .end = 20,
                .tagsExpr = &orExpr,
                .fieldsExpr = null,
            },
            .expectedErr = error.InvalidTimeRange,
        },
        .{
            // unsupported tag operator
            .query = .{
                .start = 10,
                .end = 20,
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

test "stringifyLimited" {
    const orExpr: Query.FilterExpression = .{
        .orOp = .{
            &.{ .predicate = .{ .key = "env", .value = "prod", .op = .equal } },
            &.{ .predicate = .{ .key = "service", .value = "worker", .op = .notEqual } },
        },
    };

    var buf: [12]u8 = undefined;
    var n = orExpr.stringifyLimited(&buf);
    try testing.expectEqualStrings("((env = prod", buf[0..n]);

    var bufLonger: [64]u8 = undefined;
    n = orExpr.stringifyLimited(&bufLonger);
    try testing.expectEqualStrings("((env = prod) OR (service != worker))", bufLonger[0..n]);
}

test "encodeCacheKey" {
    const andExpr: Query.FilterExpression = .{
        .andOp = .{
            &.{ .predicate = .{ .key = "env", .value = "prod", .op = .equal } },
            &.{ .predicate = .{ .key = "service", .value = "worker", .op = .notEqual } },
        },
    };
    const orExpr: Query.FilterExpression = .{
        .orOp = .{
            &.{ .predicate = .{ .key = "env", .value = "prod", .op = .equal } },
            &.{ .predicate = .{ .key = "service", .value = "worker", .op = .notEqual } },
        },
    };

    const Case = struct {
        expr: Query.FilterExpression,
        expected: []const u8,
    };
    const cases = [_]Case{
        .{
            .expr = andExpr,
            .expected = "\x01\x00\x00\x03env\x04prod\x00\x01\x07service\x06worker",
        },
        .{
            .expr = orExpr,
            .expected = "\x02\x00\x00\x03env\x04prod\x00\x01\x07service\x06worker",
        },
    };

    for (cases) |case| {
        var buf: [64]u8 = undefined;
        const n = case.expr.encodeCacheKey(&buf);
        try testing.expectEqualSlices(u8, case.expected, buf[0..n]);
    }
}
