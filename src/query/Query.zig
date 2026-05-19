const std = @import("std");

pub const Query = @This();

// start time range inclusive in ns
start: u64,
// end time range inclusive in ns
end: u64,

tagsExpr: *const FilterExpression,
fieldsExpr: ?*const FilterExpression,

pub fn validate(q: *const Query) !void {
    if (q.start >= q.end) {
        return error.InvalidTimeRange;
    }

    // validate only tags, because we restrict them to use only equal and notEq operations
    try q.tagsExpr.validateTags();
}

pub const InvalidQueryError = error{
    InvalidTimeRange,
    UnsupportedTagOperator,
};

pub const MatchOp = enum {
    equal,
    notEqual,

    matchRegex,
    notMatchRegex,
    lessThan,
    lessThanOrEqual,
    greaterThan,
    greaterThanOrEqual,
};

pub const FilterPredicate = struct {
    key: []const u8,
    value: []const u8,
    op: MatchOp,
};

pub const FilterExpression = union(enum) {
    predicate: FilterPredicate,
    andOp: [2]*const FilterExpression,
    orOp: [2]*const FilterExpression,

    pub fn validateTags(filter: *const FilterExpression) InvalidQueryError!void {
        switch (filter.*) {
            .predicate => |p| if (p.op != .equal and p.op != .notEqual) {
                return error.UnsupportedTagOperator;
            },
            .andOp => |ops| {
                try ops[0].validateTags();
                try ops[1].validateTags();
            },
            .orOp => |ops| {
                try ops[0].validateTags();
                try ops[1].validateTags();
            },
        }
    }

    pub fn stringifyLimited(filter: *const FilterExpression, buf: []u8) []const u8 {
        var n = 0;
        while (n < buf.len) {
            switch (filter.*) {
                .predicate => |p| {
                    const opStr = switch (p.op) {
                        .equal => "==",
                        .notEqual => "!=",
                        .matchRegex => "~",
                        .notMatchRegex => "!~",
                        .lessThan => "<",
                        .lessThanOrEqual => "<=",
                        .greaterThan => ">",
                        .greaterThanOrEqual => ">=",
                    };
                    n += std.fmt.bufPrint(buf[n..], "{s} {s} {s}", .{ p.key, opStr, p.value });
                },
                .andOp => |ops| {
                    n += std.fmt.bufPrint(buf[n..], "({s}) AND ({s})", .{
                        ops[0].stringifyLimited(buf[n..]),
                        ops[1].stringifyLimited(buf[n..]),
                    });
                },
                .orOp => |ops| {
                    n += std.fmt.bufPrint(buf[n..], "({s}) OR ({s})", .{
                        ops[0].stringifyLimited(buf[n..]),
                        ops[1].stringifyLimited(buf[n..]),
                    });
                },
            }
        }

        return buf[0..n];
    }
};

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

    var buf: [8]u8 = undefined;
    var str = orExpr.stringifyLimited(&buf);
    try testing.expectEqualStrings("((env ==", str);

    var bufLonger: [64]u8 = undefined;
    str = orExpr.stringifyLimited(&bufLonger);
    try testing.expectEqualStrings("((env == prod) OR (service != worker))", str);
}
