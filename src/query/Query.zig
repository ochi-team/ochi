const std = @import("std");

pub const Query = @This();

startTimeNs: u64,
endTimeNs: u64,

tagsExpr: *const FilterExpression,
fieldsExpr: ?*const FilterExpression,

pub fn validate(q: *const Query) !void {
    if (q.startTimeNs >= q.endTimeNs) {
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
                .startTimeNs = 10,
                .endTimeNs = 20,
                .tagsExpr = &orExpr,
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
                .tagsExpr = &orExpr,
                .fieldsExpr = null,
            },
            .expectedErr = error.InvalidTimeRange,
        },
        .{
            // invalid time range when start is after end
            .query = .{
                .startTimeNs = 21,
                .endTimeNs = 20,
                .tagsExpr = &orExpr,
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
