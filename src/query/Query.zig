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

    pub fn stringifyLimited(filter: *const FilterExpression, buf: []u8) usize {
        std.debug.assert(buf.len > 0);

        var n: usize = 0;
        while (n < buf.len) {
            switch (filter.*) {
                .predicate => |p| {
                    const opStr = switch (p.op) {
                        .equal => "=",
                        .notEqual => "!=",
                        .matchRegex => "~",
                        .notMatchRegex => "!~",
                        .lessThan => "<",
                        .lessThanOrEqual => "<=",
                        .greaterThan => ">",
                        .greaterThanOrEqual => ">=",
                    };
                    buf[n] = '(';
                    n += 1;
                    if (buf[n..].len < p.key.len) break;
                    @memcpy(buf[n .. n + p.key.len], p.key);
                    n += p.key.len;
                    if (n >= buf.len) break;
                    buf[n] = ' ';
                    n += 1;
                    if (buf[n..].len < opStr.len) break;
                    @memcpy(buf[n .. n + opStr.len], opStr);
                    n += opStr.len;
                    if (n >= buf.len) break;
                    buf[n] = ' ';
                    n += 1;
                    if (buf[n..].len < p.value.len) break;
                    @memcpy(buf[n .. n + p.value.len], p.value);
                    n += p.value.len;
                    if (n < buf.len) {
                        buf[n] = ')';
                        n += 1;
                    }
                    break;
                },
                .andOp => |ops| {
                    buf[n] = '(';
                    n += 1;
                    if (n >= buf.len) break;
                    n += stringifyLimited(ops[0], buf[n..]);
                    if (n >= buf.len) break;
                    buf[n] = ' ';
                    n += 1;
                    if (buf[n..].len < 3) break;
                    @memcpy(buf[n .. n + 3], "AND");
                    n += 3;
                    if (n >= buf.len) break;
                    buf[n] = ' ';
                    n += 1;
                    if (n >= buf.len) break;
                    n += stringifyLimited(ops[1], buf[n..]);
                    if (n < buf.len) {
                        buf[n] = ')';
                        n += 1;
                    }
                    break;
                },
                .orOp => |ops| {
                    buf[n] = '(';
                    n += 1;
                    if (n >= buf.len) break;
                    n += stringifyLimited(ops[0], buf[n..]);
                    if (n >= buf.len) break;
                    buf[n] = ' ';
                    n += 1;
                    if (buf[n..].len < 2) break;
                    @memcpy(buf[n .. n + 2], "OR");
                    n += 2;
                    if (n >= buf.len) break;
                    buf[n] = ' ';
                    n += 1;
                    if (n >= buf.len) break;
                    n += stringifyLimited(ops[1], buf[n..]);
                    if (n < buf.len) {
                        buf[n] = ')';
                        n += 1;
                    }
                    break;
                },
            }
        }

        return n;
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

    var buf: [12]u8 = undefined;
    var n = orExpr.stringifyLimited(&buf);
    try testing.expectEqualStrings("((env = prod", buf[0..n]);

    var bufLonger: [64]u8 = undefined;
    n = orExpr.stringifyLimited(&bufLonger);
    try testing.expectEqualStrings("((env = prod) OR (service != worker))", bufLonger[0..n]);
}
