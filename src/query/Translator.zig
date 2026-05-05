const std = @import("std");
const Allocator = std.mem.Allocator;

pub const zeit = @import("zeit");

const Query = @import("Query.zig");
const FilterExpression = Query.FilterExpression;
const MatchOp = Query.MatchOp;
const TimeRangeExpression = @import("Parser.zig").TimeRangeExpression;
const TimeValue = @import("Parser.zig").TimeValue;
const QuerySet = @import("Parser.zig").QuerySet;
const Expression = @import("Parser.zig").Expression;

pub const TranslateError = error{
    InvalidDuration,
    OverflowDuration,
    InvalidTimestamp,
    InvalidRange,
    ExpectedLiteral,
    UnexpectedTagExpression,
} || Allocator.Error;

const Translator = @This();

garbage: std.ArrayListUnmanaged(*FilterExpression) = .empty,

pub fn deinit(self: *Translator, allocator: Allocator) void {
    for (self.garbage.items) |expr| allocator.destroy(expr);
    self.garbage.deinit(allocator);
}

pub fn query(self: *Translator, allocator: Allocator, qset: QuerySet, nowNs: u64) TranslateError!Query {
    const range = try translateRange(qset.timeRange, nowNs);
    const tagsExpr = try self.translateTags(allocator, qset.tags);
    const q: Query = .{
        .startTimeNs = range.startTimeNs,
        .endTimeNs = range.endTimeNs,
        .tagsExpr = tagsExpr,
        .fieldsExpr = null,
    };

    if (q.startTimeNs >= q.endTimeNs) {
        return TranslateError.InvalidRange;
    }

    return q;
}

fn translateRange(range: TimeRangeExpression, nowNs: u64) TranslateError!struct { startTimeNs: u64, endTimeNs: u64 } {
    return .{
        .startTimeNs = try translateTimeValue(range[0], nowNs),
        .endTimeNs = try translateTimeValue(range[1], nowNs),
    };
}

fn translateTimeValue(value: TimeValue, nowNs: u64) TranslateError!u64 {
    switch (value) {
        .now => return nowNs,
        .duration => |d| {
            if (d.len == 0) {
                return TranslateError.InvalidDuration;
            }

            if (d[0] == '-') {
                const dur = try translateTimeDuration(d[1..]);
                return std.math.sub(u64, nowNs, dur) catch TranslateError.OverflowDuration;
            } else {
                const dur = try translateTimeDuration(d);
                return std.math.add(u64, nowNs, dur) catch TranslateError.InvalidDuration;
            }
        },
        .timestamp => |ts| {
            const timestamp = zeit.Time.fromISO8601(ts) catch return TranslateError.InvalidTimestamp;
            const ns = timestamp.instant().timestamp;
            if (ns <= 0) {
                return TranslateError.InvalidTimestamp;
            }
            return @intCast(ns);
        },
    }
}

fn translateTimeDuration(raw: []const u8) TranslateError!u64 {
    if (raw.len < 2) {
        return TranslateError.InvalidDuration;
    }

    const unit = raw[raw.len - 1];
    const valueRaw = raw[0 .. raw.len - 1];
    const value = std.fmt.parseInt(u64, valueRaw, 10) catch return TranslateError.InvalidDuration;

    const multiplier: u64 = switch (unit) {
        's' => std.time.ns_per_s,
        'm' => std.time.ns_per_min,
        'h' => std.time.ns_per_hour,
        'd' => std.time.ns_per_day,
        else => return TranslateError.InvalidDuration,
    };

    return std.math.mul(u64, value, multiplier) catch TranslateError.InvalidDuration;
}

fn translateTags(self: *Translator, alloc: Allocator, tags: Expression) TranslateError!*FilterExpression {
    return switch (tags) {
        .equalOp => |pred| try self.translatePredicate(alloc, pred[0].*, pred[1].*, .equal),
        .notEqualOp => |pred| try self.translatePredicate(alloc, pred[0].*, pred[1].*, .notEqual),
        .matchRegexOp => |pred| try self.translatePredicate(alloc, pred[0].*, pred[1].*, .matchRegex),
        .notMatchRegexOp => |pred| try self.translatePredicate(alloc, pred[0].*, pred[1].*, .notMatchRegex),
        .andOp => |pair| blk: {
            const left = try self.translateTags(alloc, pair[0].*);
            const right = try self.translateTags(alloc, pair[1].*);
            break :blk try self.allocFilterExpression(alloc, .{ .andOp = .{ left, right } });
        },
        .orOp => |pair| blk: {
            const left = try self.translateTags(alloc, pair[0].*);
            const right = try self.translateTags(alloc, pair[1].*);
            break :blk try self.allocFilterExpression(alloc, .{ .orOp = .{ left, right } });
        },
        .grouping => |inner| blk: {
            const nested = try self.translateTags(alloc, inner.*);
            break :blk try self.allocFilterExpression(alloc, .{ .grouping = nested });
        },
        else => error.UnexpectedTagExpression,
    };
}

fn translatePredicate(
    self: *Translator,
    allocator: Allocator,
    left: Expression,
    right: Expression,
    op: MatchOp,
) TranslateError!*FilterExpression {
    const key = switch (left) {
        .literal => |v| v,
        else => return TranslateError.ExpectedLiteral,
    };

    const value = switch (right) {
        .literal => |v| v,
        else => return TranslateError.ExpectedLiteral,
    };

    return self.allocFilterExpression(allocator, .{ .predicate = .{
        .key = key,
        .value = value,
        .op = op,
    } });
}

fn allocFilterExpression(
    self: *Translator,
    allocator: Allocator,
    expr: FilterExpression,
) TranslateError!*FilterExpression {
    try self.garbage.ensureUnusedCapacity(allocator, 1);

    const node = try allocator.create(FilterExpression);
    errdefer allocator.destroy(node);

    self.garbage.appendAssumeCapacity(node);
    node.* = expr;
    return node;
}

const testing = std.testing;

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
                .startTimeNs = now - (5 * std.time.ns_per_min),
                .endTimeNs = now + (30 * std.time.ns_per_s),
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
                .startTimeNs = expectedStart,
                .endTimeNs = expectedEnd,
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
    };

    for (cases) |case| {
        var translator: Translator = .{};
        defer translator.deinit(allocator);

        if (case.expectedErr) |expectedErr| {
            try testing.expectError(expectedErr, translator.query(allocator, case.qset, case.nowNs));
            continue;
        }

        const got = try translator.query(allocator, case.qset, case.nowNs);
        try testing.expectEqualDeep(case.expectedQuery.?, got);
    }
}
