const std = @import("std");
const Allocator = std.mem.Allocator;

const zeit = @import("zeit");

const stdsTime = @import("../stds/time.zig");
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
    DurationBeforeUnixEpoch,
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

    var tagsExpr: ?*const FilterExpression = null;
    if (qset.tags) |tags| {
        tagsExpr = try self.translateExpression(allocator, unwrapGroup(&tags));
    }
    const fieldsExpr = if (qset.query) |q| try self.translateExpression(allocator, unwrapGroup(&q)) else null;
    const q: Query = .{
        .start = range.startTimeNs,
        .end = range.endTimeNs,
        .tagsExpr = tagsExpr,
        .fieldsExpr = fieldsExpr,
    };

    if (q.start >= q.end) {
        return TranslateError.InvalidRange;
    }

    return q;
}

// Recursively unwraps grouping expressions to get to the underlying predicate or operator expression
fn unwrapGroup(expr: *const Expression) Expression {
    if (expr.* == .grouping) {
        return unwrapGroup(expr.grouping);
    }

    return expr.*;
}

fn translateRange(range: TimeRangeExpression, nowNs: u64) TranslateError!struct { startTimeNs: u64, endTimeNs: u64 } {
    return .{
        .startTimeNs = try translateTimeValue(range[0], nowNs),
        .endTimeNs = try translateTimeValue(range[1], nowNs),
    };
}

pub fn translateTimeValue(value: TimeValue, nowNs: u64) TranslateError!u64 {
    switch (value) {
        .now => return nowNs,
        .duration => |d| {
            if (d.len == 0) {
                return TranslateError.InvalidDuration;
            }

            if (d[0] == '-') {
                const dur = try translateTimeDuration(d[1..]);
                return std.math.sub(u64, nowNs, dur) catch TranslateError.DurationBeforeUnixEpoch;
            } else {
                const dur = try translateTimeDuration(d);
                return std.math.add(u64, nowNs, dur) catch TranslateError.OverflowDuration;
            }
        },
        .timestamp => |ts| {
            const timestamp = zeit.Time.fromISO8601(ts) catch return TranslateError.InvalidTimestamp;
            const ns = timestamp.instant().timestamp;
            if (ns < 0) {
                return TranslateError.InvalidTimestamp;
            }
            if (ns > std.math.maxInt(u64)) {
                return TranslateError.InvalidTimestamp;
            }
            return @intCast(ns);
        },
    }
}

fn translateTimeDuration(raw: []const u8) TranslateError!u64 {
    return stdsTime.parseDurationNs(raw) catch return TranslateError.InvalidDuration;
}

fn translateExpression(self: *Translator, alloc: Allocator, tags: Expression) TranslateError!*FilterExpression {
    return switch (tags) {
        .equalOp => |pred| try self.translatePredicate(alloc, pred[0].*, pred[1].*, .equal),
        .notEqualOp => |pred| try self.translatePredicate(alloc, pred[0].*, pred[1].*, .notEqual),
        .matchRegexOp => |pred| try self.translatePredicate(alloc, pred[0].*, pred[1].*, .matchRegex),
        .notMatchRegexOp => |pred| try self.translatePredicate(alloc, pred[0].*, pred[1].*, .notMatchRegex),
        .andOp => |pair| blk: {
            const left = try self.translateExpression(alloc, pair[0].*);
            const right = try self.translateExpression(alloc, pair[1].*);
            break :blk try self.allocFilterExpression(alloc, .{ .andOp = .{ left, right } });
        },
        .orOp => |pair| blk: {
            const left = try self.translateExpression(alloc, pair[0].*);
            const right = try self.translateExpression(alloc, pair[1].*);
            break :blk try self.allocFilterExpression(alloc, .{ .orOp = .{ left, right } });
        },
        .grouping => |inner| try self.translateExpression(alloc, inner.*),
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

test {
    _ = @import("Translator_test.zig");
}
