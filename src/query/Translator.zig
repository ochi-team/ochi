const std = @import("std");
const Allocator = std.mem.Allocator;
const testing = std.testing;

const Field = @import("../store/lines.zig").Field;
const Parser = @import("Parser.zig");
const Loql = @import("Loql.zig");

pub const TranslateError = error{
    OutOfMemory,
    UnsupportedExpression,
    InvalidRange,
    InvalidDuration,
    InvalidTimestamp,
};

pub const Translator = @This();

allocator: Allocator,
garbage: std.ArrayList(*Loql.FilterExpression) = .empty,
fieldGarbage: std.ArrayList([]const Field) = .empty,

pub fn init(allocator: Allocator) Translator {
    return .{ .allocator = allocator };
}

pub fn deinit(self: *Translator) void {
    for (self.fieldGarbage.items) |fields| {
        self.allocator.free(fields);
    }
    self.fieldGarbage.deinit(self.allocator);

    for (self.garbage.items) |node| {
        self.allocator.destroy(node);
    }
    self.garbage.deinit(self.allocator);
}

pub fn query(self: *Translator, querySet: Parser.QuerySet) TranslateError!Loql.Query {
    const nowNs = currentTimeNs();
    return self.queryAt(querySet, nowNs);
}

pub fn queryAt(self: *Translator, querySet: Parser.QuerySet, nowNs: u64) TranslateError!Loql.Query {
    _ = querySet.pipes; // ignored for now by design

    const range = try translateRange(querySet.timeRange, nowNs);

    const tagsExpr = try self.translateFilterExpression(querySet.tags);
    const fieldsExpr = try self.translateFilterExpression(querySet.query);

    const tagsLegacy = try self.legacyConjunctiveEquals(querySet.tags);
    const fieldsLegacy = try self.legacyConjunctiveEquals(querySet.query);

    return .{
        .startTimeNs = range.start,
        .endTimeNs = range.end,
        .tags = tagsLegacy,
        .fields = fieldsLegacy,
        .tagsExpr = tagsExpr,
        .fieldsExpr = fieldsExpr,
    };
}

const TranslatedRange = struct {
    start: u64,
    end: u64,
};

fn translateRange(maybeExpr: ?Parser.Expression, nowNs: u64) TranslateError!TranslatedRange {
    const expr = maybeExpr orelse {
        const fiveMinutes = 5 * std.time.ns_per_min;
        return .{
            .start = if (nowNs > fiveMinutes) nowNs - fiveMinutes else 0,
            .end = nowNs,
        };
    };

    return switch (expr) {
        .range => |bounds| blk: {
            const startNs = try parseTimeValue(bounds[0], nowNs, .start);
            const endNs = try parseTimeValue(bounds[1], nowNs, .end);
            if (startNs > endNs) return error.InvalidRange;
            break :blk .{ .start = startNs, .end = endNs };
        },
        else => error.UnsupportedExpression,
    };
}

const RangeBound = enum { start, end };

fn parseTimeValue(raw: []const u8, nowNs: u64, bound: RangeBound) TranslateError!u64 {
    if (raw.len == 0) {
        return switch (bound) {
            .start => 0,
            .end => std.math.maxInt(u64),
        };
    }

    if (std.mem.eql(u8, raw, "now")) {
        return nowNs;
    }

    if (raw[0] == '-' or raw[0] == '+') {
        return parseRelativeTimeNs(raw, nowNs);
    }

    return std.fmt.parseInt(u64, raw, 10) catch error.InvalidTimestamp;
}

fn parseRelativeTimeNs(raw: []const u8, nowNs: u64) TranslateError!u64 {
    if (raw.len < 3) {
        return error.InvalidDuration;
    }

    const sign = raw[0];
    const durationNs = try parseDurationNs(raw[1..]);

    if (sign == '-') {
        return if (durationNs > nowNs) 0 else nowNs - durationNs;
    }

    return std.math.add(u64, nowNs, durationNs) catch error.InvalidDuration;
}

fn parseDurationNs(raw: []const u8) TranslateError!u64 {
    if (raw.len < 2) {
        return error.InvalidDuration;
    }

    const unit = raw[raw.len - 1];
    const valueRaw = raw[0 .. raw.len - 1];
    const value = std.fmt.parseInt(u64, valueRaw, 10) catch return error.InvalidDuration;

    const multiplier: u64 = switch (unit) {
        's' => std.time.ns_per_s,
        'm' => std.time.ns_per_min,
        'h' => std.time.ns_per_hour,
        'd' => std.time.ns_per_day,
        else => return error.InvalidDuration,
    };

    return std.math.mul(u64, value, multiplier) catch error.InvalidDuration;
}

fn translateFilterExpression(self: *Translator, maybeExpr: ?Parser.Expression) TranslateError!?*const Loql.FilterExpression {
    const expr = maybeExpr orelse return null;
    return try self.translateExpression(expr);
}

fn translateExpression(self: *Translator, expr: Parser.Expression) TranslateError!*const Loql.FilterExpression {
    return switch (expr) {
        .equalOp => |pair| try self.translatePredicate(pair[0].*, pair[1].*, .equal),
        .notEqualOp => |pair| try self.translatePredicate(pair[0].*, pair[1].*, .not_equal),
        .andOp => |pair| blk: {
            const left = try self.translateExpression(pair[0].*);
            const right = try self.translateExpression(pair[1].*);
            break :blk try self.allocFilterExpression(.{ .andOp = .{ left, right } });
        },
        .orOp => |pair| blk: {
            const left = try self.translateExpression(pair[0].*);
            const right = try self.translateExpression(pair[1].*);
            break :blk try self.allocFilterExpression(.{ .orOp = .{ left, right } });
        },
        .grouping => |inner| blk: {
            const nested = try self.translateExpression(inner.*);
            break :blk try self.allocFilterExpression(.{ .grouping = nested });
        },
        else => error.UnsupportedExpression,
    };
}

fn translatePredicate(
    self: *Translator,
    left: Parser.Expression,
    right: Parser.Expression,
    op: Loql.MatchOp,
) TranslateError!*const Loql.FilterExpression {
    const key = switch (left) {
        .literal => |v| v,
        else => return error.UnsupportedExpression,
    };

    const value = switch (right) {
        .literal => |v| v,
        else => return error.UnsupportedExpression,
    };

    return self.allocFilterExpression(.{ .predicate = .{
        .key = key,
        .value = value,
        .op = op,
    } });
}

fn allocFilterExpression(self: *Translator, expr: Loql.FilterExpression) TranslateError!*const Loql.FilterExpression {
    try self.garbage.ensureUnusedCapacity(self.allocator, 1);

    const node = try self.allocator.create(Loql.FilterExpression);
    errdefer self.allocator.destroy(node);

    self.garbage.appendAssumeCapacity(node);
    node.* = expr;
    return node;
}

fn legacyConjunctiveEquals(self: *Translator, maybeExpr: ?Parser.Expression) TranslateError![]const Field {
    const expr = maybeExpr orelse return &.{};

    var collected: std.ArrayList(Field) = .empty;
    defer collected.deinit(self.allocator);

    const representable = try self.collectLegacyAndEquals(expr, &collected);
    if (!representable or collected.items.len == 0) {
        return &.{};
    }

    const dst = try self.allocator.alloc(Field, collected.items.len);
    @memcpy(dst, collected.items);
    try self.fieldGarbage.append(self.allocator, dst);
    return dst;
}

fn collectLegacyAndEquals(
    self: *Translator,
    expr: Parser.Expression,
    dst: *std.ArrayList(Field),
) TranslateError!bool {
    return switch (expr) {
        .grouping => |inner| try self.collectLegacyAndEquals(inner.*, dst),
        .andOp => |pair| blk: {
            const leftOk = try self.collectLegacyAndEquals(pair[0].*, dst);
            const rightOk = try self.collectLegacyAndEquals(pair[1].*, dst);
            break :blk leftOk and rightOk;
        },
        .equalOp => |pair| blk: {
            const key = switch (pair[0].*) {
                .literal => |v| v,
                else => return false,
            };
            const value = switch (pair[1].*) {
                .literal => |v| v,
                else => return false,
            };
            try dst.append(self.allocator, .{ .key = key, .value = value });
            break :blk true;
        },
        else => false,
    };
}

fn currentTimeNs() u64 {
    return @intCast(std.time.nanoTimestamp());
}

fn expectPredicate(expr: *const Loql.FilterExpression, key: []const u8, value: []const u8, op: Loql.MatchOp) !void {
    switch (expr.*) {
        .predicate => |p| {
            try testing.expectEqualStrings(key, p.key);
            try testing.expectEqualStrings(value, p.value);
            try testing.expectEqual(op, p.op);
        },
        else => return error.TestUnexpectedResult,
    }
}

test "Translator.queryAt translates simple tags and fields" {
    const allocator = testing.allocator;

    var translator = Translator.init(allocator);
    defer translator.deinit();

    var tagKey: Parser.Expression = .{ .literal = "env" };
    var tagValue: Parser.Expression = .{ .literal = "prod" };
    const tagsExpr: Parser.Expression = .{ .equalOp = .{ &tagKey, &tagValue } };

    var fieldKey: Parser.Expression = .{ .literal = "level" };
    var fieldValue: Parser.Expression = .{ .literal = "error" };
    const queryExpr: Parser.Expression = .{ .equalOp = .{ &fieldKey, &fieldValue } };

    const querySet: Parser.QuerySet = .{
        .timeRange = .{ .range = .{ "-5m", "now" } },
        .tags = tagsExpr,
        .query = queryExpr,
        .pipes = .empty,
    };

    const nowNs: u64 = 10 * std.time.ns_per_min;
    const translated = try translator.queryAt(querySet, nowNs);

    try testing.expectEqual(nowNs - (5 * std.time.ns_per_min), translated.startTimeNs);
    try testing.expectEqual(nowNs, translated.endTimeNs);

    try testing.expectEqual(@as(usize, 1), translated.tags.len);
    try testing.expectEqualStrings("env", translated.tags[0].key);
    try testing.expectEqualStrings("prod", translated.tags[0].value);

    try testing.expectEqual(@as(usize, 1), translated.fields.len);
    try testing.expectEqualStrings("level", translated.fields[0].key);
    try testing.expectEqualStrings("error", translated.fields[0].value);

    try testing.expect(translated.tagsExpr != null);
    try testing.expect(translated.fieldsExpr != null);

    try expectPredicate(translated.tagsExpr.?, "env", "prod", .equal);
    try expectPredicate(translated.fieldsExpr.?, "level", "error", .equal);
}

test "Translator translates boolean trees and keeps legacy filters empty for OR" {
    const allocator = testing.allocator;

    var translator = Translator.init(allocator);
    defer translator.deinit();

    var keyA: Parser.Expression = .{ .literal = "service" };
    var valA: Parser.Expression = .{ .literal = "api" };
    var eqA: Parser.Expression = .{ .equalOp = .{ &keyA, &valA } };

    var keyB: Parser.Expression = .{ .literal = "service" };
    var valB: Parser.Expression = .{ .literal = "worker" };
    var neqB: Parser.Expression = .{ .notEqualOp = .{ &keyB, &valB } };

    const tagsRoot: Parser.Expression = .{ .orOp = .{ &eqA, &neqB } };

    const querySet: Parser.QuerySet = .{
        .timeRange = .{ .range = .{ "0", "100" } },
        .tags = tagsRoot,
        .query = null,
        .pipes = .empty,
    };

    const translated = try translator.queryAt(querySet, 1000);

    try testing.expectEqual(@as(usize, 0), translated.tags.len);
    try testing.expect(translated.tagsExpr != null);

    switch (translated.tagsExpr.?.*) {
        .orOp => |pair| {
            try expectPredicate(pair[0], "service", "api", .equal);
            try expectPredicate(pair[1], "service", "worker", .not_equal);
        },
        else => return error.TestUnexpectedResult,
    }
}

test "Translator.queryAt rejects unsupported expression" {
    const allocator = testing.allocator;

    var translator = Translator.init(allocator);
    defer translator.deinit();

    const querySet: Parser.QuerySet = .{
        .timeRange = .{ .range = .{ "-5m", "now" } },
        .tags = .{ .literal = "env" },
        .query = null,
        .pipes = .empty,
    };

    try testing.expectError(error.UnsupportedExpression, translator.queryAt(querySet, 100));
}
