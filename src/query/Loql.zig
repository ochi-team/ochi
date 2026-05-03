/// Logql is a Logs Ochi Query Language,
/// translates a query to ochi query API
const std = @import("std");

const Field = @import("../store/lines.zig").Field;

const Scanner = @import("Scanner.zig");
const ErrorReporter = @import("ErrorReporter.zig");

pub const Query = struct {
    startTimeNs: u64,
    endTimeNs: u64,

    // legacy exact-match filters, preserved for backward compatibility
    tags: []const Field,
    fields: []const Field,

    // v2 boolean filter trees used by the translator
    tagsExpr: ?*const FilterExpression = null,
    fieldsExpr: ?*const FilterExpression = null,
};

pub const MatchOp = enum {
    equal,
    not_equal,
    match_regex,
    not_match_regex,
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
    grouping: *const FilterExpression,
};

const Loql = @This();

scanner: Scanner,
reporter: ErrorReporter,

// TODO: it must accept a reader, not a query string
pub fn translateQuery(self: *const Loql, fullQuery: []const u8) ![]u8 {
    var query = std.mem.trim(u8, fullQuery, " ");
    query = std.mem.trim(u8, query, "\n");
    query = std.mem.trim(u8, query, "\t");
    for (query, 0..) |c, i| {
        query[i] = std.ascii.toLower(c);
    }

    // TODO: validate whether it's a syntax error and return report content
    // or unknown error
    // TODO: most likely scanner has a state, we must reset it
    const tokens = try self.scanner.scan(query, &self.reporter);
    for (tokens) |token| {
        std.debug.print("Token: {s}\n", .{token});
    }
}
