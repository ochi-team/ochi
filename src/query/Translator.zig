const std = @import("std");
const Allocator = std.mem.Allocator;

const Query = @import("Query.zig");
const Expression = @import("Parser.zig").Expression;

pub const TranslateError = error{};

const Translator = @This();

pub fn query(allocator: Allocator, expr: Expression) TranslateError!Query {
    _ = allocator;
    _ = expr;

    const q: Query = .{
        .startTimeNs = 0,
        .endTimeNs = 0,
        .tagsExpr = null,
        .fieldsExpr = null,
    };

    try q.validateQuery();
    return q;
}
