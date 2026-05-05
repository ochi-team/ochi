pub const Query = @This();

startTimeNs: u64,
endTimeNs: u64,

// v2 boolean filter trees used by the translator
tagsExpr: ?*const FilterExpression = null,
fieldsExpr: ?*const FilterExpression = null,

pub fn validate(q: *const Query) !void {
    if (q.startTimeNs >= q.endTimeNs) {
        return error.InvalidTimeRange;
    }

    if (q.tagsExpr) |tags| try tags.validate();
}

pub const InvalidQueryError = error{
    InvalidTimeRange,
    UnsupportedTagOperator,
};

pub const MatchOp = enum {
    equal,
    notEq,
    matchRegex,
    notMatchRegex,
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

    pub fn validate(filter: *const FilterExpression) InvalidQueryError!void {
        switch (filter.*) {
            .predicate => |p| if (p.op != .equal and p.op != .notEq) {
                return error.UnsupportedTagOperator;
            },
            .andOp => |ops| {
                try ops[0].validate();
                try ops[1].validate();
            },
            .orOp => |ops| {
                try ops[0].validate();
                try ops[1].validate();
            },
            .grouping => |inner| try inner.validate(),
        }
    }
};
