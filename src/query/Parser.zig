const std = @import("std");
const Allocator = std.mem.Allocator;

const Token = @import("Scanner.zig").Token;
const TokenKind = @import("Scanner.zig").TokenKind;
const Error = @import("Scanner.zig").Error;
const ErrorReporter = @import("ErrorReporter.zig");

pub const expectCloseParenthesis = "Expect ')' after expression.";
pub const expectCloseCurlyBracket = "Expect '}' after tags.";
pub const expectOpenSquareBracket = "Expect '[' before time range.";
pub const expectCloseSquareBracket = "Expect ']' after time range.";
pub const expectComaBetweenTimeValues = "Expect ',' between time range values.";
pub const expectExpression = "Expect expression.";

// TODO: benchmark if *[2]Expression gives better locality,
// do the same for FilterExpression
pub const Expression = union(enum) {
    equalOp: [2]*const Expression,
    notEqualOp: [2]*const Expression,
    andOp: [2]*const Expression,
    orOp: [2]*const Expression,
    grouping: *const Expression,
    literal: []const u8,

    // regex are applied only to query and never to tags
    matchRegexOp: [2]*const Expression,
    notMatchRegexOp: [2]*const Expression,
};

pub const PipeEpxpression = struct {};

pub const TimeRangeExpression = [2]TimeValue;
pub const TimeValue = union(enum) {
    timestamp: []const u8,
    duration: []const u8,
    now: void,
};

pub const QuerySet = struct {
    timeRange: TimeRangeExpression,
    tags: ?Expression,
    query: ?Expression,
    pipes: std.ArrayList(PipeEpxpression) = .empty,
};

pub const Parser = @This();

pub const ParseError = Error || error{OutOfMemory};

// state
current: usize = 0,

// TODO: we might want to use a pool here to create a bunch of expressions,
// better to emter how many we create them per query
// we expect to call query only in read API using an arena allocator, perhaps we get can rid of it,
// apply the same to Translator
garbage: std.ArrayList(*Expression) = .empty,

pub fn deinit(self: *Parser, allocator: Allocator) void {
    for (self.garbage.items) |node| {
        allocator.destroy(node);
    }
    self.garbage.deinit(allocator);
}

pub fn querySet(
    self: *Parser,
    allocator: Allocator,
    tokens: []const Token,
    reporter: *ErrorReporter,
) ParseError!QuerySet {
    const range = try self.timeRange(tokens, reporter);
    const ts = try self.tags(allocator, tokens, reporter);
    const q = try self.query(allocator, tokens, reporter);
    const ps = try self.pipes(allocator, tokens);

    return .{
        .timeRange = range,
        .tags = ts,
        .query = q,
        .pipes = ps,
    };
}

fn timeRange(self: *Parser, tokens: []const Token, reporter: *ErrorReporter) ParseError!TimeRangeExpression {
    try self.consume(tokens, .LeftSquareBracket, expectOpenSquareBracket, reporter);
    const leftHand = try self.time(tokens, reporter);
    try self.consume(tokens, .Comma, expectComaBetweenTimeValues, reporter);
    const rightHand = try self.time(tokens, reporter);
    try self.consume(tokens, .RightSquareBracket, expectCloseSquareBracket, reporter);

    if (leftHand == null and rightHand == null) {
        const line, const col = self.currentPosition(tokens);
        _ = reporter.reportSyntaxError(.{
            .line = line,
            .col = col,
            .message = "At least one of the time range values must be specified.",
        });
        return Error.SyntaxError;
    }

    const left: TimeValue = leftHand orelse .{ .now = {} };
    const right: TimeValue = rightHand orelse .{ .now = {} };

    return .{ left, right };
}

fn time(self: *Parser, tokens: []const Token, reporter: *ErrorReporter) ParseError!?TimeValue {
    switch (tokens[self.current].kind) {
        // one of the values is omited, e.g. [-5m,]
        .RightSquareBracket, .Comma => {
            return null;
        },
        else => {},
    }

    if (self.match(tokens, &.{.Literal})) {
        const token = tokens[self.current - 1];
        if (std.mem.eql(u8, token.lexeme, "now")) {
            return .{ .now = {} };
        }
        // if it ends at duration symbols than parser as a duration literal
        switch (token.lexeme[token.lexeme.len - 1]) {
            's', 'm', 'h', 'd' => return .{ .duration = token.lexeme },
            else => return .{ .timestamp = token.lexeme },
        }
    }

    _ = reporter.reportSyntaxError(.{
        .line = tokens[self.current].line,
        .col = tokens[self.current].col,
        .message = "Expect time value.",
    });
    return Error.SyntaxError;
}

fn tags(self: *Parser, allocator: Allocator, tokens: []const Token, reporter: *ErrorReporter) ParseError!?Expression {
    if (!self.match(tokens, &.{.LeftCurlyBracket})) {
        return null;
    }

    const expr = try self.boolean(allocator, tokens, &.{ .Equal, .NotEqual }, reporter);
    try self.consume(tokens, .RightCurlyBracket, expectCloseCurlyBracket, reporter);

    return expr;
}

fn query(self: *Parser, allocator: Allocator, tokens: []const Token, reporter: *ErrorReporter) ParseError!?Expression {
    if (self.current >= tokens.len) {
        return null;
    }

    if (tokens[self.current].kind == .Pipe) {
        return null;
    }

    const expr = try self.boolean(allocator, tokens, &.{ .Equal, .NotEqual, .MatchRegex, .NotMatchRegex }, reporter);
    return expr;
}

fn pipes(self: *Parser, allocator: Allocator, tokens: []const Token) ParseError!std.ArrayList(PipeEpxpression) {
    if (tokens[self.current..].len > 0) {
        // TODO: implement me
        unreachable;
    }

    _ = allocator;
    return .empty;
}

// TODO: ideally we make the parser not recursive and implement a linter rule to ban recursion,
// it limits the stack buffers which we could actively use since we know the max query size
fn boolean(
    self: *Parser,
    allocator: Allocator,
    tokens: []const Token,
    allowsOps: []const TokenKind,
    reporter: *ErrorReporter,
) ParseError!Expression {
    var expr = try self.conjunction(allocator, tokens, allowsOps, reporter);

    while (self.match(tokens, &.{.Or})) {
        const right = try self.conjunction(allocator, tokens, allowsOps, reporter);

        const leftNode = try self.allocExpression(allocator, expr);
        const rightNode = try self.allocExpression(allocator, right);

        expr = .{ .orOp = .{ leftNode, rightNode } };
    }

    return expr;
}

fn conjunction(
    self: *Parser,
    allocator: Allocator,
    tokens: []const Token,
    allowsOps: []const TokenKind,
    reporter: *ErrorReporter,
) ParseError!Expression {
    var expr = try self.equality(allocator, tokens, allowsOps, reporter);

    while (self.match(tokens, &.{.And}) or self.matchEquality(tokens, allowsOps)) {
        const right = try self.equality(allocator, tokens, allowsOps, reporter);

        const leftNode = try self.allocExpression(allocator, expr);
        const rightNode = try self.allocExpression(allocator, right);

        expr = .{ .andOp = .{ leftNode, rightNode } };
    }

    return expr;
}

fn matchEquality(self: *const Parser, tokens: []const Token, allowsOps: []const TokenKind) bool {
    if (self.current >= tokens.len) {
        return false;
    }

    // identifies it starts with <Literal, Op>
    switch (tokens[self.current].kind) {
        .LeftParenthesis => return true,
        .Literal => {
            if (self.current + 1 >= tokens.len) {
                return false;
            }

            for (allowsOps) |op| {
                if (tokens[self.current + 1].kind == op) {
                    return true;
                }
            }

            return false;
        },
        else => return false,
    }
}

fn equality(
    self: *Parser,
    allocator: Allocator,
    tokens: []const Token,
    allowsOps: []const TokenKind,
    reporter: *ErrorReporter,
) ParseError!Expression {
    const start = self.current;
    var expr = try self.primary(allocator, tokens, allowsOps, reporter);

    while (self.match(tokens, allowsOps)) {
        const op = tokens[self.current - 1].kind;
        const right = try self.primary(allocator, tokens, allowsOps, reporter);

        const leftNode = try self.allocExpression(allocator, expr);
        const rightNode = try self.allocExpression(allocator, right);

        expr = switch (op) {
            .Equal => .{ .equalOp = .{ leftNode, rightNode } },
            .NotEqual => .{ .notEqualOp = .{ leftNode, rightNode } },
            .MatchRegex => .{ .matchRegexOp = .{ leftNode, rightNode } },
            .NotMatchRegex => .{ .notMatchRegexOp = .{ leftNode, rightNode } },
            else => {
                const line, const col = self.currentPosition(tokens);
                _ = reporter.reportSyntaxError(.{
                    .line = line,
                    .col = col,
                    .message = "Unexpected operator.",
                });
                return Error.SyntaxError;
            },
        };
    }

    switch (expr) {
        .literal => {
            if (!self.atOperator(tokens)) {
                _ = reporter.reportSyntaxError(.{
                    .line = tokens[start].line,
                    .col = tokens[start].col,
                    .message = expectExpression,
                });
                return Error.SyntaxError;
            }
        },
        else => {},
    }

    return expr;
}

fn atOperator(self: *const Parser, tokens: []const Token) bool {
    if (self.current >= tokens.len) {
        return false;
    }

    return switch (tokens[self.current].kind) {
        .Equal, .NotEqual, .MatchRegex, .NotMatchRegex => true,
        else => false,
    };
}

fn primary(
    self: *Parser,
    allocator: Allocator,
    tokens: []const Token,
    allowsOps: []const TokenKind,
    reporter: *ErrorReporter,
) ParseError!Expression {
    if (self.match(tokens, &.{.Literal})) {
        return .{ .literal = tokens[self.current - 1].lexeme };
    }

    if (self.match(tokens, &.{.LeftParenthesis})) {
        const expr = try self.boolean(allocator, tokens, allowsOps, reporter);
        try self.consume(tokens, .RightParenthesis, expectCloseParenthesis, reporter);

        const node = try self.allocExpression(allocator, expr);
        return .{ .grouping = node };
    }

    const line, const col = self.currentPosition(tokens);
    _ = reporter.reportSyntaxError(.{
        .line = line,
        .col = col,
        .message = expectExpression,
    });
    return Error.SyntaxError;
}

fn match(self: *Parser, tokens: []const Token, types: []const TokenKind) bool {
    for (types) |t| {
        if (self.current < tokens.len and tokens[self.current].kind == t) {
            self.current += 1;
            return true;
        }
    }

    return false;
}

fn consume(
    self: *Parser,
    tokens: []const Token,
    kind: TokenKind,
    message: []const u8,
    reporter: *ErrorReporter,
) Error!void {
    if (self.current < tokens.len and tokens[self.current].kind == kind) {
        self.current += 1;
        return;
    }

    const line, const col = self.currentPosition(tokens);

    _ = reporter.reportSyntaxError(.{
        .line = line,
        .col = col,
        .message = message,
    });
    return Error.SyntaxError;
}

fn currentPosition(self: *const Parser, tokens: []const Token) struct { u16, u16 } {
    if (tokens.len == 0) {
        return .{ 1, 1 };
    }

    if (self.current < tokens.len) {
        return .{ tokens[self.current].line, tokens[self.current].col };
    }

    const last = tokens[tokens.len - 1];
    return .{ last.line, last.col };
}

fn allocExpression(self: *Parser, allocator: Allocator, expr: Expression) !*Expression {
    try self.garbage.ensureUnusedCapacity(allocator, 1);

    const node = try allocator.create(Expression);
    errdefer allocator.destroy(node);

    self.garbage.appendAssumeCapacity(node);
    node.* = expr;
    return node;
}

test {
    _ = @import("Parser_test.zig");
}
