const std = @import("std");
const Allocator = std.mem.Allocator;

const Token = @import("Scanner.zig").Token;
const TokenKind = @import("Scanner.zig").TokenKind;
const Error = @import("Scanner.zig").Error;
const ErrorReporter = @import("ErrorReporter.zig");

const expectCloseParenthesis = "Expect ')' after expression.";
const expectOpenCurlyBracket = "Expect '{' before tags.";
const expectCloseCurlyBracket = "Expect '}' after tags.";
const expectOpenSquareBracket = "Expect '[' before time range.";
const expectCloseSquareBracket = "Expect ']' after time range.";
const expectComaBetweenTimeValues = "Expect ',' between time range values.";
const expectExpression = "Expect expression.";

const maxParseIterations = 512;

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

// TODO: ideally we make the parser not recursive and implement a linter rule to ban recursion
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

    return expr;
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

const testing = std.testing;

test "Parser.expression" {
    const allocator = testing.allocator;

    const Case = struct {
        query: []const Token,
        expectedQuerySet: ?QuerySet = null,
        expectedErr: ?anyerror = null,
        expectedSyntaxErrors: []const ErrorReporter.SyntaxError,
    };

    const cases = [_]Case{
        // simple query
        .{
            .query = &.{
                .{ .kind = .LeftSquareBracket, .lexeme = "[", .line = 1, .col = 1 },
                .{ .kind = .Literal, .lexeme = "-5m", .line = 1, .col = 2 },
                .{ .kind = .Comma, .lexeme = ",", .line = 1, .col = 5 },
                .{ .kind = .RightSquareBracket, .lexeme = "]", .line = 1, .col = 6 },
                .{ .kind = .LeftCurlyBracket, .lexeme = "{", .line = 1, .col = 1 },
                .{ .kind = .Literal, .lexeme = "env", .line = 1, .col = 2 },
                .{ .kind = .Equal, .lexeme = "=", .line = 1, .col = 5 },
                .{ .kind = .Literal, .lexeme = "prod", .line = 1, .col = 6 },
                .{ .kind = .RightCurlyBracket, .lexeme = "}", .line = 1, .col = 10 },
                .{ .kind = .Literal, .lexeme = "field", .line = 1, .col = 11 },
                .{ .kind = .Equal, .lexeme = "=", .line = 1, .col = 16 },
                .{ .kind = .Literal, .lexeme = "value", .line = 1, .col = 17 },
            },
            .expectedQuerySet = .{
                .timeRange = .{ .{ .duration = "-5m" }, .{ .now = {} } },
                .tags = .{ .equalOp = .{
                    &.{ .literal = "env" },
                    &.{ .literal = "prod" },
                } },
                .query = .{ .equalOp = .{
                    &.{ .literal = "field" },
                    &.{ .literal = "value" },
                } },
                .pipes = .empty,
            },
            .expectedSyntaxErrors = &.{},
        },
        // null tags
        .{
            .query = &.{
                .{ .kind = .LeftSquareBracket, .lexeme = "[", .line = 1, .col = 1 },
                .{ .kind = .Literal, .lexeme = "-5m", .line = 1, .col = 2 },
                .{ .kind = .Comma, .lexeme = ",", .line = 1, .col = 5 },
                .{ .kind = .RightSquareBracket, .lexeme = "]", .line = 1, .col = 6 },
                .{ .kind = .Literal, .lexeme = "field", .line = 1, .col = 11 },
                .{ .kind = .Equal, .lexeme = "=", .line = 1, .col = 16 },
                .{ .kind = .Literal, .lexeme = "value", .line = 1, .col = 17 },
            },
            .expectedQuerySet = .{
                .timeRange = .{ .{ .duration = "-5m" }, .{ .now = {} } },
                .tags = null,
                .query = .{ .equalOp = .{
                    &.{ .literal = "field" },
                    &.{ .literal = "value" },
                } },
                .pipes = .empty,
            },
            .expectedSyntaxErrors = &.{},
        },
        // implicit AND between adjacent equality expressions
        .{
            .query = &.{
                .{ .kind = .LeftSquareBracket, .lexeme = "[", .line = 1, .col = 1 },
                .{ .kind = .Literal, .lexeme = "-5m", .line = 1, .col = 2 },
                .{ .kind = .Comma, .lexeme = ",", .line = 1, .col = 5 },
                .{ .kind = .RightSquareBracket, .lexeme = "]", .line = 1, .col = 6 },
                .{ .kind = .Literal, .lexeme = "env", .line = 1, .col = 8 },
                .{ .kind = .Equal, .lexeme = "=", .line = 1, .col = 11 },
                .{ .kind = .Literal, .lexeme = "prod", .line = 1, .col = 12 },
                .{ .kind = .Literal, .lexeme = "message", .line = 1, .col = 17 },
                .{ .kind = .Equal, .lexeme = "=", .line = 1, .col = 24 },
                .{ .kind = .Literal, .lexeme = "err", .line = 1, .col = 25 },
            },
            .expectedQuerySet = .{
                .timeRange = .{ .{ .duration = "-5m" }, .{ .now = {} } },
                .tags = null,
                .query = .{ .andOp = .{
                    &.{ .equalOp = .{
                        &.{ .literal = "env" },
                        &.{ .literal = "prod" },
                    } },
                    &.{ .equalOp = .{
                        &.{ .literal = "message" },
                        &.{ .literal = "err" },
                    } },
                } },
                .pipes = .empty,
            },
            .expectedSyntaxErrors = &.{},
        },
        // test no parentheses of the query
        .{
            .query = &.{
                .{ .kind = .LeftSquareBracket, .lexeme = "[", .line = 1, .col = 1 },
                .{ .kind = .Literal, .lexeme = "-5m", .line = 1, .col = 2 },
                .{ .kind = .Comma, .lexeme = ",", .line = 1, .col = 5 },
                .{ .kind = .RightSquareBracket, .lexeme = "]", .line = 1, .col = 6 },
                .{ .kind = .LeftCurlyBracket, .lexeme = "{", .line = 1, .col = 1 },
                .{ .kind = .Literal, .lexeme = "env", .line = 1, .col = 2 },
                .{ .kind = .Equal, .lexeme = "=", .line = 1, .col = 5 },
                .{ .kind = .Literal, .lexeme = "prod", .line = 1, .col = 6 },
                .{ .kind = .And, .lexeme = "and", .line = 1, .col = 11 },
                .{ .kind = .Literal, .lexeme = "service", .line = 1, .col = 15 },
                .{ .kind = .Equal, .lexeme = "=", .line = 1, .col = 22 },
                .{ .kind = .Literal, .lexeme = "api", .line = 1, .col = 23 },
                .{ .kind = .RightCurlyBracket, .lexeme = "}", .line = 1, .col = 26 },
                .{ .kind = .Literal, .lexeme = "field", .line = 1, .col = 2 },
                .{ .kind = .NotEqual, .lexeme = "!=", .line = 1, .col = 5 },
                .{ .kind = .Literal, .lexeme = "value", .line = 1, .col = 6 },
                .{ .kind = .Or, .lexeme = "or", .line = 1, .col = 11 },
                .{ .kind = .Literal, .lexeme = "call", .line = 1, .col = 15 },
                .{ .kind = .Equal, .lexeme = "=", .line = 1, .col = 22 },
                .{ .kind = .Literal, .lexeme = "get", .line = 1, .col = 23 },
            },
            .expectedQuerySet = .{
                .timeRange = .{ .{ .duration = "-5m" }, .{ .now = {} } },
                .tags = .{ .andOp = .{
                    &.{ .equalOp = .{
                        &.{ .literal = "env" },
                        &.{ .literal = "prod" },
                    } },
                    &.{ .equalOp = .{
                        &.{ .literal = "service" },
                        &.{ .literal = "api" },
                    } },
                } },
                .query = .{ .orOp = .{
                    &.{ .notEqualOp = .{
                        &.{ .literal = "field" },
                        &.{ .literal = "value" },
                    } },
                    &.{ .equalOp = .{
                        &.{ .literal = "call" },
                        &.{ .literal = "get" },
                    } },
                } },
                .pipes = .empty,
            },
            .expectedSyntaxErrors = &.{},
        },
        // test no query, only and/or of the tags
        .{
            .query = &.{
                .{ .kind = .LeftSquareBracket, .lexeme = "[", .line = 1, .col = 1 },
                .{ .kind = .Literal, .lexeme = "-5m", .line = 1, .col = 2 },
                .{ .kind = .Comma, .lexeme = ",", .line = 1, .col = 5 },
                .{ .kind = .RightSquareBracket, .lexeme = "]", .line = 1, .col = 6 },
                .{ .kind = .LeftCurlyBracket, .lexeme = "{", .line = 1, .col = 1 },
                .{ .kind = .Literal, .lexeme = "env", .line = 1, .col = 2 },
                .{ .kind = .Equal, .lexeme = "=", .line = 1, .col = 5 },
                .{ .kind = .Literal, .lexeme = "prod", .line = 1, .col = 6 },
                .{ .kind = .Or, .lexeme = "or", .line = 1, .col = 11 },
                .{ .kind = .Literal, .lexeme = "service", .line = 1, .col = 14 },
                .{ .kind = .Equal, .lexeme = "=", .line = 1, .col = 21 },
                .{ .kind = .Literal, .lexeme = "api", .line = 1, .col = 22 },
                .{ .kind = .And, .lexeme = "and", .line = 1, .col = 26 },
                .{ .kind = .Literal, .lexeme = "host", .line = 1, .col = 30 },
                .{ .kind = .Equal, .lexeme = "=", .line = 1, .col = 34 },
                .{ .kind = .Literal, .lexeme = "web", .line = 1, .col = 35 },
                .{ .kind = .RightCurlyBracket, .lexeme = "}", .line = 1, .col = 38 },
            },
            .expectedQuerySet = .{
                .timeRange = .{ .{ .duration = "-5m" }, .{ .now = {} } },
                .tags = .{ .orOp = .{
                    &.{ .equalOp = .{
                        &.{ .literal = "env" },
                        &.{ .literal = "prod" },
                    } },
                    &.{ .andOp = .{
                        &.{ .equalOp = .{
                            &.{ .literal = "service" },
                            &.{ .literal = "api" },
                        } },
                        &.{ .equalOp = .{
                            &.{ .literal = "host" },
                            &.{ .literal = "web" },
                        } },
                    } },
                } },
                .query = null,
                .pipes = .empty,
            },
            .expectedSyntaxErrors = &.{},
        },
        // test operator precedence and grouping
        .{
            .query = &.{
                .{ .kind = .LeftSquareBracket, .lexeme = "[", .line = 1, .col = 1 },
                .{ .kind = .Literal, .lexeme = "-5m", .line = 1, .col = 2 },
                .{ .kind = .Comma, .lexeme = ",", .line = 1, .col = 5 },
                .{ .kind = .RightSquareBracket, .lexeme = "]", .line = 1, .col = 6 },
                .{ .kind = .LeftCurlyBracket, .lexeme = "{", .line = 1, .col = 1 },
                .{ .kind = .LeftParenthesis, .lexeme = "(", .line = 1, .col = 2 },
                .{ .kind = .Literal, .lexeme = "env", .line = 1, .col = 3 },
                .{ .kind = .Equal, .lexeme = "=", .line = 1, .col = 6 },
                .{ .kind = .Literal, .lexeme = "prod", .line = 1, .col = 7 },
                .{ .kind = .Or, .lexeme = "or", .line = 1, .col = 12 },
                .{ .kind = .Literal, .lexeme = "service", .line = 1, .col = 15 },
                .{ .kind = .Equal, .lexeme = "=", .line = 1, .col = 22 },
                .{ .kind = .Literal, .lexeme = "api", .line = 1, .col = 23 },
                .{ .kind = .RightParenthesis, .lexeme = ")", .line = 1, .col = 26 },
                .{ .kind = .And, .lexeme = "and", .line = 1, .col = 28 },
                .{ .kind = .LeftParenthesis, .lexeme = "(", .line = 1, .col = 32 },
                .{ .kind = .Literal, .lexeme = "host", .line = 1, .col = 33 },
                .{ .kind = .Equal, .lexeme = "=", .line = 1, .col = 37 },
                .{ .kind = .Literal, .lexeme = "web", .line = 1, .col = 38 },
                .{ .kind = .Or, .lexeme = "or", .line = 1, .col = 42 },
                .{ .kind = .Literal, .lexeme = "host", .line = 1, .col = 45 },
                .{ .kind = .Equal, .lexeme = "=", .line = 1, .col = 49 },
                .{ .kind = .Literal, .lexeme = "api", .line = 1, .col = 50 },
                .{ .kind = .RightParenthesis, .lexeme = ")", .line = 1, .col = 53 },
                .{ .kind = .RightCurlyBracket, .lexeme = "}", .line = 1, .col = 54 },
                .{ .kind = .LeftParenthesis, .lexeme = "(", .line = 1, .col = 2 },
                .{ .kind = .Literal, .lexeme = "call", .line = 1, .col = 3 },
                .{ .kind = .Equal, .lexeme = "=", .line = 1, .col = 6 },
                .{ .kind = .Literal, .lexeme = "get", .line = 1, .col = 7 },
                .{ .kind = .Or, .lexeme = "or", .line = 1, .col = 12 },
                .{ .kind = .Literal, .lexeme = "boost", .line = 1, .col = 15 },
                .{ .kind = .Equal, .lexeme = "=", .line = 1, .col = 22 },
                .{ .kind = .Literal, .lexeme = "yes", .line = 1, .col = 23 },
                .{ .kind = .RightParenthesis, .lexeme = ")", .line = 1, .col = 26 },
                .{ .kind = .And, .lexeme = "and", .line = 1, .col = 28 },
                .{ .kind = .LeftParenthesis, .lexeme = "(", .line = 1, .col = 32 },
                .{ .kind = .Literal, .lexeme = "url", .line = 1, .col = 33 },
                .{ .kind = .Equal, .lexeme = "=", .line = 1, .col = 37 },
                .{ .kind = .Literal, .lexeme = "one", .line = 1, .col = 38 },
                .{ .kind = .Or, .lexeme = "or", .line = 1, .col = 42 },
                .{ .kind = .Literal, .lexeme = "key", .line = 1, .col = 45 },
                .{ .kind = .Equal, .lexeme = "=", .line = 1, .col = 49 },
                .{ .kind = .Literal, .lexeme = "first", .line = 1, .col = 50 },
                .{ .kind = .RightParenthesis, .lexeme = ")", .line = 1, .col = 53 },
            },
            .expectedQuerySet = .{
                .timeRange = .{ .{ .duration = "-5m" }, .{ .now = {} } },
                .tags = .{ .andOp = .{
                    &.{ .grouping = &.{ .orOp = .{
                        &.{ .equalOp = .{
                            &.{ .literal = "env" },
                            &.{ .literal = "prod" },
                        } },
                        &.{ .equalOp = .{
                            &.{ .literal = "service" },
                            &.{ .literal = "api" },
                        } },
                    } } },
                    &.{ .grouping = &.{ .orOp = .{
                        &.{ .equalOp = .{
                            &.{ .literal = "host" },
                            &.{ .literal = "web" },
                        } },
                        &.{ .equalOp = .{
                            &.{ .literal = "host" },
                            &.{ .literal = "api" },
                        } },
                    } } },
                } },
                .query = .{ .andOp = .{
                    &.{ .grouping = &.{ .orOp = .{
                        &.{ .equalOp = .{
                            &.{ .literal = "call" },
                            &.{ .literal = "get" },
                        } },
                        &.{ .equalOp = .{
                            &.{ .literal = "boost" },
                            &.{ .literal = "yes" },
                        } },
                    } } },
                    &.{ .grouping = &.{ .orOp = .{
                        &.{ .equalOp = .{
                            &.{ .literal = "url" },
                            &.{ .literal = "one" },
                        } },
                        &.{ .equalOp = .{
                            &.{ .literal = "key" },
                            &.{ .literal = "first" },
                        } },
                    } } },
                } },
                .pipes = .empty,
            },
            .expectedSyntaxErrors = &.{},
        },
        // test bunch of nested groups
        .{
            .query = &.{
                .{ .kind = .LeftSquareBracket, .lexeme = "[", .line = 1, .col = 1 },
                .{ .kind = .Literal, .lexeme = "-5m", .line = 1, .col = 2 },
                .{ .kind = .Comma, .lexeme = ",", .line = 1, .col = 5 },
                .{ .kind = .RightSquareBracket, .lexeme = "]", .line = 1, .col = 6 },
                .{ .kind = .LeftCurlyBracket, .lexeme = "{", .line = 1, .col = 1 },
                .{ .kind = .LeftParenthesis, .lexeme = "(", .line = 1, .col = 2 },
                .{ .kind = .LeftParenthesis, .lexeme = "(", .line = 1, .col = 3 },
                .{ .kind = .Literal, .lexeme = "env", .line = 1, .col = 4 },
                .{ .kind = .Equal, .lexeme = "=", .line = 1, .col = 7 },
                .{ .kind = .Literal, .lexeme = "prod", .line = 1, .col = 8 },
                .{ .kind = .RightParenthesis, .lexeme = ")", .line = 1, .col = 12 },
                .{ .kind = .RightParenthesis, .lexeme = ")", .line = 1, .col = 13 },
                .{ .kind = .RightCurlyBracket, .lexeme = "}", .line = 1, .col = 14 },

                .{ .kind = .LeftParenthesis, .lexeme = "(", .line = 1, .col = 2 },
                .{ .kind = .LeftParenthesis, .lexeme = "(", .line = 1, .col = 3 },
                .{ .kind = .Literal, .lexeme = "field", .line = 1, .col = 4 },
                .{ .kind = .Equal, .lexeme = "=", .line = 1, .col = 7 },
                .{ .kind = .Literal, .lexeme = "value", .line = 1, .col = 8 },
                .{ .kind = .And, .lexeme = "and", .line = 1, .col = 3 },
                .{ .kind = .Literal, .lexeme = "start", .line = 1, .col = 4 },
                .{ .kind = .Equal, .lexeme = "=", .line = 1, .col = 7 },
                .{ .kind = .Literal, .lexeme = "alpha", .line = 1, .col = 8 },
                .{ .kind = .RightParenthesis, .lexeme = ")", .line = 1, .col = 12 },
                .{ .kind = .Or, .lexeme = "or", .line = 1, .col = 3 },
                .{ .kind = .LeftParenthesis, .lexeme = "(", .line = 1, .col = 12 },
                .{ .kind = .Literal, .lexeme = "another", .line = 1, .col = 4 },
                .{ .kind = .Equal, .lexeme = "=", .line = 1, .col = 7 },
                .{ .kind = .Literal, .lexeme = "some", .line = 1, .col = 8 },
                .{ .kind = .RightParenthesis, .lexeme = ")", .line = 1, .col = 12 },
                .{ .kind = .RightParenthesis, .lexeme = ")", .line = 1, .col = 12 },
            },
            .expectedQuerySet = .{
                .timeRange = .{ .{ .duration = "-5m" }, .{ .now = {} } },
                .tags = .{ .grouping = &.{ .grouping = &.{ .equalOp = .{
                    &.{ .literal = "env" },
                    &.{ .literal = "prod" },
                } } } },
                .query = .{ .grouping = &.{ .orOp = .{
                    &.{ .grouping = &.{ .andOp = .{
                        &.{ .equalOp = .{
                            &.{ .literal = "field" },
                            &.{ .literal = "value" },
                        } },
                        &.{ .equalOp = .{
                            &.{ .literal = "start" },
                            &.{ .literal = "alpha" },
                        } },
                    } } },
                    &.{ .grouping = &.{ .equalOp = .{
                        &.{ .literal = "another" },
                        &.{ .literal = "some" },
                    } } },
                } } },
                .pipes = .empty,
            },
            .expectedSyntaxErrors = &.{},
        },
        // {} must contain at least one expression
        .{
            .query = &.{
                .{ .kind = .LeftSquareBracket, .lexeme = "[", .line = 1, .col = 1 },
                .{ .kind = .Literal, .lexeme = "-5m", .line = 1, .col = 2 },
                .{ .kind = .Comma, .lexeme = ",", .line = 1, .col = 5 },
                .{ .kind = .RightSquareBracket, .lexeme = "]", .line = 1, .col = 6 },
                .{ .kind = .LeftCurlyBracket, .lexeme = "{", .line = 1, .col = 1 },
                .{ .kind = .RightCurlyBracket, .lexeme = "}", .line = 1, .col = 2 },
            },
            .expectedErr = Error.SyntaxError,
            .expectedSyntaxErrors = &[_]ErrorReporter.SyntaxError{
                .{ .line = 1, .col = 2, .message = expectExpression },
            },
        },
        // = predicate must have a key (primary token)
        .{
            .query = &.{
                .{ .kind = .LeftSquareBracket, .lexeme = "[", .line = 1, .col = 1 },
                .{ .kind = .Literal, .lexeme = "-5m", .line = 1, .col = 2 },
                .{ .kind = .Comma, .lexeme = ",", .line = 1, .col = 5 },
                .{ .kind = .RightSquareBracket, .lexeme = "]", .line = 1, .col = 6 },
                .{ .kind = .LeftCurlyBracket, .lexeme = "{", .line = 1, .col = 1 },
                .{ .kind = .Equal, .lexeme = "=", .line = 1, .col = 2 },
                .{ .kind = .Literal, .lexeme = "prod", .line = 1, .col = 3 },
                .{ .kind = .RightCurlyBracket, .lexeme = "}", .line = 1, .col = 7 },
            },
            .expectedErr = Error.SyntaxError,
            .expectedSyntaxErrors = &[_]ErrorReporter.SyntaxError{
                .{ .line = 1, .col = 2, .message = expectExpression },
            },
        },
        // = predicate msut have a value (primary token)
        .{
            .query = &.{
                .{ .kind = .LeftSquareBracket, .lexeme = "[", .line = 1, .col = 1 },
                .{ .kind = .Literal, .lexeme = "-5m", .line = 1, .col = 2 },
                .{ .kind = .Comma, .lexeme = ",", .line = 1, .col = 5 },
                .{ .kind = .RightSquareBracket, .lexeme = "]", .line = 1, .col = 6 },
                .{ .kind = .LeftCurlyBracket, .lexeme = "{", .line = 1, .col = 1 },
                .{ .kind = .Literal, .lexeme = "env", .line = 1, .col = 2 },
                .{ .kind = .Equal, .lexeme = "=", .line = 1, .col = 5 },
                .{ .kind = .RightCurlyBracket, .lexeme = "}", .line = 1, .col = 6 },
            },
            .expectedErr = Error.SyntaxError,
            .expectedSyntaxErrors = &[_]ErrorReporter.SyntaxError{
                .{ .line = 1, .col = 6, .message = expectExpression },
            },
        },
        // tags must have a closing curly bracket
        .{
            .query = &.{
                .{ .kind = .LeftSquareBracket, .lexeme = "[", .line = 1, .col = 1 },
                .{ .kind = .Literal, .lexeme = "-5m", .line = 1, .col = 2 },
                .{ .kind = .Comma, .lexeme = ",", .line = 1, .col = 5 },
                .{ .kind = .RightSquareBracket, .lexeme = "]", .line = 1, .col = 6 },
                .{ .kind = .LeftCurlyBracket, .lexeme = "{", .line = 1, .col = 1 },
                .{ .kind = .Literal, .lexeme = "env", .line = 1, .col = 2 },
                .{ .kind = .Equal, .lexeme = "=", .line = 1, .col = 5 },
                .{ .kind = .Literal, .lexeme = "prod", .line = 1, .col = 6 },
            },
            .expectedErr = Error.SyntaxError,
            .expectedSyntaxErrors = &[_]ErrorReporter.SyntaxError{
                .{ .line = 1, .col = 6, .message = expectCloseCurlyBracket },
            },
        },
        // missing closing parentheses
        .{
            .query = &.{
                .{ .kind = .LeftSquareBracket, .lexeme = "[", .line = 1, .col = 1 },
                .{ .kind = .Literal, .lexeme = "-5m", .line = 1, .col = 2 },
                .{ .kind = .Comma, .lexeme = ",", .line = 1, .col = 5 },
                .{ .kind = .RightSquareBracket, .lexeme = "]", .line = 1, .col = 6 },
                .{ .kind = .LeftCurlyBracket, .lexeme = "{", .line = 1, .col = 1 },
                .{ .kind = .LeftParenthesis, .lexeme = "(", .line = 1, .col = 2 },
                .{ .kind = .Literal, .lexeme = "env", .line = 1, .col = 3 },
                .{ .kind = .Equal, .lexeme = "=", .line = 1, .col = 6 },
                .{ .kind = .Literal, .lexeme = "prod", .line = 1, .col = 7 },
                .{ .kind = .RightCurlyBracket, .lexeme = "}", .line = 1, .col = 11 },
            },
            .expectedErr = Error.SyntaxError,
            .expectedSyntaxErrors = &[_]ErrorReporter.SyntaxError{
                .{ .line = 1, .col = 11, .message = expectCloseParenthesis },
            },
        },
        // and has no right hand
        .{
            .query = &.{
                .{ .kind = .LeftSquareBracket, .lexeme = "[", .line = 1, .col = 1 },
                .{ .kind = .Literal, .lexeme = "-5m", .line = 1, .col = 2 },
                .{ .kind = .Comma, .lexeme = ",", .line = 1, .col = 5 },
                .{ .kind = .RightSquareBracket, .lexeme = "]", .line = 1, .col = 6 },
                .{ .kind = .LeftCurlyBracket, .lexeme = "{", .line = 1, .col = 1 },
                .{ .kind = .Literal, .lexeme = "env", .line = 1, .col = 2 },
                .{ .kind = .Equal, .lexeme = "=", .line = 1, .col = 5 },
                .{ .kind = .Literal, .lexeme = "prod", .line = 1, .col = 6 },
                .{ .kind = .And, .lexeme = "and", .line = 1, .col = 11 },
                .{ .kind = .RightCurlyBracket, .lexeme = "}", .line = 1, .col = 14 },
            },
            .expectedErr = Error.SyntaxError,
            .expectedSyntaxErrors = &[_]ErrorReporter.SyntaxError{
                .{ .line = 1, .col = 14, .message = expectExpression },
            },
        },
        // , is an invalid token
        .{
            .query = &.{
                .{ .kind = .LeftSquareBracket, .lexeme = "[", .line = 1, .col = 1 },
                .{ .kind = .Literal, .lexeme = "-5m", .line = 1, .col = 2 },
                .{ .kind = .Comma, .lexeme = ",", .line = 1, .col = 5 },
                .{ .kind = .RightSquareBracket, .lexeme = "]", .line = 1, .col = 6 },
                .{ .kind = .LeftCurlyBracket, .lexeme = "{", .line = 1, .col = 1 },
                .{ .kind = .Literal, .lexeme = "env", .line = 1, .col = 2 },
                .{ .kind = .Equal, .lexeme = "=", .line = 1, .col = 5 },
                .{ .kind = .Literal, .lexeme = "prod", .line = 1, .col = 6 },
                .{ .kind = .Comma, .lexeme = ",", .line = 1, .col = 10 },
                .{ .kind = .Literal, .lexeme = "service", .line = 1, .col = 11 },
                .{ .kind = .Equal, .lexeme = "=", .line = 1, .col = 18 },
                .{ .kind = .Literal, .lexeme = "api", .line = 1, .col = 19 },
                .{ .kind = .RightCurlyBracket, .lexeme = "}", .line = 1, .col = 22 },
            },
            .expectedErr = Error.SyntaxError,
            .expectedSyntaxErrors = &[_]ErrorReporter.SyntaxError{
                .{ .line = 1, .col = 10, .message = expectCloseCurlyBracket },
            },
        },
    };

    for (cases) |case| {
        var reporter: ErrorReporter = .{};

        var parser: Parser = .{};
        defer parser.deinit(allocator);

        const result = parser.querySet(allocator, case.query, &reporter);
        if (case.expectedErr) |expectedErr| {
            try testing.expectError(expectedErr, result);
        } else {
            const parsed = result catch |err| {
                for (reporter.syntaxErrors()) |e| ErrorReporter.log(e);
                return err;
            };

            try testing.expectEqualDeep(case.expectedQuerySet, parsed);
        }

        try testing.expectEqualDeep(case.expectedSyntaxErrors, reporter.syntaxErrors());
    }
}
