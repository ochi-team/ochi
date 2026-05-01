const std = @import("std");
const Allocator = std.mem.Allocator;
const testing = std.testing;

const Token = @import("Scanner.zig").Token;
const TokenKind = @import("Scanner.zig").TokenKind;
const Error = @import("Scanner.zig").Error;
const ErrorReporter = @import("ErrorReporter.zig");

const expectCloseParenthesis = "Expect ')' after expression.";
const expectOpenCurlyBracket = "Expect '{' before tags.";
const expectCloseCurlyBracket = "Expect '}' after tags.";

// TODO: benchmark if *[2]Expression gives better locality
pub const Expression = union(enum) {
    range: [2][]const u8,
    equalOp: [2]*Expression,
    notEqualOp: [2]*Expression,
    andOp: [2]*Expression,
    orOp: [2]*Expression,
    grouping: [1]*Expression,
    literal: []const u8,
    timestamp: []const u8,
    duration: []const u8,
    now: void,
};

pub const QuerySet = struct {
    timeRange: ?Expression,
    tags: ?Expression,
    query: ?Expression,
    pipes: std.ArrayList(Expression),
};

pub const Parser = @This();

pub const ParseError = Error || error{OutOfMemory};

// state
current: usize = 0,
reporter: *ErrorReporter,

// TODO: we might want to use a pool here to create a bunch of expressions,
// better to emter how many we create them per query
allocator: Allocator,
// we expect to call query only in read API using an arena allocator, perhaps we get can rid of it
garbage: std.ArrayList(*Expression) = .empty,

pub fn init(allocator: Allocator, reporter: *ErrorReporter) Parser {
    return Parser{
        .allocator = allocator,
        .reporter = reporter,
    };
}

pub fn deinit(self: *Parser) void {
    for (self.garbage.items) |node| {
        self.allocator.destroy(node);
    }
    self.garbage.deinit(self.allocator);
}

pub fn querySet(self: *Parser, tokens: []const Token) ParseError!QuerySet {
    const range = try self.timeRange(tokens);
    const ts = try self.tags(tokens);
    const q = try self.query(tokens);
    const ps = try self.pipes(tokens);

    return .{
        .timeRange = range,
        .tags = ts,
        .query = q,
        .pipes = ps,
    };
}

fn timeRange(self: *Parser, tokens: []const Token) ParseError!?Expression {
    _ = self;
    _ = tokens;
    return .{
        .range = &[_][]const u8{ "-5m", "now" },
    };
}

fn tags(self: *Parser, tokens: []const Token) ParseError!?Expression {
    self.consume(tokens, .LeftCurlyBracket, expectOpenCurlyBracket);
    const expr = try self.boolean(tokens, &.{ .Equal, .NotEqual }, .RightCurlyBracket);
    self.consume(tokens, .RightCurlyBracket, expectCloseCurlyBracket);

    return expr;
}

fn query(self: *Parser, tokens: []const Token) ParseError!?Expression {
    _ = self;
    _ = tokens;
    return null;
}

fn pipes(self: *Parser, tokens: []const Token) ParseError!std.ArrayList(Expression) {
    _ = self;
    _ = tokens;
    return .empty;
}

fn boolean(self: *Parser, tokens: []const Token, allowsOps: []const TokenKind) ParseError!Expression {
    var expr = try self.equality(tokens, allowsOps);

    while (self.match(tokens, &.{ .And, .Or })) {
        const op = tokens[self.current - 1].kind;
        const right = try self.equality(tokens, allowsOps);

        const leftNode = try self.allocExpression(expr);
        const rightNode = try self.allocExpression(right);

        expr = switch (op) {
            .And => .{ .andOp = .{ leftNode, rightNode } },
            .Or => .{ .orOp = .{ leftNode, rightNode } },
            else => {
                const line, const col = self.currentPosition(tokens);
                _ = self.reporter.reportSyntaxError(.{
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

fn equality(self: *Parser, tokens: []const Token, allowsOps: []const TokenKind) ParseError!Expression {
    var expr = try self.primary(tokens);

    while (self.match(tokens, allowsOps)) {
        const op = tokens[self.current - 1].kind;
        const right = try self.primary(tokens, allowsOps);

        const leftNode = try self.allocExpression(expr);
        const rightNode = try self.allocExpression(right);

        expr = switch (op) {
            .Equal => .{ .equalOp = .{ leftNode, rightNode } },
            .NotEqual => .{ .notEqualOp = .{ leftNode, rightNode } },
            else => {
                const line, const col = self.currentPosition(tokens);
                _ = self.reporter.reportSyntaxError(.{
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

fn primary(self: *Parser, tokens: []const Token, allowsOps: []const TokenKind) ParseError!Expression {
    if (self.match(tokens, &.{.Literal})) {
        return .{ .literal = tokens[self.current - 1].lexeme };
    }

    if (self.match(tokens, &.{.LeftParenthesis})) {
        const expr = try self.boolean(tokens, allowsOps);
        try self.consume(tokens, .RightParenthesis, expectCloseParenthesis);

        const node = try self.allocExpression(expr);
        return .{ .grouping = .{node} };
    }

    const line, const col = self.currentPosition(tokens);
    _ = self.reporter.reportSyntaxError(.{
        .line = line,
        .col = col,
        .message = "Expect expression.",
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

fn consume(self: *Parser, tokens: []const Token, kind: TokenKind, message: []const u8) Error!void {
    if (self.current < tokens.len and tokens[self.current].kind == kind) {
        self.current += 1;
        return;
    }

    const line, const col = self.currentPosition(tokens);

    _ = self.reporter.reportSyntaxError(.{
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

fn allocExpression(self: *Parser, expr: Expression) !*Expression {
    try self.garbage.ensureUnusedCapacity(self.allocator, 1);

    const node = try self.allocator.create(Expression);
    errdefer self.allocator.destroy(node);

    self.garbage.appendAssumeCapacity(node);
    node.* = expr;
    return node;
}

test "Parser.expression" {
    const allocator = std.testing.allocator;

    const ExpectedType = enum {
        literal,
        grouping_literal,
        equal_op,
    };

    const Case = struct {
        tokens: []const Token,
        expectedType: ?ExpectedType = null,
        expectedLeftLiteral: ?[]const u8 = null,
        expectedRightLiteral: ?[]const u8 = null,
        expectedErr: ?anyerror = null,
        expectedSyntaxErrors: []const ErrorReporter.SyntaxError,
    };

    const cases = [_]Case{
        .{
            .tokens = &[_]Token{
                .{ .kind = .Literal, .lexeme = "foo", .line = 1, .col = 1 },
            },
            .expectedType = .literal,
            .expectedLeftLiteral = "foo",
            .expectedSyntaxErrors = &[_]ErrorReporter.SyntaxError{},
        },
        .{
            .tokens = &[_]Token{
                .{ .kind = .LeftParenthesis, .lexeme = "(", .line = 1, .col = 1 },
                .{ .kind = .Literal, .lexeme = "foo", .line = 1, .col = 2 },
                .{ .kind = .RightParenthesis, .lexeme = ")", .line = 1, .col = 5 },
            },
            .expectedType = .grouping_literal,
            .expectedLeftLiteral = "foo",
            .expectedSyntaxErrors = &[_]ErrorReporter.SyntaxError{},
        },
        .{
            .tokens = &[_]Token{
                .{ .kind = .Literal, .lexeme = "foo", .line = 1, .col = 1 },
                .{ .kind = .Equal, .lexeme = "=", .line = 1, .col = 5 },
                .{ .kind = .Literal, .lexeme = "bar", .line = 1, .col = 7 },
            },
            .expectedType = .equal_op,
            .expectedLeftLiteral = "foo",
            .expectedRightLiteral = "bar",
            .expectedSyntaxErrors = &[_]ErrorReporter.SyntaxError{},
        },
        .{
            .tokens = &[_]Token{
                .{ .kind = .LeftParenthesis, .lexeme = "(", .line = 1, .col = 1 },
                .{ .kind = .Literal, .lexeme = "foo", .line = 1, .col = 2 },
            },
            .expectedErr = Error.SyntaxError,
            .expectedSyntaxErrors = &[_]ErrorReporter.SyntaxError{
                .{ .line = 1, .col = 2, .message = expectCloseParenthesis },
            },
        },
    };

    for (cases) |case| {
        var errsBuf: [8]ErrorReporter.SyntaxError = undefined;
        var reporter = ErrorReporter.init(&errsBuf);

        var parser: Parser = .init(allocator, &reporter);
        defer parser.deinit();

        const result = parser.querySet(case.tokens);
        if (case.expectedErr) |expectedErr| {
            try testing.expectError(expectedErr, result);
        } else {
            const expr = try result;
            switch (case.expectedType.?) {
                .literal => switch (expr) {
                    .literal => |value| try testing.expectEqualStrings(case.expectedLeftLiteral.?, value),
                    else => return error.TestUnexpectedResult,
                },
                .grouping_literal => switch (expr) {
                    .grouping => |inner| switch (inner[0].*) {
                        .literal => |value| try testing.expectEqualStrings(case.expectedLeftLiteral.?, value),
                        else => return error.TestUnexpectedResult,
                    },
                    else => return error.TestUnexpectedResult,
                },
                .equal_op => switch (expr) {
                    .equalOp => |nodes| {
                        switch (nodes[0].*) {
                            .literal => |value| try testing.expectEqualStrings(case.expectedLeftLiteral.?, value),
                            else => return error.TestUnexpectedResult,
                        }
                        switch (nodes[1].*) {
                            .literal => |value| try testing.expectEqualStrings(case.expectedRightLiteral.?, value),
                            else => return error.TestUnexpectedResult,
                        }
                    },
                    else => return error.TestUnexpectedResult,
                },
            }
        }

        try testing.expectEqualDeep(case.expectedSyntaxErrors, reporter.syntaxErrors());
    }
}
