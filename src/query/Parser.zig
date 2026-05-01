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
const expectExpression = "Expect expression.";

// TODO: benchmark if *[2]Expression gives better locality
pub const Expression = union(enum) {
    range: [2][]const u8,
    equalOp: [2]*const Expression,
    notEqualOp: [2]*const Expression,
    andOp: [2]*const Expression,
    orOp: [2]*const Expression,
    grouping: *const Expression,
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
        .range = .{ "-5m", "now" },
    };
}

fn tags(self: *Parser, tokens: []const Token) ParseError!?Expression {
    try self.consume(tokens, .LeftCurlyBracket, expectOpenCurlyBracket);
    const expr = try self.boolean(tokens, &.{ .Equal, .NotEqual });
    try self.consume(tokens, .RightCurlyBracket, expectCloseCurlyBracket);

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
    var expr = try self.primary(tokens, allowsOps);

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
        return .{ .grouping = node };
    }

    const line, const col = self.currentPosition(tokens);
    _ = self.reporter.reportSyntaxError(.{
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

    const Case = struct {
        query: []const Token,
        expectedTags: ?Expression = null,
        expectedErr: ?anyerror = null,
        expectedSyntaxErrors: []const ErrorReporter.SyntaxError,
    };

    const cases = [_]Case{
        .{
            .query = &.{
                .{ .kind = .LeftCurlyBracket, .lexeme = "{", .line = 1, .col = 1 },
                .{ .kind = .Literal, .lexeme = "env", .line = 1, .col = 2 },
                .{ .kind = .Equal, .lexeme = "=", .line = 1, .col = 5 },
                .{ .kind = .Literal, .lexeme = "prod", .line = 1, .col = 6 },
                .{ .kind = .RightCurlyBracket, .lexeme = "}", .line = 1, .col = 10 },
            },
            .expectedTags = .{ .equalOp = .{
                &.{ .literal = "env" },
                &.{ .literal = "prod" },
            } },
            .expectedSyntaxErrors = &.{},
        },
        .{
            .query = &.{
                .{ .kind = .LeftCurlyBracket, .lexeme = "{", .line = 1, .col = 1 },
                .{ .kind = .Literal, .lexeme = "env", .line = 1, .col = 2 },
                .{ .kind = .Equal, .lexeme = "=", .line = 1, .col = 5 },
                .{ .kind = .Literal, .lexeme = "prod", .line = 1, .col = 6 },
                .{ .kind = .And, .lexeme = "and", .line = 1, .col = 11 },
                .{ .kind = .Literal, .lexeme = "service", .line = 1, .col = 15 },
                .{ .kind = .Equal, .lexeme = "=", .line = 1, .col = 22 },
                .{ .kind = .Literal, .lexeme = "api", .line = 1, .col = 23 },
                .{ .kind = .RightCurlyBracket, .lexeme = "}", .line = 1, .col = 26 },
            },
            .expectedTags = .{ .andOp = .{
                &.{ .equalOp = .{
                    &.{ .literal = "env" },
                    &.{ .literal = "prod" },
                } },
                &.{ .equalOp = .{
                    &.{ .literal = "service" },
                    &.{ .literal = "api" },
                } },
            } },
            .expectedSyntaxErrors = &.{},
        },
        .{
            .query = &.{
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
            .expectedTags = .{ .andOp = .{
                &.{ .orOp = .{
                    &.{ .equalOp = .{
                        &.{ .literal = "env" },
                        &.{ .literal = "prod" },
                    } },
                    &.{ .equalOp = .{
                        &.{ .literal = "service" },
                        &.{ .literal = "api" },
                    } },
                } },
                &.{ .equalOp = .{
                    &.{ .literal = "host" },
                    &.{ .literal = "web" },
                } },
            } },
            .expectedSyntaxErrors = &.{},
        },
        .{
            .query = &.{
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
            },
            .expectedTags = .{
                .andOp = .{
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
                },
            },
            .expectedSyntaxErrors = &.{},
        },
        .{
            .query = &.{
                .{ .kind = .LeftCurlyBracket, .lexeme = "{", .line = 1, .col = 1 },
                .{ .kind = .LeftParenthesis, .lexeme = "(", .line = 1, .col = 2 },
                .{ .kind = .LeftParenthesis, .lexeme = "(", .line = 1, .col = 3 },
                .{ .kind = .Literal, .lexeme = "env", .line = 1, .col = 4 },
                .{ .kind = .Equal, .lexeme = "=", .line = 1, .col = 7 },
                .{ .kind = .Literal, .lexeme = "prod", .line = 1, .col = 8 },
                .{ .kind = .RightParenthesis, .lexeme = ")", .line = 1, .col = 12 },
                .{ .kind = .RightParenthesis, .lexeme = ")", .line = 1, .col = 13 },
                .{ .kind = .RightCurlyBracket, .lexeme = "}", .line = 1, .col = 14 },
            },
            .expectedTags = .{
                .grouping = &.{ .grouping = &.{ .equalOp = .{
                    &.{ .literal = "env" },
                    &.{ .literal = "prod" },
                } } },
            },
            .expectedSyntaxErrors = &.{},
        },
        .{
            .query = &.{
                .{ .kind = .LeftCurlyBracket, .lexeme = "{", .line = 1, .col = 1 },
                .{ .kind = .RightCurlyBracket, .lexeme = "}", .line = 1, .col = 2 },
            },
            .expectedErr = Error.SyntaxError,
            .expectedSyntaxErrors = &[_]ErrorReporter.SyntaxError{
                .{ .line = 1, .col = 2, .message = expectExpression },
            },
        },
        .{
            .query = &.{
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
        .{
            .query = &.{
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
        .{
            .query = &.{
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
        .{
            .query = &.{
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
        .{
            .query = &.{
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
        .{
            .query = &.{
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
        var errsBuf: [8]ErrorReporter.SyntaxError = undefined;
        var reporter = ErrorReporter.init(&errsBuf);

        var parser: Parser = .init(allocator, &reporter);
        defer parser.deinit();

        const result = parser.querySet(case.query);
        if (case.expectedErr) |expectedErr| {
            try testing.expectError(expectedErr, result);
        } else {
            const parsed = try result;
            const timeRangeExpr: Expression = .{ .range = .{ "-5m", "now" } };
            try testing.expectEqualDeep(timeRangeExpr, parsed.timeRange);

            try testing.expectEqualDeep(case.expectedTags, parsed.tags);

            try testing.expect(parsed.query == null);
            try testing.expectEqual(@as(usize, 0), parsed.pipes.items.len);
        }

        try testing.expectEqualDeep(case.expectedSyntaxErrors, reporter.syntaxErrors());
    }
}
