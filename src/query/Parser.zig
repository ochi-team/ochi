const Token = @import("Scanner.zig").Token;
const TokenKind = @import("Scanner.zig").TokenKind;
const Error = @import("Scanner.zig").Error;
const ErrorReporter = @import("ErrorReporter.zig");

pub const Op = enum {
    Equal,
    NotEqual,
    And,
    Or,
};

pub const Expression = union(enum) {
    range: *Range,
    binary: *Binary,
    grouping: *Grouping,
    literal: *Literal,
};

pub const Range = struct {
    start: Expression,
    end: Expression,
};

pub const Binary = struct {
    op: Op,
    left: Expression,
    right: Expression,
};

pub const Grouping = struct {
    inner: Expression,
};

pub const Tags = struct {
    tags: Expression,
};

pub const Literal = union(enum) {
    string: []const u8,
    time: Time,
};

pub const Time = union(enum) {
    timestamp: []const u8,
    duration: []const u8,
    now: void,
};

pub const Parser = @This();

current: usize = 0,
reporter: *ErrorReporter,

pub fn expression(self: *Parser, tokens: []const Token) Error!Expression {
    return self.equality(tokens);
}

fn equality(self: *Parser, tokens: []const Token) Expression {
    var expr = self.primary(tokens);

    if (match(tokens, .{ .Equal, .NotEqual })) {
        const op = tokens[self.current - 1];
        const right = self.primary(tokens);
        expr = Binary{
            .op = op,
            .left = expr,
            .right = right,
        };
    }

    return expr;
}

fn primary(self: *Parser, tokens: []const Token) Expression {
    if (match(tokens, .{.Literal})) {
        return Literal{
            .value = tokens[self.current].lexeme,
        };
    }

    if (match(tokens, .{.LeftParenthesis})) {
        const expr = self.expression(tokens);
        self.consume(tokens, .RightParen, "Expect ')' after expression.");
        return Grouping{
            .inner = expr,
        };
    }
}

fn match(self: *Parser, tokens: []const Token, types: []const TokenKind) bool {
    for (types) |t| {
        if (tokens[self.current].kind == t) {
            self.current += 1;
            return true;
        }
    }

    return false;
}

fn consume(self: *Parser, tokens: []const Token, kind: TokenKind, message: []const u8) Error!void {
    if (tokens[self.current].kind == kind) {
        self.current += 1;
        return;
    }

    self.reporter.reportSyntaxError(.{
        .line = tokens[self.current].line,
        .col = tokens[self.current].col,
        .message = message,
    });
    return Error.SyntaxError;
}
