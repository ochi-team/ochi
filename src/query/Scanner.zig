const std = @import("std");
const Allocator = std.mem.Allocator;
const testing = std.testing;

const ErrorReporter = @import("ErrorReporter.zig");

pub const Error = error{
    SyntaxError,
} || Allocator.Error;

const ScannedToken = struct {
    token: ?Token,
    tail: []const u8,
};

fn isKeyword(word: []const u8) ?TokenKind {
    // we don't expect supporting larger keywords
    if (word.len > 8) {
        return null;
    }

    var lowerCaseBuf: [8]u8 = undefined;
    for (0..word.len) |i| lowerCaseBuf[i] = std.ascii.toLower(word[i]);
    const lowerCaseWord = lowerCaseBuf[0..word.len];

    if (std.mem.eql(u8, lowerCaseWord, "or")) return .Or;
    if (std.mem.eql(u8, lowerCaseWord, "and")) return .And;

    return null;
}

pub const TokenKind = enum {
    LeftSquareBracket,
    RightSquareBracket,
    LeftCurlyBracket,
    RightCurlyBracket,
    LeftParenthesis,
    RightParenthesis,

    Comma,

    Equal,
    NotEqual,

    MatchRegex,
    NotMatchRegex,

    And,
    Or,

    Literal,

    Pipe,
};

pub const Token = struct {
    kind: TokenKind,
    lexeme: []const u8,
    line: u16,
    col: u16,
};

pub const Scanner = @This();
/// tokens holds the current parsed tokens state,
tokens: std.ArrayList(Token) = .empty,
/// quotedLiterals are a special chunk, it's required to build new slices for such tokens
/// in order to unescape quotes
quotedLiterals: std.ArrayList([]const u8) = .empty,

// state
line: u16 = 1,
col: u16 = 1,

pub fn deinit(self: *Scanner, allocator: Allocator) void {
    for (self.quotedLiterals.items) |literal| allocator.free(literal);
    self.quotedLiterals.deinit(allocator);
    self.tokens.deinit(allocator);
}

/// scan is responsible for scanning the query and returning a list of tokens,
/// it also reports any syntax errors to the ErrorReporter.
pub fn scan(
    self: *Scanner,
    allocator: Allocator,
    query: []const u8,
    reporter: *ErrorReporter,
) !void {
    var tail = query[0..];

    while (tail.len > 0) {
        const next = try self.scanToken(allocator, tail, reporter);
        tail = next.tail;
        if (next.token) |token| {
            try self.tokens.append(allocator, token);
        }
    }
}

fn scanToken(
    self: *Scanner,
    allocator: Allocator,
    query: []const u8,
    reporter: *ErrorReporter,
) Error!ScannedToken {
    const token: ScannedToken = switch (query[0]) {
        // time range
        '[' => .{
            .token = .{
                .kind = .LeftSquareBracket,
                .lexeme = query[0..1],
                .line = self.line,
                .col = self.col,
            },
            .tail = query[1..],
        },
        ']' => .{
            .token = .{
                .kind = .RightSquareBracket,
                .lexeme = query[0..1],
                .line = self.line,
                .col = self.col,
            },
            .tail = query[1..],
        },
        ',' => .{
            .token = .{
                .kind = .Comma,
                .lexeme = query[0..1],
                .line = self.line,
                .col = self.col,
            },
            .tail = query[1..],
        },
        // tags
        '{' => .{
            .token = .{ .kind = .LeftCurlyBracket, .lexeme = query[0..1], .line = self.line, .col = self.col },
            .tail = query[1..],
        },
        '}' => .{
            .token = .{ .kind = .RightCurlyBracket, .lexeme = query[0..1], .line = self.line, .col = self.col },
            .tail = query[1..],
        },
        '(' => .{
            .token = .{ .kind = .LeftParenthesis, .lexeme = query[0..1], .line = self.line, .col = self.col },
            .tail = query[1..],
        },
        ')' => .{
            .token = .{ .kind = .RightParenthesis, .lexeme = query[0..1], .line = self.line, .col = self.col },
            .tail = query[1..],
        },
        // equal
        '=' => .{
            .token = .{ .kind = .Equal, .lexeme = query[0..1], .line = self.line, .col = self.col },
            .tail = query[1..],
        },
        '~' => .{
            .token = .{ .kind = .MatchRegex, .lexeme = query[0..1], .line = self.line, .col = self.col },
            .tail = query[1..],
        },
        // not equal
        '!' => blk: {
            if (query.len > 1 and query[1] == '=') {
                break :blk .{
                    .token = .{
                        .kind = .NotEqual,
                        .lexeme = query[0..2],
                        .line = self.line,
                        .col = self.col,
                    },
                    .tail = query[2..],
                };
            } else if (query.len > 1 and query[1] == '~') {
                break :blk .{
                    .token = .{
                        .kind = .NotMatchRegex,
                        .lexeme = query[0..2],
                        .line = self.line,
                        .col = self.col,
                    },
                    .tail = query[2..],
                };
            } else {
                _ = reporter.reportSyntaxError(
                    .{
                        .line = self.line,
                        .col = self.col,
                        .message = "unexpected token: !",
                    },
                );
                return Error.SyntaxError;
            }
        },
        // TODO: implement lessThan and others
        // pipe
        '|' => .{
            .token = .{
                .kind = .Pipe,
                .lexeme = query[0..1],
                .line = self.line,
                .col = self.col,
            },
            .tail = query[1..],
        },
        '\'', '"' => try self.scanQuotedLiteral(allocator, query, reporter),
        // comment OR alphanumeric or literal value starting with '_'
        'a'...'z', 'A'...'Z', '0'...'9', '_', '-', ':', '@', '+', '/', '.' => blk: {
            // / could be not only a comment, but a url path e.g. /health
            if (query.len > 1 and query[1] == '/') {
                const nextLineIdx = std.mem.indexOfScalar(u8, query, '\n') orelse query.len;
                const consumed = if (nextLineIdx < query.len) nextLineIdx + 1 else nextLineIdx;
                break :blk .{
                    .token = null,
                    .tail = query[consumed..],
                };
            }

            var idx: usize = 0;
            while (idx < query.len and
                (std.ascii.isAlphanumeric(query[idx]) or
                    query[idx] == '_' or
                    query[idx] == '-' or
                    query[idx] == ':' or
                    query[idx] == '/' or
                    query[idx] == '@' or
                    query[idx] == '.' or
                    query[idx] == '+')) : (idx += 1)
            {}

            const word = query[0..idx];
            const kind = if (isKeyword(word)) |keyword| keyword else TokenKind.Literal;

            break :blk .{
                .token = .{
                    .kind = kind,
                    .lexeme = word,
                    .line = self.line,
                    .col = self.col,
                },
                .tail = query[idx..],
            };
        },
        // whitespace
        '\n' => .{ .token = null, .tail = query[1..] },
        ' ', '\t' => blk: {
            break :blk .{
                .token = null,
                .tail = query[1..],
            };
        },
        else => {
            _ = reporter.reportSyntaxError(.{ .line = self.line, .col = self.col, .message = "unexpected token" });
            return Error.SyntaxError;
        },
    };

    const consumed = query.len - token.tail.len;
    self.advancePosition(query[0..consumed]);
    return token;
}

fn scanQuotedLiteral(
    self: *Scanner,
    allocator: Allocator,
    query: []const u8,
    reporter: *ErrorReporter,
) Error!ScannedToken {
    const opener = query[0];
    var escaped: bool = false;
    var hasEscapes: bool = false;
    for (1..query.len) |i| {
        const char = query[i];
        if (char == opener and !escaped) {
            const lexeme = if (hasEscapes) try self.unescapeQuotedLiteral(allocator, query[1..i]) else query[1..i];
            return .{
                .token = .{
                    .kind = .Literal,
                    .lexeme = lexeme,
                    .line = self.line,
                    .col = self.col,
                },
                .tail = query[i + 1 ..],
            };
        }

        if (char == '\\' and !escaped) {
            hasEscapes = true;
        }
        escaped = char == '\\' and !escaped;
    }

    _ = reporter.reportSyntaxError(.{
        .line = self.line,
        .col = self.col,
        .message = "Unterminated quoted literal.",
    });
    return Error.SyntaxError;
}

fn unescapeQuotedLiteral(self: *Scanner, allocator: Allocator, raw: []const u8) Allocator.Error![]const u8 {
    try self.quotedLiterals.ensureUnusedCapacity(allocator, 1);

    var buf = try std.ArrayList(u8).initCapacity(allocator, raw.len);
    errdefer buf.deinit(allocator);

    var escaped = false;
    for (raw) |char| {
        if (escaped) {
            buf.appendAssumeCapacity(char);
            escaped = false;
        } else if (char == '\\') {
            escaped = true;
        } else {
            buf.appendAssumeCapacity(char);
        }
    }

    if (escaped) {
        buf.appendAssumeCapacity('\\');
    }

    const literal = try buf.toOwnedSlice(allocator);
    errdefer allocator.free(literal);
    self.quotedLiterals.appendAssumeCapacity(literal);
    return literal;
}

// TODO: benchmark if it's better to return the position shift
// from the token, not to iterate over the consumed query again
fn advancePosition(self: *Scanner, consumed: []const u8) void {
    for (consumed) |ch| {
        if (ch == '\n') {
            self.line += 1;
            self.col = 1;
        } else {
            self.col += 1;
        }
    }
}

test "Scanner.scan table-driven" {
    const alloc = testing.allocator;

    const Case = struct {
        query: []const u8,
        expectedTokens: []const Token,
        expectedErr: ?anyerror = null,
        expectedSyntaxErrors: []const ErrorReporter.SyntaxError,
    };

    const cases = [_]Case{
        .{
            .query = "[]{or}\n(and)",
            .expectedTokens = &[_]Token{
                .{ .kind = .LeftSquareBracket, .lexeme = "[", .line = 1, .col = 1 },
                .{ .kind = .RightSquareBracket, .lexeme = "]", .line = 1, .col = 2 },
                .{ .kind = .LeftCurlyBracket, .lexeme = "{", .line = 1, .col = 3 },
                .{ .kind = .Or, .lexeme = "or", .line = 1, .col = 4 },
                .{ .kind = .RightCurlyBracket, .lexeme = "}", .line = 1, .col = 6 },
                .{ .kind = .LeftParenthesis, .lexeme = "(", .line = 2, .col = 1 },
                .{ .kind = .And, .lexeme = "and", .line = 2, .col = 2 },
                .{ .kind = .RightParenthesis, .lexeme = ")", .line = 2, .col = 5 },
            },
            .expectedSyntaxErrors = &[_]ErrorReporter.SyntaxError{},
        },
        .{
            .query = "andrew and or orban",
            .expectedTokens = &[_]Token{
                .{ .kind = .Literal, .lexeme = "andrew", .line = 1, .col = 1 },
                .{ .kind = .And, .lexeme = "and", .line = 1, .col = 8 },
                .{ .kind = .Or, .lexeme = "or", .line = 1, .col = 12 },
                .{ .kind = .Literal, .lexeme = "orban", .line = 1, .col = 15 },
            },
            .expectedSyntaxErrors = &[_]ErrorReporter.SyntaxError{},
        },
        .{
            .query = "message='msg=\"failed\"' field=\"msg=\\\"failed\\\"\" other=\"msg='failed'\"",
            .expectedTokens = &[_]Token{
                .{ .kind = .Literal, .lexeme = "message", .line = 1, .col = 1 },
                .{ .kind = .Equal, .lexeme = "=", .line = 1, .col = 8 },
                .{ .kind = .Literal, .lexeme = "msg=\"failed\"", .line = 1, .col = 9 },
                .{ .kind = .Literal, .lexeme = "field", .line = 1, .col = 24 },
                .{ .kind = .Equal, .lexeme = "=", .line = 1, .col = 29 },
                .{ .kind = .Literal, .lexeme = "msg=\"failed\"", .line = 1, .col = 30 },
                .{ .kind = .Literal, .lexeme = "other", .line = 1, .col = 47 },
                .{ .kind = .Equal, .lexeme = "=", .line = 1, .col = 52 },
                .{ .kind = .Literal, .lexeme = "msg='failed'", .line = 1, .col = 53 },
            },
            .expectedSyntaxErrors = &[_]ErrorReporter.SyntaxError{},
        },
        .{
            .query = "message='unterminated",
            .expectedTokens = &[_]Token{
                .{ .kind = .Literal, .lexeme = "message", .line = 1, .col = 1 },
                .{ .kind = .Equal, .lexeme = "=", .line = 1, .col = 8 },
            },
            .expectedErr = Error.SyntaxError,
            .expectedSyntaxErrors = &[_]ErrorReporter.SyntaxError{
                .{ .line = 1, .col = 9, .message = "Unterminated quoted literal." },
            },
        },
        .{
            .query = "[]{}\n()\\",
            .expectedTokens = &[_]Token{
                .{ .kind = .LeftSquareBracket, .lexeme = "[", .line = 1, .col = 1 },
                .{ .kind = .RightSquareBracket, .lexeme = "]", .line = 1, .col = 2 },
                .{ .kind = .LeftCurlyBracket, .lexeme = "{", .line = 1, .col = 3 },
                .{ .kind = .RightCurlyBracket, .lexeme = "}", .line = 1, .col = 4 },
                .{ .kind = .LeftParenthesis, .lexeme = "(", .line = 2, .col = 1 },
                .{ .kind = .RightParenthesis, .lexeme = ")", .line = 2, .col = 2 },
            },
            .expectedErr = Error.SyntaxError,
            .expectedSyntaxErrors = &[_]ErrorReporter.SyntaxError{
                .{ .line = 2, .col = 3, .message = "unexpected token" },
            },
        },
    };

    for (cases) |case| {
        var scanner = Scanner{};
        defer scanner.deinit(alloc);

        var reporter: ErrorReporter = .{};

        const scanResult = scanner.scan(alloc, case.query, &reporter);
        if (case.expectedErr) |expectedErr| {
            try testing.expectError(expectedErr, scanResult);
        } else {
            try scanResult;
        }

        try testing.expectEqualDeep(case.expectedTokens, scanner.tokens.items);
        try testing.expectEqualDeep(case.expectedSyntaxErrors, reporter.syntaxErrors());
    }
}

test "isKeyword" {
    const cases = &[_]struct {
        word: []const u8,
        expected: ?TokenKind,
    }{
        .{ .word = "or", .expected = .Or },
        .{ .word = "OR", .expected = .Or },
        .{ .word = "and", .expected = .And },
        .{ .word = "AND", .expected = .And },
        .{ .word = "orban", .expected = null },
        .{ .word = "andrew", .expected = null },
    };

    for (cases) |case| {
        try testing.expectEqual(case.expected, isKeyword(case.word));
    }
}
