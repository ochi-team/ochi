const std = @import("std");
const Allocator = std.mem.Allocator;
const testing = std.testing;

const ErrorReporter = @import("ErrorReporter.zig");

pub const Error = error{SyntaxError};

const ScannedToken = struct {
    token: ?Token,
    tail: []const u8,
};

fn isKeyword(word: []const u8) ?TokenKind {
    if (std.mem.eql(u8, word, "or")) return .Or;
    if (std.mem.eql(u8, word, "and")) return .And;

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
    Pipe,
    And,
    Or,
};

pub const Token = struct {
    kind: TokenKind,
    lexeme: []const u8,
    // TODO: implement a query size validation min max
    line: u16,
    col: u16,
};

pub const Scanner = struct {
    /// tokens holds the current parsed tokens state,
    /// we keep it asa member to reuse the allocated memory
    tokens: std.ArrayList(Token) = .empty,

    // state
    current: u16 = 0,
    line: u16 = 1,
    col: u16 = 1,

    pub fn deinit(self: *Scanner, allocator: Allocator) void {
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
            const next = self.scanToken(tail, reporter) catch |err| {
                switch (err) {
                    Error.SyntaxError => return err,
                }
            };
            tail = next.tail;
            if (next.token) |token| {
                try self.tokens.append(allocator, token);
            }
        }
    }

    fn scanToken(self: *Scanner, query: []const u8, reporter: *ErrorReporter) Error!ScannedToken {
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
                } else {
                    reporter.reportSyntaxError(.{ .line = self.line, .col = self.col, .message = "unexpected token: !" });
                    return Error.SyntaxError;
                }
            },
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
            // comments
            '/' => blk: {
                if (query.len > 1 and query[1] == '/') {
                    const nextLineIdx = std.mem.indexOfScalar(u8, query, '\n') orelse query.len;
                    const consumed = if (nextLineIdx < query.len) nextLineIdx + 1 else nextLineIdx;
                    break :blk .{
                        .token = null,
                        .tail = query[consumed..],
                    };
                } else {
                    reporter.reportSyntaxError(
                        .{ .line = self.line, .col = self.col, .message = "unexpected token: /" },
                    );
                    return Error.SyntaxError;
                }
            },
            'a' => blk: {
                if (query.len > 2 and std.mem.eql(u8, query[0..3], "and")) {
                    break :blk .{
                        .token = .{
                            .kind = .And,
                            .lexeme = query[0..3],
                            .line = self.line,
                            .col = self.col,
                        },
                        .tail = query[3..],
                    };
                }

                reporter.reportSyntaxError(
                    .{ .line = self.line, .col = self.col, .message = "unexpected token" },
                );
                return Error.SyntaxError;
            },
            'o' => blk: {
                if (query.len > 1 and std.mem.eql(u8, query[0..2], "or")) {
                    break :blk .{
                        .token = .{
                            .kind = .Or,
                            .lexeme = query[0..2],
                            .line = self.line,
                            .col = self.col,
                        },
                        .tail = query[2..],
                    };
                }

                reporter.reportSyntaxError(
                    .{ .line = self.line, .col = self.col, .message = "unexpected token" },
                );
                return Error.SyntaxError;
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
                reporter.reportSyntaxError(.{ .line = self.line, .col = self.col, .message = "unexpected token" });
                return Error.SyntaxError;
            },
        };

        const consumed = query.len - token.tail.len;
        self.advancePosition(query[0..consumed]);
        return token;
    }

    // TODO: benchmark if it's better to return the position shift
    // from the token, not to iterate over the consumed query again
    fn advancePosition(self: *Scanner, consumed: []const u8) void {
        self.current += @intCast(consumed.len);
        for (consumed) |ch| {
            if (ch == '\n') {
                self.line += 1;
                self.col = 1;
            } else {
                self.col += 1;
            }
        }
    }
};

test "scan returns token sequence and syntax error position" {
    const alloc = testing.allocator;

    var okScanner = Scanner{};
    defer okScanner.deinit(alloc);

    var okReporter = try ErrorReporter.init(alloc);
    defer okReporter.deinit(alloc);

    try okScanner.scan(alloc, "[]{or}\n(and)", &okReporter);
    try testing.expectEqual(@as(usize, 8), okScanner.tokens.items.len);

    const expectedTokens = [_]Token{
        .{ .kind = .LeftSquareBracket, .lexeme = "[", .line = 1, .col = 1 },
        .{ .kind = .RightSquareBracket, .lexeme = "]", .line = 1, .col = 2 },
        .{ .kind = .LeftCurlyBracket, .lexeme = "{", .line = 1, .col = 3 },
        .{ .kind = .Or, .lexeme = "or", .line = 1, .col = 4 },
        .{ .kind = .RightCurlyBracket, .lexeme = "}", .line = 1, .col = 6 },
        .{ .kind = .LeftParenthesis, .lexeme = "(", .line = 2, .col = 1 },
        .{ .kind = .And, .lexeme = "and", .line = 2, .col = 2 },
        .{ .kind = .RightParenthesis, .lexeme = ")", .line = 2, .col = 5 },
    };
    try testing.expectEqualDeep(expectedTokens[0..], okScanner.tokens.items);
    try testing.expectEqual(@as(usize, 0), okReporter.syntaxErrors().len);

    var errScanner = Scanner{};
    defer errScanner.deinit(alloc);

    var errReporter = try ErrorReporter.init(alloc);
    defer errReporter.deinit(alloc);

    const err = errScanner.scan(alloc, "[]{}\n()\\", &errReporter);
    try testing.expectError(Error.SyntaxError, err);

    const expectedErr = [_]ErrorReporter.SyntaxError{
        .{ .line = 2, .col = 3, .message = "unexpected token" },
    };
    try testing.expectEqualDeep(expectedErr[0..], errReporter.syntaxErrors());
}
