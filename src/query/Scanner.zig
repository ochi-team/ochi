const std = @import("std");
const Allocator = std.mem.Allocator;

const ErrorReporter = @import("ErrorReporter.zig");

pub const Error = error{SyntaxError};

const ScannedToken = struct {
    token: ?Token,
    tail: []const u8,
};

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

    pub fn reset(self: *Scanner, allocator: Allocator) void {
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
                    Error.SyntaxError => {
                        reporter.reportSyntaxError(tail);
                        return err;
                    },
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
                    reporter.reportSyntaxError(query);
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
                    self.line += 1;
                    const nextLineIdx = std.mem.indexOf(u8, query, '\n') orelse query.len;
                    break :blk .{
                        .token = null,
                        .tail = query[nextLineIdx..],
                    };
                } else {
                    reporter.reportSyntaxError(query);
                    return Error.SyntaxError;
                }
            },
            // whitespace
            '\n' => blk: {
                self.line += 1;
                break :blk .{
                    .token = null,
                    .tail = query[1..],
                };
            },
            ' ', '\t' => blk: {
                break :blk .{
                    .token = null,
                    .tail = query[1..],
                };
            },
        };

        self.current += if (token.token) |t| t.lexeme.len else 0;
        return token;
    }
};
