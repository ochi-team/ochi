const std = @import("std");
const testing = std.testing;

const ErrorReporter = @import("ErrorReporter.zig");
const Scanner = @import("Scanner.zig");
const Token = Scanner.Token;
const TokenKind = Scanner.TokenKind;
const Error = Scanner.Error;
const isKeyword = Scanner.isKeyword;

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
