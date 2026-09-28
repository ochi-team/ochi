const std = @import("std");

const Token = @import("Scanner.zig").Token;
const Error = @import("Scanner.zig").Error;
const Parser = @import("Parser.zig");
const QuerySet = Parser.QuerySet;
const ErrorReporter = @import("ErrorReporter.zig");

const expectCloseParenthesis = Parser.expectCloseParenthesis;
const expectCloseCurlyBracket = Parser.expectCloseCurlyBracket;
const expectExpression = Parser.expectExpression;

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
