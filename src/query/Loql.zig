/// Logql is a Logs Ochi Query Language,
/// translates a query to ochi query API
const std = @import("std");
const Allocator = std.mem.Allocator;

const Scanner = @import("Scanner.zig");
const Parser = @import("Parser.zig");
const Translator = @import("Translator.zig");
const ErrorReporter = @import("ErrorReporter.zig");

const Query = @import("Query.zig");

pub const QueryError = error{
    EmptyQuery,
    QueryTooLong,
};

const Loql = @This();

scanner: Scanner = .{},
parser: Parser = .{},
translator: Translator = .{},

pub fn deinit(self: *Loql, allocator: Allocator) void {
    self.scanner.deinit(allocator);
    self.parser.deinit(allocator);
    self.translator.deinit(allocator);
}

pub const maxQueryLength = 2048;
// -128 for timestamps, in case they are passed as short duration,
// but we need to parse as full timestamps (-64)
// and a little gap for safety (-64)
pub const maxQueryBodyLength = maxQueryLength - 128;
// TODO: it must accept a reader probably, not a query string
pub fn translateQuery(
    self: *Loql,
    allocator: Allocator,
    reporter: *ErrorReporter,
    fullQueryStr: []const u8,
    nowNs: u64,
) !Query {
    const trimmedQueryStr = std.mem.trim(u8, fullQueryStr, " \n\t");
    if (trimmedQueryStr.len == 0) {
        return error.EmptyQuery;
    }
    if (trimmedQueryStr.len > maxQueryLength) {
        return error.QueryTooLong;
    }

    // TODO: validate whether it's a syntax error and return report content
    // or unknown error
    try self.scanner.scan(allocator, trimmedQueryStr, reporter);
    const expr = try self.parser.querySet(allocator, self.scanner.tokens.items, reporter);
    const query = try self.translator.query(allocator, expr, nowNs);

    return query;
}

test {
    _ = @import("Loql_test.zig");
}
