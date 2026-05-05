/// Logql is a Logs Ochi Query Language,
/// translates a query to ochi query API
const std = @import("std");
const Allocator = std.mem.Allocator;

const Scanner = @import("Scanner.zig");
const Parser = @import("Parser.zig");
const Translator = @import("Translator.zig");
const ErrorReporter = @import("ErrorReporter.zig");

const Query = @import("Query.zig");

const Loql = @This();

scanner: Scanner = .{},
parser: Parser = .{},
translator: Translator = .{},

// TODO: it must accept a reader, not a query string
pub fn translateQuery(
    self: *Loql,
    allocator: Allocator,
    reporter: *ErrorReporter.ErrorReporter,
    fullQueryStr: []const u8,
    nowNs: u64,
) !Query {
    const trimmedQueryStr = std.mem.trim(u8, fullQueryStr, " \n\t");

    // TODO: validate whether it's a syntax error and return report content
    // or unknown error
    // TODO: most likely scanner has a state, we must reset it
    try self.scanner.scan(allocator, trimmedQueryStr, reporter);
    const expr = try self.parser.querySet(allocator, self.scanner.tokens.items, reporter);
    const query = try self.translator.query(allocator, expr, nowNs);

    return query;
}
