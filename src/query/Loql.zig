/// Logql is a Logs Ochi Query Language,
/// translates a query to ochi query API
const std = @import("std");

const Scanner = @import("Scanner.zig");
const ErrorReporter = @import("ErrorReporter.zig");

const Loql = @This();

scanner: Scanner,
reporter: ErrorReporter,

// TODO: it must accept a reader, not a query string
pub fn translate(self: *const Loql, fullQuery: []const u8) ![]u8 {
    var query = std.mem.trim(u8, fullQuery, " ");
    query = std.mem.trim(u8, query, "\n");
    query = std.mem.trim(u8, query, "\t");
    for (query, 0..) |c, i| {
        query[i] = std.ascii.toLower(c);
    }

    // TODO: validate whether it's a syntax error and return report content
    // or unknown error
    // TODO: most likely scanner has a state, we must reset it
    const tokens = try self.scanner.scan(query, &self.reporter);
    for (tokens) |token| {
        std.debug.print("Token: {s}\n", .{token});
    }
}
