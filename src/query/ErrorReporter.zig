const std = @import("std");
const Allocator = std.mem.Allocator;

pub const SyntaxError = struct {
    line: u16,
    col: u16,
    message: []const u8,
};

const maxErrs = 16;

/// ErrorReporter is responsible for reporting errors during the translation process,
/// it provides user readable error messages in order to debug the passed query.
pub const ErrorReporter = @This();

errs: std.ArrayList(SyntaxError) = .empty,

pub fn init(allocator: Allocator) !ErrorReporter {
    var errs = try std.ArrayList(SyntaxError).initCapacity(allocator, maxErrs);
    errdefer errs.deinit(allocator);

    return .{
        .errs = errs,
    };
}

pub fn deinit(self: *ErrorReporter, allocator: Allocator) void {
    self.errs.deinit(allocator);
}

pub fn reset(self: *ErrorReporter) void {
    self.errs.clearRetainingCapacity();
}

pub fn reportSyntaxError(self: *ErrorReporter, err: SyntaxError) void {
    self.errs.appendBounded(err) catch {
        // do nothing, gather only 16 errors is enough
    };
}

pub fn syntaxErrors(self: *const ErrorReporter) []const SyntaxError {
    return self.errs.items;
}
