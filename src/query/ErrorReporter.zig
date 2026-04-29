const std = @import("std");

pub const SyntaxError = struct {
    line: u16,
    col: u16,
    message: []const u8,
};

/// ErrorReporter is responsible for reporting errors during the translation process,
/// it provides user readable error messages in order to debug the passed query.
pub const ErrorReporter = @This();

errs: std.ArrayList(SyntaxError) = .empty,

pub fn init(buf: []SyntaxError) ErrorReporter {
    const errs = std.ArrayList(SyntaxError).initBuffer(buf);

    return .{
        .errs = errs,
    };
}

pub fn reset(self: *ErrorReporter) void {
    self.errs.clearRetainingCapacity();
}

pub fn reportSyntaxError(self: *ErrorReporter, err: SyntaxError) bool {
    self.errs.appendBounded(err) catch {
        // do nothing, gather only 16 errors is enough
        return false;
    };

    return true;
}

pub fn syntaxErrors(self: *const ErrorReporter) []const SyntaxError {
    return self.errs.items;
}
