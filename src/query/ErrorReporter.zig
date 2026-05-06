const std = @import("std");

pub const SyntaxError = struct {
    line: u16,
    col: u16,
    message: []const u8,
};

pub fn log(e: SyntaxError) void {
    std.debug.print("Syntax error at line {d}, column {d}: {s}\n", .{ e.line, e.col, e.message });
}

const maxErrors: usize = 4;

/// ErrorReporter is responsible for reporting errors during the translation process,
/// it provides user readable error messages in order to debug the passed query.
pub const ErrorReporter = @This();

errs: [maxErrors]SyntaxError = std.mem.zeroes([maxErrors]SyntaxError),
len: usize = 0,

pub fn reset(self: *ErrorReporter) void {
    self.errs.clearRetainingCapacity();
}

pub fn reportSyntaxError(self: *ErrorReporter, err: SyntaxError) bool {
    if (self.len >= maxErrors) {
        // too many errors, stop reporting
        return false;
    }

    self.errs[self.len] = err;
    self.len += 1;

    return true;
}

pub fn syntaxErrors(self: *const ErrorReporter) []const SyntaxError {
    return self.errs[0..self.len];
}
