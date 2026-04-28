const std = @import("std");
const Allocator = std.mem.Allocator;

const SyntaxError = struct {
    line: u16,
    col: u16,
    message: []const u8,
};

/// ErrorReporter is responsible for reporting errors during the translation process,
/// it provides user readable error messages in order to debug the passed query.
pub const ErrorReporter = @This();

errs: std.ArrayList(SyntaxError) = .empty,

pub fn init(allocator: Allocator) !ErrorReporter {
    var errs = try std.ArrayList(SyntaxError).init(allocator);
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

fn reportSyntaxError(self: *ErrorReporter, err: SyntaxError) void {
    self.errs.appendBounded(err) catch |e| {
        _ = e;
        // do nothing, gather only 16 errors is enough
    };
}
