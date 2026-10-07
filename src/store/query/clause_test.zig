const std = @import("std");

const clause = @import("clause.zig");

test "collectOrs" {
    const f = struct {
        fn f() !void {
            try std.testing.expect(true);
        }
    }.f;

    try f();
}
