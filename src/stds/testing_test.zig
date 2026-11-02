const std = @import("std");
const Allocator = std.mem.Allocator;

const testOptions = @import("test_options");

const stesting = @import("testing.zig");
const checkAllAllocationFailures = stesting.checkAllAllocationFailures;
const checkAllocationFailuresAllowingSwallow = stesting.checkAllocationFailuresAllowingSwallow;

test "allocation failure wrapper counts and forwards extra arguments" {
    const State = struct {
        runs: usize = 0,
        failures: usize = 0,
        successes: usize = 0,
    };
    const f = struct {
        fn f(a: Allocator, state: *State, size: usize, marker: []const u8) !void {
            state.runs += 1;
            errdefer |err| {
                if (err == error.OutOfMemory) state.failures += 1;
            }
            try std.testing.expectEqualStrings("forwarded", marker);
            const first = try a.alloc(u8, size);
            defer a.free(first);
            const second = try a.alloc(u8, size + 1);
            defer a.free(second);
            state.successes += 1;
        }
    }.f;

    inline for (.{ checkAllAllocationFailures, checkAllocationFailuresAllowingSwallow }) |checker| {
        var state: State = .{};
        try checker(std.testing.allocator, f, .{ &state, 8, "forwarded" });
        const expected: State = if (testOptions.allocationFailures)
            .{ .runs = 3, .failures = 2, .successes = 1 }
        else
            .{ .runs = 1, .failures = 0, .successes = 1 };
        try std.testing.expectEqualDeep(expected, state);
    }
}
