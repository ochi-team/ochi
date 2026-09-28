const std = @import("std");

pub fn Ring(comptime T: type) type {
    return struct {
        const Self = @This();

        resources: []T,
        nextIdx: std.atomic.Value(usize) = .init(0),

        pub fn init(resources: []T) Self {
            std.debug.assert(resources.len > 0);
            return .{ .resources = resources };
        }

        pub fn next(self: *Self) *T {
            const idx = self.nextIdx.fetchAdd(1, .monotonic);
            return &self.resources[idx % self.resources.len];
        }
    };
}

test {
    _ = @import("Ring_test.zig");
}
