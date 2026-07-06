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

const testing = std.testing;

test "Ring.next cycles over caller resources" {
    const Case = struct {
        resources: []const i32,
        takes: usize,
        expected: []const i32,
    };

    const cases = [_]Case{
        .{
            .resources = &.{1},
            .takes = 4,
            .expected = &.{ 1, 1, 1, 1 },
        },
        .{
            .resources = &.{ 10, 20, 30 },
            .takes = 8,
            .expected = &.{ 10, 20, 30, 10, 20, 30, 10, 20 },
        },
    };

    inline for (cases) |case| {
        var resources: [case.resources.len]i32 = undefined;
        @memcpy(&resources, case.resources);

        var ring = Ring(i32).init(&resources);
        var actual: [case.takes]i32 = undefined;

        var i: usize = 0;
        while (i < case.takes) : (i += 1) {
            actual[i] = ring.next().*;
        }

        try testing.expectEqualDeep(case.expected, &actual);
    }
}

test "Ring.next returns mutable pointers to resources" {
    var resources = [_]i32{ 1, 2 };
    var ring = Ring(i32).init(resources[0..]);

    ring.next().* += 10;
    ring.next().* += 20;
    ring.next().* += 30;

    try testing.expectEqualDeep(&[_]i32{ 41, 22 }, &resources);
}
