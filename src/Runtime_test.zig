const std = @import("std");

const Runtime = @import("Runtime.zig");

const testing = std.testing;

test "getFreeDiskSpace returns same positive value" {
    const r = try Runtime.init(testing.io, testing.allocator, ".", 0.5);
    defer r.deinit(testing.allocator);

    const first = r.getFreeDiskSpace(testing.io);
    const second = r.getFreeDiskSpace(testing.io);

    try testing.expect(first > 0);
    try testing.expectEqual(first, second);
}
