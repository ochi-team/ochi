const std = @import("std");

const Runtime = @import("Runtime.zig");
const Conf = @import("Conf.zig");

const testing = std.testing;

test "maxQueryConnectionsLimit" {
    const c = Conf.default();
    const r1: Runtime = .{ .cpus = 1, .maxMem = undefined, .cacheSize = undefined, .diskSpace = undefined, .path = undefined };
    const r2: Runtime = .{ .cpus = 4, .maxMem = undefined, .cacheSize = undefined, .diskSpace = undefined, .path = undefined };
    const r3: Runtime = .{ .cpus = 8, .maxMem = undefined, .cacheSize = undefined, .diskSpace = undefined, .path = undefined };
    const r4: Runtime = .{ .cpus = 16, .maxMem = undefined, .cacheSize = undefined, .diskSpace = undefined, .path = undefined };
    const r5: Runtime = .{ .cpus = 32, .maxMem = undefined, .cacheSize = undefined, .diskSpace = undefined, .path = undefined };
    try testing.expectEqual(4, c.app.maxQueryConnectionsLimit(&r1));
    try testing.expectEqual(4, c.app.maxQueryConnectionsLimit(&r2));
    try testing.expectEqual(8, c.app.maxQueryConnectionsLimit(&r3));
    try testing.expectEqual(16, c.app.maxQueryConnectionsLimit(&r4));
    try testing.expectEqual(16, c.app.maxQueryConnectionsLimit(&r5));
}
