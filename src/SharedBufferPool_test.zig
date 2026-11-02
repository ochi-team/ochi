const std = @import("std");
const testing = std.testing;
const SharedBufferPool = @import("SharedBufferPool.zig");

test "shared buffers are bounded and recycled only after the last release" {
    for ([_]usize{ 1, 2, 8 }) |capacity| {
        const pool = try SharedBufferPool.init(testing.io, testing.allocator, capacity);
        defer pool.deinit();
        var held: [8]*SharedBufferPool.Buffer = undefined;
        for (held[0..capacity]) |*buffer| buffer.* = (try pool.tryAcquire()).?;
        try testing.expectEqual(null, try pool.tryAcquire());

        const shared = held[0];
        shared.bytes[0] = 42;
        shared.retain();
        shared.release();
        try testing.expectEqual(null, try pool.tryAcquire());
        try testing.expectEqual(42, shared.bytes[0]);
        shared.release();

        const recycled = (try pool.tryAcquire()).?;
        try testing.expect(recycled == shared);
        try testing.expectEqual(1, recycled.refs.load(.acquire));
        recycled.release();
        for (held[1..capacity]) |buffer| buffer.release();
        try testing.expectEqual(capacity, pool.created);
    }
}

fn checkAllocationFailures(alloc: std.mem.Allocator) !void {
    const pool = try SharedBufferPool.init(testing.io, alloc, 2);
    defer pool.deinit();
    const first = (try pool.tryAcquire()).?;
    defer first.release();
    const second = (try pool.tryAcquire()).?;
    defer second.release();
    first.retain();
    first.release();
    try testing.expectEqual(null, try pool.tryAcquire());
}

test "shared buffer allocation failures release all backing storage" {
    try testing.checkAllAllocationFailures(testing.allocator, checkAllocationFailures, .{});
}
