const std = @import("std");

const PprofAllocator = @import("PprofAllocator.zig");

test "sample: bytes unit samples when accumulated bytes reach ratio" {
    var pprof = PprofAllocator{
        .child = std.testing.allocator,
        .io = undefined,
        .samplerUnit = .bytes,
        .sampleRatio = 100,
    };

    // 30
    try std.testing.expect(!pprof.sample(30));
    try std.testing.expectEqual(30, pprof.sampleState.load(.monotonic));

    // 70
    try std.testing.expect(!pprof.sample(40));
    try std.testing.expectEqual(70, pprof.sampleState.load(.monotonic));

    // 110
    try std.testing.expect(pprof.sample(40));
    try std.testing.expectEqual(10, pprof.sampleState.load(.monotonic));
}

test "sample: count unit samples every Nth allocation" {
    var pprof = PprofAllocator{
        .child = std.testing.allocator,
        .io = undefined,
        .samplerUnit = .count,
        .sampleRatio = 3,
    };

    // Allocation #1.
    try std.testing.expect(!pprof.sample(100));
    try std.testing.expectEqual(1, pprof.sampleState.load(.monotonic));

    // Allocation #2.
    try std.testing.expect(!pprof.sample(200));
    try std.testing.expectEqual(2, pprof.sampleState.load(.monotonic));

    // Allocation #3 reaches the ratio.
    try std.testing.expect(pprof.sample(300));
    try std.testing.expectEqual(0, pprof.sampleState.load(.monotonic));

    // Start the next sampling interval.
    try std.testing.expect(!pprof.sample(400));
    try std.testing.expectEqual(1, pprof.sampleState.load(.monotonic));

    try std.testing.expect(!pprof.sample(500));
    try std.testing.expectEqual(2, pprof.sampleState.load(.monotonic));

    try std.testing.expect(pprof.sample(600));
    try std.testing.expectEqual(0, pprof.sampleState.load(.monotonic));
}

test "alloc/free bucket accounting" {
    const alloc0 = std.testing.allocator;
    var profiler: PprofAllocator = .{ .child = alloc0, .io = std.testing.io };
    defer profiler.deinit();

    const profAlloc = profiler.allocator();
    const buf = try profAlloc.alloc(u8, 128);

    const bucketsAfterAlloc = try profiler.snapshot(alloc0);
    defer alloc0.free(bucketsAfterAlloc);
    try std.testing.expectEqual(1, bucketsAfterAlloc.len);
    try std.testing.expectEqual(1, bucketsAfterAlloc[0].allocObjects);
    try std.testing.expectEqual(128, bucketsAfterAlloc[0].allocBytes);
    try std.testing.expectEqual(1, bucketsAfterAlloc[0].inuseObjects);
    try std.testing.expectEqual(128, bucketsAfterAlloc[0].inuseBytes);

    profAlloc.free(buf);

    const bucketsAfterFree = try profiler.snapshot(alloc0);
    defer alloc0.free(bucketsAfterFree);
    try std.testing.expectEqual(1, bucketsAfterFree.len);
    try std.testing.expectEqual(1, bucketsAfterFree[0].allocObjects);
    try std.testing.expectEqual(128, bucketsAfterFree[0].allocBytes);
    try std.testing.expectEqual(0, bucketsAfterFree[0].inuseObjects);
    try std.testing.expectEqual(0, bucketsAfterFree[0].inuseBytes);
}
