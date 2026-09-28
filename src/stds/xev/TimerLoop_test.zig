const std = @import("std");

const TimerLoop = @import("TimerLoop.zig");

const testing = std.testing;

test "TimerLoop fires a repeating timer and stops cleanly" {
    const alloc = testing.allocator;
    const io = testing.io;

    const loop = try TimerLoop.init(alloc);
    defer loop.deinit();

    var counter: usize = 0;
    const tick = struct {
        fn run(ctx: *anyopaque) void {
            const c: *usize = @ptrCast(@alignCast(ctx));
            c.* += 1;
        }
    }.run;

    try loop.addTimer(5 * std.time.ns_per_ms, &counter, tick);
    try loop.start();

    try std.Io.sleep(io, .fromMilliseconds(50), .real);

    loop.stop();
    loop.join();

    try testing.expect(counter > 0);
}

test "TimerLoop arms timers added after start, shared across multiple owners" {
    const alloc = testing.allocator;
    const io = testing.io;

    const loop = try TimerLoop.init(alloc);
    defer loop.deinit();

    var counterA: usize = 0;
    var counterB: usize = 0;
    const tick = struct {
        fn run(ctx: *anyopaque) void {
            const c: *usize = @ptrCast(@alignCast(ctx));
            c.* += 1;
        }
    }.run;

    try loop.addTimer(5 * std.time.ns_per_ms, &counterA, tick);
    try loop.start();
    // start() must be idempotent: recorders sharing the loop all call it
    try loop.start();

    // registered after the loop is already running, simulating a partition
    // opened later on top of a Store-owned shared TimerLoop
    try loop.addTimer(5 * std.time.ns_per_ms, &counterB, tick);

    try std.Io.sleep(io, .fromMilliseconds(50), .real);

    loop.stop();
    loop.join();

    try testing.expect(counterA > 0);
    try testing.expect(counterB > 0);
}
