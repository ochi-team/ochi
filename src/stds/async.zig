const std = @import("std");
const Io = std.Io;

pub fn sleepOrStop(io: Io, stopped: *const std.atomic.Value(bool), ns: u64) void {
    // TODO: make this interval configurable,
    // it must be shorter for tests and longer for production

    const stepNs = std.time.ns_per_ms * 100;
    var remaining = ns;

    while (remaining > 0) {
        // TODO this is an anti-pattern in concurrent programming.
        // a thread shouldn't waste cycles spinning. instead
        // it should sleep, and be signalled when to wake up
        // and do work. Using std.Io.Condition, or even better
        // redesigning the worker threads to use Queues as input/output.
        if (stopped.load(.acquire)) return;

        const s = @min(remaining, stepNs);
        Io.sleep(io, .fromNanoseconds(s), .real) catch |err| {
            std.debug.print("Sleep or stop error: {}", .{err});
        };
        remaining -= s;
    }
}
