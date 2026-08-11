const std = @import("std");
const Io = std.Io;

// TODO: replace to std Semaphore.waitTimout:
// https://codeberg.org/ziglang/zig/commit/21980c82f48f239e50239d5af264706d15829268
/// Blocks until a `permit` is available and consumes a single one.
/// Unblocks without consuming a `permit` when canceled or when the provided
/// timeout expires before a `permit` is available.
///
/// See also:
/// * `wait`
/// * `waitUncancelable`
pub fn timedWait(s: *Io.Semaphore, io: Io, timeout: u64) !void {
    const now = Io.Timestamp.now(io, .real);
    const deadline = now.addDuration(.{ .nanoseconds = timeout });
    try s.mutex.lock(io);
    defer s.mutex.unlock(io);
    while (s.permits == 0) try waitTimeoutCond(&s.cond, io, &s.mutex, .{ .deadline = .{ .raw = deadline, .clock = .real } });
    s.permits -= 1;
    if (s.permits > 0) s.cond.signal(io);
}

/// Blocks until the condition is signaled, canceled, or the provided
/// timeout expires.
///
/// See also:
/// * `wait`
/// * `waitUncancelable`
pub fn waitTimeoutCond(cond: *Io.Condition, io: Io, mutex: *Io.Mutex, timeout: Io.Timeout) !void {
    const deadline = timeout.toDeadline(io);

    var epoch = cond.epoch.load(.acquire); // `.acquire` to ensure ordered before state load

    {
        const prev_state = cond.state.fetchAdd(.{ .waiters = 1, .signals = 0 }, .monotonic);
        std.debug.assert(prev_state.waiters < std.math.maxInt(u16)); // overflow caused by too many waiters
    }

    mutex.unlock(io);
    defer mutex.lockUncancelable(io);

    while (true) {
        const result = io.futexWaitTimeout(u32, &cond.epoch.raw, epoch, deadline);

        epoch = cond.epoch.load(.acquire); // `.acquire` to ensure ordered before `state` laod

        // We were woken normally, so try to consume a pending signal. A signal takes
        // priority over an expired deadline, so this is checked before the deadline
        // below. On error we safely remove ourselves as a waiter and propagate the error.
        if (result) |_| {
            var prev_state = cond.state.load(.monotonic);
            while (prev_state.signals > 0) {
                prev_state = cond.state.cmpxchgWeak(prev_state, .{
                    .waiters = prev_state.waiters - 1,
                    .signals = prev_state.signals - 1,
                }, .acquire, .monotonic) orelse {
                    // We successfully consumed a signal.
                    return;
                };
            }
        } else |err| {
            deregister(cond, io);
            return err;
        }

        // There are no signals available and no error; if a timeout was specified and
        // the deadline has passed, remove ourselves as a waiter and return
        // `error.Timeout`. Otherwise, this was a spurious wakeup: loop back to the
        // futex wait.
        switch (deadline) {
            .none => {},
            .deadline => |d| if (d.untilNow(io).raw.nanoseconds >= 0) {
                deregister(cond, io);
                return error.Timeout;
            },
            .duration => unreachable,
        }
    }
}

fn deregister(cond: *Io.Condition, io: Io) void {
    var prev_state = cond.state.load(.monotonic);
    while (true) {
        const new_signals = @min(prev_state.signals, prev_state.waiters - 1);
        prev_state = cond.state.cmpxchgWeak(prev_state, .{
            .waiters = prev_state.waiters - 1,
            .signals = new_signals,
        }, .monotonic, .monotonic) orelse {
            if (prev_state.signals > 0 and prev_state.signals < prev_state.waiters) {
                // We kept a signal we are not consuming; wake a remaining waiter for it.
                _ = cond.epoch.fetchAdd(1, .release);
                io.futexWake(u32, &cond.epoch.raw, 1);
            }
            return;
        };
    }
}
