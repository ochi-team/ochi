const std = @import("std");
const Io = std.Io;

pub const WorkerEvent = struct {
    event: Io.Event = .unset,
    pending: std.atomic.Value(bool) = .init(false),

    pub fn notify(self: *WorkerEvent, io: Io) void {
        self.pending.store(true, .release);
        self.event.set(io);
    }

    pub fn wait(self: *WorkerEvent, io: Io, stopped: *const std.atomic.Value(bool)) bool {
        if (stopped.load(.acquire)) return false;
        if (self.pending.swap(false, .acq_rel)) return !stopped.load(.acquire);

        self.event.reset();
        if (self.pending.swap(false, .acq_rel)) return !stopped.load(.acquire);

        self.event.waitUncancelable(io);
        self.event.reset();
        _ = self.pending.swap(false, .acq_rel);
        return !stopped.load(.acquire);
    }

    pub fn waitOrTimeout(
        self: *WorkerEvent,
        io: Io,
        stopped: *const std.atomic.Value(bool),
        ns: u64,
    ) bool {
        if (stopped.load(.acquire)) return false;
        if (self.pending.swap(false, .acq_rel)) return !stopped.load(.acquire);

        self.event.reset();
        if (self.pending.swap(false, .acq_rel)) return !stopped.load(.acquire);

        self.event.waitTimeout(io, .{
            .duration = .{
                .raw = .fromNanoseconds(@intCast(ns)),
                .clock = .real,
            },
        }) catch |err| switch (err) {
            error.Timeout => {},
            error.Canceled => return false,
        };
        self.event.reset();
        _ = self.pending.swap(false, .acq_rel);

        return !stopped.load(.acquire);
    }
};

pub fn sleepOrStop(io: Io, stopped: *const std.atomic.Value(bool), ns: u64) void {
    // TODO: make this interval configurable,
    // it must be shorter for tests and longer for production

    const stepNs = std.time.ns_per_ms * 100;
    var remaining = ns;

    while (remaining > 0) {
        // TODO: this is an anti-pattern in concurrent programming.
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
