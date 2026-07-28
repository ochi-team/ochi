const std = @import("std");
const Allocator = std.mem.Allocator;
const xev = @import("xev");
const Logger = @import("logging");

const TimerLoop = @This();

pub const TickFn = *const fn (ctx: *anyopaque) void;

const Entry = struct {
    parent: *TimerLoop,
    xevTimer: xev.Timer,
    completion: xev.Completion = .{},
    intervalMs: u64,
    ctx: *anyopaque,
    tick: TickFn,

    fn callback(
        ud: ?*Entry,
        loop: *xev.Loop,
        c: *xev.Completion,
        r: xev.Timer.RunError!void,
    ) xev.CallbackAction {
        _ = r catch {};
        const entry = ud.?;

        if (!entry.parent.stopping.load(.acquire)) {
            entry.tick(entry.ctx);
        }

        if (entry.parent.stopping.load(.acquire)) {
            return .disarm;
        }

        entry.xevTimer.run(loop, c, entry.intervalMs, Entry, entry, callback);
        return .disarm;
    }
};

alloc: Allocator,
loop: xev.Loop,
wake: xev.Async,
wakeCompletion: xev.Completion = .{},
thread: ?std.Thread = null,
entries: std.ArrayList(*Entry) = .empty,
// entries queued by addTimer() calls made after start(); drained and armed
// on the loop thread by wakeCallback, since xev.Loop isn't thread-safe.
// a spinlock is fine here: the critical section is a single array append
// and contention is rare
pendingMx: std.atomic.Mutex = .unlocked,
pendingEntries: std.ArrayList(*Entry) = .empty,
// callbacks invoked on every loop-thread wake, for owners that need to marshal
// their own state onto the loop thread (xev.Loop/Completion registration is
// only safe there) without standing up a second xev.Async of their own.
wakeHandlers: std.ArrayList(WakeHandler) = .empty,
stopping: std.atomic.Value(bool) = .init(false),
pool: std.heap.MemoryPool(Entry),

pub const WakeHandler = struct {
    ctx: *anyopaque,
    cb: *const fn (ctx: *anyopaque, loop: *xev.Loop) void,
};

pub fn init(alloc: Allocator) !*TimerLoop {
    const self = try alloc.create(TimerLoop);
    errdefer alloc.destroy(self);

    var loop = try xev.Loop.init(.{});
    errdefer loop.deinit();

    var wake = try xev.Async.init();
    errdefer wake.deinit();

    const pool: std.heap.MemoryPool(Entry) = try .initCapacity(alloc, 32);
    errdefer pool.deinit(alloc);

    self.* = .{
        .alloc = alloc,
        .loop = loop,
        .wake = wake,
        .pool = pool,
    };
    return self;
}

pub fn deinit(self: *TimerLoop) void {
    self.entries.deinit(self.alloc);
    self.pendingEntries.deinit(self.alloc);
    self.wakeHandlers.deinit(self.alloc);
    self.pool.deinit(self.alloc);
    self.wake.deinit();
    self.loop.deinit();
    self.alloc.destroy(self);
}

/// registers a timer that fires `tick(ctx)` every `intervalNs`,
/// re-arming itself until the TimerLoop is stopped. Safe to call after start().
/// Once started, the timer is armed on the loop thread as
/// soon as it wakes up, so several owners can keep registering timers as they come online
pub fn addTimer(self: *TimerLoop, intervalNs: u64, ctx: *anyopaque, tick: TickFn) !void {
    // here we lock pool allocator as well, no only pending entries,
    // create a block in order not to lock on notify
    {
        spinLock(&self.pendingMx);
        defer self.pendingMx.unlock();

        const entry = try self.pool.create(self.alloc);
        errdefer self.pool.destroy(entry);

        entry.* = .{
            .parent = self,
            .xevTimer = try xev.Timer.init(),
            .intervalMs = intervalNs / std.time.ns_per_ms,
            .ctx = ctx,
            .tick = tick,
        };

        if (self.thread == null) {
            try self.entries.append(self.alloc, entry);
            return;
        }

        try self.pendingEntries.append(self.alloc, entry);
    }

    self.notify();
}

pub fn spinLock(m: *std.atomic.Mutex) void {
    while (!m.tryLock()) std.atomic.spinLoopHint();
}

/// registers a callback invoked on every loop-thread wake, i.e. whenever
/// notify() runs. Safe to call before or after start(): guarded by the same
/// pendingMx addTimer uses, since the loop thread reads wakeHandlers under
/// that lock too (see wakeCallback).
pub fn addWakeHandler(self: *TimerLoop, ctx: *anyopaque, cb: *const fn (ctx: *anyopaque, loop: *xev.Loop) void) !void {
    spinLock(&self.pendingMx);
    defer self.pendingMx.unlock();
    try self.wakeHandlers.append(self.alloc, .{ .ctx = ctx, .cb = cb });
}

/// wakes the loop thread; safe to call from any thread. Combined with a
/// registered wake handler, lets owners marshal work onto the loop thread
/// without a second xev.Async.
pub fn notify(self: *TimerLoop) void {
    self.wake.notify() catch |err| {
        Logger.log(.err, "TimerLoop: failed to notify wake async", .{ .err = err });
    };
}

fn wakeCallback(
    ud: ?*TimerLoop,
    loop: *xev.Loop,
    _: *xev.Completion,
    r: xev.Async.WaitError!void,
) xev.CallbackAction {
    _ = r catch {};
    const self = ud.?;

    if (self.stopping.load(.acquire)) {
        loop.stop();
        return .disarm;
    }

    var pending: std.ArrayList(*Entry) = blk: {
        spinLock(&self.pendingMx);
        defer self.pendingMx.unlock();
        const p = self.pendingEntries;
        self.pendingEntries = .empty;
        break :blk p;
    };
    defer pending.deinit(self.alloc);

    for (pending.items) |entry| {
        self.entries.append(self.alloc, entry) catch |err| {
            Logger.log(.err, "TimerLoop: failed to register pending timer", .{ .err = err });
            continue;
        };
        entry.xevTimer.run(loop, &entry.completion, entry.intervalMs, Entry, entry, Entry.callback);
    }

    {
        spinLock(&self.pendingMx);
        defer self.pendingMx.unlock();
        for (self.wakeHandlers.items) |h| h.cb(h.ctx, loop);
    }

    return .rearm;
}

/// spawns the thread driving the registered timers. Safe to call more than
/// once (e.g. from several recorders sharing the same loop): a no-op if
/// already started.
pub fn start(self: *TimerLoop) !void {
    if (self.thread != null) return;

    self.wake.wait(&self.loop, &self.wakeCompletion, TimerLoop, self, wakeCallback);
    for (self.entries.items) |entry| {
        entry.xevTimer.run(&self.loop, &entry.completion, entry.intervalMs, Entry, entry, Entry.callback);
    }

    self.thread = try std.Thread.spawn(.{}, run, .{self});
}

fn run(self: *TimerLoop) void {
    self.loop.run(.until_done) catch |err| {
        Logger.log(.err, "TimerLoop: loop.run failed", .{ .err = err });
    };
}

/// requests the loop to stop; does not block, call join() to wait for the
/// thread to exit.
pub fn stop(self: *TimerLoop) void {
    self.stopping.store(true, .release);
    self.notify();
}

pub fn join(self: *TimerLoop) void {
    if (self.thread) |t| {
        t.join();
        self.thread = null;
    }
}

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
