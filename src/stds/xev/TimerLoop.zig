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
    cancelCompletion: xev.Completion = .{},
    intervalMs: u64,
    ctx: *anyopaque,
    tick: TickFn,
    detached: bool = false,

    fn callback(
        ud: ?*Entry,
        loop: *xev.Loop,
        c: *xev.Completion,
        r: xev.Timer.RunError!void,
    ) xev.CallbackAction {
        _ = r catch |err| {
            Logger.log(.err, "failed to run TimerLoop callback", .{ .err = err });
        };
        const entry = ud.?;

        if (!entry.detached and !entry.parent.stopping.load(.acquire)) {
            entry.tick(entry.ctx);
        }

        if (entry.detached or entry.parent.stopping.load(.acquire)) {
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
joinMx: std.atomic.Mutex = .unlocked,
pendingEntries: std.ArrayList(*Entry) = .empty,
// callbacks invoked on every loop-thread wake, for owners that need to marshal
// their own state onto the loop thread (xev.Loop/Completion registration is
// only safe there) without standing up a second xev.Async of their own.
wakeHandlers: std.ArrayList(WakeHandler) = .empty,
stopping: std.atomic.Value(bool) = .init(false),
pool: std.heap.MemoryPool(Entry),
detaching: ?*Detach = null,

const Detach = struct {
    ctx: *anyopaque,
    periodicCtx: *anyopaque,
    drain: *const fn (*anyopaque, *xev.Loop) bool,
    io: std.Io,
    done: std.Io.Event = .unset,
    next: ?*Detach = null,
};

/// Detaches an independently destroyed owner. Published owners must detach outside the loop thread.
/// drain cancels embedded completions and returns true only when all are dead.
/// The owner must close scheduling admission before calling this method.
/// Unpublished and fully detached owners are a no-op; embedded completions must
/// only be armed through the owner's published registrations.
pub fn detach(self: *TimerLoop, io: std.Io, ctx: *anyopaque, periodicCtx: *anyopaque, drain: *const fn (*anyopaque, *xev.Loop) bool) void {
    var request: Detach = .{ .ctx = ctx, .periodicCtx = periodicCtx, .drain = drain, .io = io };
    spinLock(&self.pendingMx);
    // Rollback may hold a lock needed by an unrelated callback, or run on the driver itself.
    // Only owners with published registrations or an outstanding detach need a barrier.
    if (!self.hasOwner(ctx, periodicCtx)) {
        self.pendingMx.unlock();
        return;
    }
    if (self.stopping.load(.acquire)) {
        self.pendingMx.unlock();
        self.join();
        return;
    }
    if (self.thread == null) {
        _ = self.detachRegistrations(ctx, periodicCtx);
        self.pendingMx.unlock();
        return;
    }
    request.next = self.detaching;
    self.detaching = &request;
    self.pendingMx.unlock();
    self.notify();
    request.done.waitUncancelable(io);
}

fn hasOwner(self: *TimerLoop, ctx: *anyopaque, periodicCtx: *anyopaque) bool {
    for (self.wakeHandlers.items) |handler| {
        if (handler.ctx == ctx) return true;
    }
    for (self.entries.items) |entry| {
        if (entry.ctx == ctx or entry.ctx == periodicCtx) return true;
    }
    for (self.pendingEntries.items) |entry| {
        if (entry.ctx == ctx or entry.ctx == periodicCtx) return true;
    }
    // Wake registrations are removed before embedded completion cancellation finishes.
    var request = self.detaching;
    while (request) |pending| : (request = pending.next) {
        if (pending.ctx == ctx) return true;
    }
    return false;
}

fn detachRegistrations(self: *TimerLoop, ctx: *anyopaque, periodicCtx: *anyopaque) bool {
    var i: usize = 0;
    while (i < self.wakeHandlers.items.len) {
        if (self.wakeHandlers.items[i].ctx == ctx) {
            _ = self.wakeHandlers.swapRemove(i);
        } else i += 1;
    }
    const armed = self.detachEntries(&self.entries, ctx, periodicCtx);
    const pending = self.detachEntries(&self.pendingEntries, ctx, periodicCtx);
    return armed and pending;
}

fn detachEntries(self: *TimerLoop, entries: *std.ArrayList(*Entry), ctx: *anyopaque, periodicCtx: *anyopaque) bool {
    var drained = true;
    var i: usize = 0;
    while (i < entries.items.len) {
        const entry = entries.items[i];
        if (entry.ctx != ctx and entry.ctx != periodicCtx) {
            i += 1;
            continue;
        }
        entry.detached = true;
        if (cancelTimer(&self.loop, entry.xevTimer, &entry.completion, &entry.cancelCompletion)) {
            _ = entries.swapRemove(i);
            self.pool.destroy(entry);
        } else {
            drained = false;
            i += 1;
        }
    }
    return drained;
}

/// Called only on the loop thread. Both completion objects must remain alive until true.
pub fn cancelTimer(loop: *xev.Loop, timer: xev.Timer, completion: *xev.Completion, cancelCompletion: *xev.Completion) bool {
    if (cancelCompletion.state() != .dead) return false;
    if (completion.state() == .dead) return true;
    timer.cancel(loop, completion, cancelCompletion, void, null, cancelCallback);
    return false;
}

fn cancelCallback(_: ?*void, _: *xev.Loop, _: *xev.Completion, result: xev.Timer.CancelError!void) xev.CallbackAction {
    _ = result catch {};
    return .disarm;
}

pub const WakeHandler = struct {
    ctx: *anyopaque,
    cb: *const fn (ctx: *anyopaque, loop: *xev.Loop) void,
};

pub const TimerRegistration = struct {
    intervalNs: u64,
    ctx: *anyopaque,
    tick: TickFn,
};

pub const Registration = union(enum) {
    timer: TimerRegistration,
    wake: WakeHandler,
};

/// Publishes a complete set of owner registrations, or leaves none published on failure.
/// No fallible owner initialization may follow publication: rollback could otherwise
/// wait for this loop while holding a lock needed by one of its callbacks.
pub fn register(self: *TimerLoop, registrations: []const Registration) !void {
    try self.start();
    spinLock(&self.pendingMx);
    defer self.pendingMx.unlock();
    if (self.stopping.load(.acquire)) return error.Stopped;

    var timerCount: usize = 0;
    var wakeCount: usize = 0;
    for (registrations) |registration| switch (registration) {
        .timer => timerCount += 1,
        .wake => wakeCount += 1,
    };
    // Reserve for both armed and queued entries before publishing; driver-side arming cannot fail.
    try self.entries.ensureUnusedCapacity(self.alloc, self.pendingEntries.items.len + timerCount);
    try self.pendingEntries.ensureUnusedCapacity(self.alloc, timerCount);
    try self.wakeHandlers.ensureUnusedCapacity(self.alloc, wakeCount);
    var prepared: std.ArrayList(*Entry) = .empty;
    defer prepared.deinit(self.alloc);
    try prepared.ensureTotalCapacity(self.alloc, timerCount);
    errdefer for (prepared.items) |entry| self.pool.destroy(entry);
    for (registrations) |registration| switch (registration) {
        .timer => |timer| {
            const entry = try self.pool.create(self.alloc);
            errdefer self.pool.destroy(entry);
            entry.* = .{
                .parent = self,
                .xevTimer = try xev.Timer.init(),
                .intervalMs = timer.intervalNs / std.time.ns_per_ms,
                .ctx = timer.ctx,
                .tick = timer.tick,
            };
            prepared.appendAssumeCapacity(entry);
        },
        .wake => {},
    };
    self.pendingEntries.appendSliceAssumeCapacity(prepared.items);
    for (registrations) |registration| switch (registration) {
        .timer => {},
        .wake => |wake| self.wakeHandlers.appendAssumeCapacity(wake),
    };
    self.notify();
}

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
    self.stop();
    self.join();
    self.loop.deinit();
    self.wake.deinit();
    self.entries.deinit(self.alloc);
    self.pendingEntries.deinit(self.alloc);
    self.wakeHandlers.deinit(self.alloc);
    self.pool.deinit(self.alloc);
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

        if (self.stopping.load(.acquire)) return error.Stopped;
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
    if (self.stopping.load(.acquire)) return error.Stopped;
    try self.wakeHandlers.append(self.alloc, .{ .ctx = ctx, .cb = cb });
}

/// wakes the loop thread; safe to call from any thread. Combined with a
/// registered wake handler, lets owners marshal work onto the loop thread
/// without a second xev.Async.
pub fn notify(self: *TimerLoop) void {
    if (self.stopping.load(.acquire)) return;

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
    _ = r catch |err| {
        Logger.log(.err, "failed to run wake callback", .{ .err = err });
    };
    const self = ud.?;

    if (self.stopping.load(.acquire)) {
        // Complete detach acknowledgements only after join; owners remain alive meanwhile.
        loop.stop();
        return .disarm;
    }

    {
        spinLock(&self.pendingMx);
        defer self.pendingMx.unlock();
        // Keep queued-to-armed transfer under the reservation lock so producers account for both lists.
        for (self.pendingEntries.items) |entry| {
            self.entries.appendAssumeCapacity(entry);
            entry.xevTimer.run(loop, &entry.completion, entry.intervalMs, Entry, entry, Entry.callback);
        }
        self.pendingEntries.clearRetainingCapacity();

        var link = &self.detaching;
        while (link.*) |request| {
            const periodicDrained = self.detachRegistrations(request.ctx, request.periodicCtx);
            const ownerDrained = request.drain(request.ctx, loop);
            if (periodicDrained and ownerDrained) {
                link.* = request.next;
                request.done.set(request.io);
            } else {
                link = &request.next;
            }
        }
        for (self.wakeHandlers.items) |h| h.cb(h.ctx, loop);
        if (self.detaching != null) self.notify();
    }

    return .rearm;
}

/// spawns the thread driving the registered timers. Safe to call more than
/// once (e.g. from several recorders sharing the same loop): a no-op if
/// already started.
pub fn start(self: *TimerLoop) !void {
    spinLock(&self.pendingMx);
    defer self.pendingMx.unlock();
    if (self.stopping.load(.acquire)) return error.Stopped;
    if (self.thread != null) return;
    errdefer self.stopping.store(true, .release);

    self.wake.wait(&self.loop, &self.wakeCompletion, TimerLoop, self, wakeCallback);
    for (self.entries.items) |entry| {
        entry.xevTimer.run(&self.loop, &entry.completion, entry.intervalMs, Entry, entry, Entry.callback);
    }

    self.thread = try std.Thread.spawn(.{}, run, .{self});
}

fn run(self: *TimerLoop) void {
    defer {
        spinLock(&self.pendingMx);
        defer self.pendingMx.unlock();
        // Driver failure is terminal too; no callback can enter after run returns.
        self.stopping.store(true, .release);
        while (self.detaching) |request| {
            self.detaching = request.next;
            request.done.set(request.io);
        }
    }
    self.loop.run(.until_done) catch |err| {
        Logger.log(.err, "TimerLoop: loop.run failed", .{ .err = err });
    };
}

/// requests the loop to stop; does not block, call join() to wait for the
/// thread to exit.
pub fn stop(self: *TimerLoop) void {
    spinLock(&self.pendingMx);
    const wasStopping = self.stopping.swap(true, .acq_rel);
    self.pendingMx.unlock();
    if (!wasStopping) self.wake.notify() catch {};
}

pub fn join(self: *TimerLoop) void {
    // may detach concurrently during shutdown,
    // so it never holds pendingMx while joining
    spinLock(&self.joinMx);
    defer self.joinMx.unlock();
    spinLock(&self.pendingMx);
    const thread = self.thread;
    self.pendingMx.unlock();
    if (thread) |t| t.join();
    spinLock(&self.pendingMx);
    self.thread = null;
    defer self.pendingMx.unlock();
    while (self.detaching) |request| {
        self.detaching = request.next;
        request.done.set(request.io);
    }
}
