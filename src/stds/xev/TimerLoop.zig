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
stopping: std.atomic.Value(bool) = .init(false),

pub fn init(alloc: Allocator) !*TimerLoop {
    const self = try alloc.create(TimerLoop);
    errdefer alloc.destroy(self);

    var loop = try xev.Loop.init(.{});
    errdefer loop.deinit();

    const wake = try xev.Async.init();

    self.* = .{
        .alloc = alloc,
        .loop = loop,
        .wake = wake,
    };
    return self;
}

pub fn deinit(self: *TimerLoop) void {
    for (self.entries.items) |entry| self.alloc.destroy(entry);
    self.entries.deinit(self.alloc);
    self.wake.deinit();
    self.loop.deinit();
    self.alloc.destroy(self);
}

/// registers a timer that fires `tick(ctx)` every `intervalNs`,
/// re-arming itself until the TimerLoop is stopped.
/// must be called before start().
pub fn addTimer(self: *TimerLoop, intervalNs: u64, ctx: *anyopaque, tick: TickFn) !void {
    const entry = try self.alloc.create(Entry);
    errdefer self.alloc.destroy(entry);

    entry.* = .{
        .parent = self,
        .xevTimer = try xev.Timer.init(),
        .intervalMs = intervalNs / std.time.ns_per_ms,
        .ctx = ctx,
        .tick = tick,
    };
    try self.entries.append(self.alloc, entry);
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
    return .rearm;
}

/// spawns the thread driving the registered timers. addTimer must not
/// be called after start().
pub fn start(self: *TimerLoop) !void {
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
    self.wake.notify() catch |err| {
        Logger.log(.err, "TimerLoop: failed to notify wake async", .{ .err = err });
    };
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
