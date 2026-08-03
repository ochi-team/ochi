const std = @import("std");
const Allocator = std.mem.Allocator;
const Io = std.Io;
const xev = @import("xev");

const Logger = @import("logging");

const Store = @import("Store.zig").Store;
const Accumulator = @import("Accumulator.zig");
const TimerLoop = @import("stds/xev/TimerLoop.zig");
const Ring = @import("stds/Ring.zig").Ring;
const Consts = @import("Consts.zig");

const Self = @This();

// flush proactively once a slot's buffer is this full, ahead of
// Accumulator's own (higher) internal safety threshold
const flushThresholdPercent = 80;

pub const Slot = struct {
    accumulator: Accumulator,
    mx: Io.Mutex = .init,

    // per-slot deadline timer, armed at the exact flushAtUs instant; slots
    // live for the pool's lifetime so the timer's userdata is always valid
    parent: *Self = undefined,
    xevTimer: xev.Timer,
    timerC: xev.Completion = .{},
    timerCancelC: xev.Completion = .{},
};

slots: []Slot,
ring: Ring(Slot),
io: Io,
alloc: Allocator,
timerLoop: *TimerLoop,

// arm requests come from arbitrary threads (acquire/afterAppend) and must be
// applied to timerLoop.loop only from its own thread, so they're queued here
// and drained via timerLoop's wake handler, mirroring DataRecorder's shard timers
pendingMx: std.atomic.Mutex = .unlocked,
pendingArms: std.ArrayList(*Slot) = .empty,

pub fn init(io: Io, alloc: Allocator, store: *Store, timerLoop: *TimerLoop, count: usize) !*Self {
    std.debug.assert(count > 0);

    const slots = try alloc.alloc(Slot, count);
    var inited: usize = 0;
    errdefer {
        for (slots[0..inited]) |*slot| slot.accumulator.deinit(alloc);
        alloc.free(slots);
    }

    for (0..slots.len) |i| {
        slots[i] = .{
            .accumulator = try Accumulator.init(alloc, store),
            .xevTimer = try xev.Timer.init(),
        };
        inited += 1;
    }

    const pool = try alloc.create(Self);
    pool.* = .{
        .slots = slots,
        .ring = Ring(Slot).init(slots),
        .io = io,
        .alloc = alloc,
        .timerLoop = timerLoop,
    };
    for (slots) |*slot| slot.parent = pool;

    try timerLoop.addWakeHandler(pool, wakeHandler);

    return pool;
}

pub fn deinit(self: *Self, alloc: Allocator) void {
    for (self.slots) |*slot| slot.accumulator.deinit(alloc);
    alloc.free(self.slots);
    self.pendingArms.deinit(alloc);
    alloc.destroy(self);
}

pub fn next(self: *Self) *Slot {
    return self.ring.next();
}

/// acquire locks the next slot in the ring and, if `bodySize` more bytes
/// wouldn't fit in the remaining buffer capacity, flushes it first.
pub fn acquire(self: *Self, io: Io, bodySize: usize) !*Slot {
    const slot = self.next();
    slot.mx.lockUncancelable(io);
    errdefer slot.mx.unlock(io);

    // TODO: implementing a compression this won't work,
    // we need to handle full capacity on append in order to flush a buffer on time
    const buf = &slot.accumulator.buffer;
    if (buf.end_index + bodySize > buf.buffer.len) {
        try slot.accumulator.flush(io, self.alloc);
    }

    return slot;
}

pub fn release(_: *Self, io: Io, slot: *Slot) void {
    slot.mx.unlock(io);
}

/// afterAppend flushes the slot once its buffer crosses flushThresholdPercent,
/// otherwise arms an idle flush deadline the first time the slot holds data.
pub fn afterAppend(self: *Self, io: Io, slot: *Slot) !void {
    const buf = &slot.accumulator.buffer;
    if (buf.end_index >= buf.buffer.len * flushThresholdPercent / 100) {
        try slot.accumulator.flush(io, self.alloc);
        return;
    }

    if (slot.accumulator.flushAtUs == null and buf.end_index > 0) {
        const nowUs: u64 = @intCast(Io.Timestamp.now(io, .real).toMicroseconds());
        slot.accumulator.flushAtUs = nowUs + Consts.dataFlushIntervalUs;
        self.requestArm(slot);
    }
}

/// flushAll force-flushes every slot; used by the test-only /flush endpoint
/// to make ingested data visible without waiting on threshold/idle triggers.
pub fn flushAll(self: *Self, io: Io) !void {
    for (self.slots) |*slot| {
        slot.mx.lockUncancelable(io);
        defer slot.mx.unlock(io);

        try slot.accumulator.flush(io, self.alloc);
    }
}

fn requestArm(self: *Self, slot: *Slot) void {
    TimerLoop.spinLock(&self.pendingMx);
    self.pendingArms.append(self.alloc, slot) catch |err| {
        self.pendingMx.unlock();
        Logger.log(.err, "AccumulatorPool: failed to queue timer arm", .{ .err = err });
        return;
    };
    self.pendingMx.unlock();

    self.timerLoop.notify();
}

fn wakeHandler(ctx: *anyopaque, loop: *xev.Loop) void {
    const self: *Self = @ptrCast(@alignCast(ctx));

    var arms: std.ArrayList(*Slot) = undefined;
    {
        TimerLoop.spinLock(&self.pendingMx);
        defer self.pendingMx.unlock();
        arms = self.pendingArms;
        self.pendingArms = .empty;
    }
    defer arms.deinit(self.alloc);

    for (arms.items) |slot| self.armTimer(loop, slot);
}

fn armTimer(self: *Self, loop: *xev.Loop, slot: *Slot) void {
    const flushAtUs = slot.accumulator.flushAtUs orelse return; // flushed early before the arm was drained
    const nowUs: u64 = @intCast(Io.Timestamp.now(self.io, .real).toMicroseconds());
    slot.xevTimer.reset(loop, &slot.timerC, &slot.timerCancelC, deltaMs(flushAtUs, nowUs), Slot, slot, timerCallback);
}

fn deltaMs(deadlineUs: u64, nowUs: u64) u64 {
    if (deadlineUs <= nowUs) return 0;
    return (deadlineUs - nowUs) / std.time.us_per_ms;
}

fn timerCallback(
    ud: ?*Slot,
    loop: *xev.Loop,
    c: *xev.Completion,
    r: xev.Timer.RunError!void,
) xev.CallbackAction {
    _ = r catch {};
    const slot = ud.?;
    const self = slot.parent;
    const io = self.io;

    if (!slot.mx.tryLock()) {
        // an append is in flight; retry shortly instead of dropping the deadline
        slot.xevTimer.reset(loop, c, &slot.timerCancelC, 1, Slot, slot, timerCallback);
        return .disarm;
    }
    defer slot.mx.unlock(io);

    // flushAtUs is only ever cleared (an early flush already handled it) or
    // left unchanged, never moved to a later deadline, so a stale fire after
    // an early flush is a safe no-op here.
    const flushAtUs = slot.accumulator.flushAtUs orelse return .disarm;
    const nowUs: u64 = @intCast(Io.Timestamp.now(io, .real).toMicroseconds());
    if (flushAtUs > nowUs) {
        slot.xevTimer.reset(loop, c, &slot.timerCancelC, deltaMs(flushAtUs, nowUs), Slot, slot, timerCallback);
        return .disarm;
    }

    slot.accumulator.flush(io, self.alloc) catch |err| {
        Logger.log(.err, "AccumulatorPool: failed to run scheduled flush", .{ .err = err });
    };
    return .disarm;
}
