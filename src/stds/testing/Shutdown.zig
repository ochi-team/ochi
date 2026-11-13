const std = @import("std");
const Allocator = std.mem.Allocator;
const Io = std.Io;
const testing = std.testing;
const xev = @import("xev");
const TimerLoop = @import("../xev/TimerLoop.zig");

/// Occupies the fixture's single merge worker so every recorder task remains queued.
pub fn expectQueuedWorkDrained(recorder: anytype, count: usize) !void {
    const io = testing.io;
    const alloc = testing.allocator;
    const Blocker = struct {
        task: xev.ThreadPool.Task = .{ .callback = run },
        entered: Io.Event = .unset,
        release: Io.Event = .unset,
        finished: Io.Event = .unset,

        fn run(task: *xev.ThreadPool.Task) void {
            const self: *@This() = @fieldParentPtr("task", task);
            self.entered.set(testing.io);
            self.release.waitUncancelable(testing.io);
            self.finished.set(testing.io);
        }
    };
    var blocker: Blocker = .{};
    recorder.mergePool.schedule(.from(&blocker.task));
    blocker.entered.waitUncancelable(io);
    defer {
        blocker.release.set(io);
        blocker.finished.waitUncancelable(io);
        recorder.jobs.wait(io);
    }
    for (0..count) |_| try recorder.startDiskTablesMerge(io, alloc);
    recorder.halt(io);
    try testing.expectEqual(count, recorder.jobs.pending);
    blocker.release.set(io);
    recorder.drain(io);
    try testing.expectEqual(0, recorder.jobs.pending);
}

/// Pauses merge-task allocation after its stop check, before it can be scheduled.
/// Wraps the supplied allocator so allocation-failure sweeps preserve the admission-race barrier.
pub fn expectSubmissionReserved(recorder: anytype, alloc: Allocator) !void {
    const io = testing.io;
    var paused: PausedAllocator = .{ .child = alloc, .io = io, .failAllocation = false };
    const Submit = struct {
        fn run(owner: @TypeOf(recorder), allocator: Allocator) anyerror!void {
            try owner.startDiskTablesMerge(testing.io, allocator);
        }
    };
    var submission = try io.concurrent(Submit.run, .{ recorder, paused.allocator() });
    defer {
        paused.release.set(io);
        submission.await(io) catch {};
        recorder.jobs.wait(io);
    }
    paused.entered.waitUncancelable(io);
    recorder.halt(io);
    try testing.expectEqual(1, recorder.jobs.pending);
    try testing.expect(!recorder.jobs.add(io));
    paused.release.set(io);
    const result = submission.await(io);
    recorder.jobs.wait(io);
    try testing.expectEqual(0, recorder.jobs.pending);
    try result;
}

/// Pauses an executing merge, then closes its admission/cancellation event.
pub fn expectRunningMergeStops(recorder: anytype, skipAllocations: usize) !void {
    const io = testing.io;
    const alloc = testing.allocator;
    var paused: PausedAllocator = .{
        .child = alloc,
        .io = io,
        .failAllocation = false,
        .skipAllocations = skipAllocations,
    };
    try recorder.startMemTablesMerge(io, paused.allocator());
    defer {
        paused.release.set(io);
        recorder.jobs.wait(io);
    }
    paused.entered.waitUncancelable(io);
    recorder.halt(io);
    try testing.expect(recorder.jobs.isClosed(io));
    try testing.expectEqual(1, recorder.jobs.pending);
    paused.release.set(io);
    recorder.jobs.wait(io);
    try testing.expectEqual(0, recorder.jobs.pending);
}

/// pauses timer-arm queue allocation after retaining the table but before publishing the arm.
pub fn expectTimerPublicationReserved(recorder: anytype, comptime publish: anytype, args: std.meta.ArgsTuple(@TypeOf(publish)), failAllocation: bool) !void {
    const io = testing.io;
    const alloc = testing.allocator;
    var paused: PausedAllocator = .{ .child = alloc, .io = io, .failAllocation = failAllocation };
    const original = recorder.taskCtx.alloc;
    recorder.taskCtx.alloc = paused.allocator();
    defer recorder.taskCtx.alloc = original;
    var publication = try io.concurrent(publish, args);
    defer {
        paused.release.set(io);
        publication.await(io) catch {};
        recorder.drain(io);
    }
    paused.entered.waitUncancelable(io);
    const table = recorder.memTables.items[0];
    try testing.expectEqual(2, table.refCounter.load(.acquire));
    const wake = recorder.taskRegistrations()[0].wake;
    try recorder.timerLoop.addWakeHandler(wake.ctx, wake.cb);
    const Drain = struct {
        fn run(owner: @TypeOf(recorder)) void {
            owner.drain(testing.io);
        }
    };
    var draining = try io.concurrent(Drain.run, .{recorder});
    defer {
        paused.release.set(io);
        draining.await(io);
    }

    // the paused publisher is the only job, and no other task waits on this countdown
    while (recorder.jobs.changed.state.load(.acquire).waiters == 0) std.atomic.spinLoopHint();

    {
        recorder.jobs.mx.lockUncancelable(io);
        defer recorder.jobs.mx.unlock(io);
        try testing.expect(recorder.jobs.isClosed(io));
        try testing.expectEqual(1, recorder.jobs.pending);

        TimerLoop.spinLock(&recorder.timerLoop.pendingMx);
        defer recorder.timerLoop.pendingMx.unlock();
        try testing.expectEqual(1, recorder.timerLoop.wakeHandlers.items.len);
        try testing.expectEqual(0, recorder.pendingTableArms.items.len);
        try testing.expectEqual(2, table.refCounter.load(.acquire));
    }
    paused.release.set(io);
    try publication.await(io);
    draining.await(io);
    try testing.expectEqual(0, recorder.jobs.pending);
    try testing.expectEqual(0, recorder.timerLoop.wakeHandlers.items.len);
    try testing.expectEqual(0, recorder.pendingTableArms.items.len);
    for (&recorder.tableTimerSlots) |slot| try testing.expectEqual(null, slot.table);
    // asserts the enqueued job eventually releases the table
    try testing.expectEqual(1, table.refCounter.load(.acquire));
}

pub const StopStage = enum { beforeCheck, beforeInsertion, queued };
pub const ArmKind = enum { table, shard };

/// Stops the driver at a controlled publication boundary without consuming the arm,
pub fn expectStoppedTimerPublication(recorder: anytype, comptime kind: ArmKind, comptime publish: anytype, args: std.meta.ArgsTuple(@TypeOf(publish)), stage: StopStage) !void {
    const io = testing.io;
    const alloc = testing.allocator;
    var paused: PausedAllocator = .{ .child = alloc, .io = io, .failAllocation = false };

    const original = recorder.taskCtx.alloc;
    recorder.taskCtx.alloc = paused.allocator();
    defer recorder.taskCtx.alloc = original;

    const loop = recorder.timerLoop;
    const DriverGate = struct {
        io: Io,
        entered: Io.Event = .unset,
        release: Io.Event = .unset,

        fn tick(ctx: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            self.entered.set(self.io);
            self.release.waitUncancelable(self.io);
        }
    };
    var gate: DriverGate = .{ .io = io };
    defer {
        loop.stop();
        gate.release.set(io);
        loop.join();
    }

    const wake = recorder.taskRegistrations()[0].wake;
    try loop.addWakeHandler(wake.ctx, wake.cb);
    try loop.addTimer(std.time.ns_per_ms, &gate, DriverGate.tick);
    try loop.start();
    gate.entered.waitUncancelable(io);

    switch (stage) {
        .beforeCheck => {
            loop.stop();
            gate.release.set(io);
            loop.join();
        },
        .beforeInsertion => {
            var publication = try io.concurrent(publish, args);
            defer {
                paused.release.set(io);
                publication.await(io) catch {};
            }
            paused.entered.waitUncancelable(io);
            try testing.expectEqual(1, recorder.jobs.pending);
            if (kind == .table) try testing.expectEqual(2, recorder.memTables.items[0].refCounter.load(.acquire));
            loop.stop();
            gate.release.set(io);
            loop.join();
            paused.release.set(io);
            try publication.await(io);
        },
        else => {
            paused.release.set(io);
            try @call(.auto, publish, args);
            loop.stop();
            gate.release.set(io);
            loop.join();
        },
    }

    // public ingestion also submits a merge after publishing the arm
    recorder.jobs.wait(io);
    const table = recorder.memTables.items[0];
    const queued: usize = if (stage == .beforeCheck) 0 else 1;

    if (kind == .table) {
        try testing.expectEqual(queued, recorder.pendingTableArms.items.len);
        try testing.expectEqual(1 + queued, table.refCounter.load(.acquire));
    } else {
        try testing.expectEqual(queued, recorder.pendingShardArms.items.len);
    }
    try testing.expectEqual(0, recorder.jobs.pending);
    try testing.expectError(error.Stopped, loop.start());

    loop.notify();
    recorder.drain(io);
    recorder.drain(io);
    try testing.expectEqual(0, recorder.pendingTableArms.items.len);

    if (kind == .shard) try testing.expectEqual(0, recorder.pendingShardArms.items.len);
    for (&recorder.tableTimerSlots) |slot| try testing.expectEqual(null, slot.table);
    // asserts all the timer references are released
    try testing.expectEqual(1, table.refCounter.load(.acquire));
}

const PausedAllocator = struct {
    child: Allocator,
    io: Io,
    failAllocation: bool,
    skipAllocations: usize = 0,
    entered: Io.Event = .unset,
    release: Io.Event = .unset,

    fn allocator(self: *PausedAllocator) Allocator {
        return .{ .ptr = self, .vtable = &.{ .alloc = allocate, .resize = resize, .remap = remap, .free = free } };
    }

    fn allocate(ctx: *anyopaque, len: usize, alignment: std.mem.Alignment, ra: usize) ?[*]u8 {
        const self: *PausedAllocator = @ptrCast(@alignCast(ctx));
        if (self.skipAllocations > 0) {
            self.skipAllocations -= 1;
            return self.child.rawAlloc(len, alignment, ra);
        }
        self.entered.set(self.io);
        self.release.waitUncancelable(self.io);
        if (self.failAllocation) return null;
        return self.child.rawAlloc(len, alignment, ra);
    }

    fn resize(ctx: *anyopaque, buf: []u8, alignment: std.mem.Alignment, len: usize, ra: usize) bool {
        const self: *PausedAllocator = @ptrCast(@alignCast(ctx));
        return self.child.rawResize(buf, alignment, len, ra);
    }

    fn remap(ctx: *anyopaque, buf: []u8, alignment: std.mem.Alignment, len: usize, ra: usize) ?[*]u8 {
        const self: *PausedAllocator = @ptrCast(@alignCast(ctx));
        return self.child.rawRemap(buf, alignment, len, ra);
    }

    fn free(ctx: *anyopaque, buf: []u8, alignment: std.mem.Alignment, ra: usize) void {
        const self: *PausedAllocator = @ptrCast(@alignCast(ctx));
        self.child.rawFree(buf, alignment, ra);
    }
};
