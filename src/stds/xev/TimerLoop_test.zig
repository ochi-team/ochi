const std = @import("std");

const TimerLoop = @import("TimerLoop.zig");
const xev = @import("xev");

const testing = std.testing;

test "shutdown TimerLoop driver exit closes admission even without an explicit stop request" {
    const alloc = testing.allocator;
    const loop = try TimerLoop.init(alloc);
    defer loop.deinit();
    const Owner = struct {
        fn wake(_: *anyopaque, driver: *xev.Loop) void {
            driver.stop();
        }
    };
    var ctx: u8 = 0;
    try loop.addWakeHandler(&ctx, Owner.wake);
    try loop.start();
    loop.notify();
    loop.join();
    try testing.expectError(error.Stopped, loop.addWakeHandler(&ctx, Owner.wake));
    try testing.expectError(error.Stopped, loop.start());
}

test "shutdown TimerLoop closes registration and cannot restart" {
    const f = struct {
        fn tick(_: *anyopaque) void {}
        fn wake(_: *anyopaque, _: *xev.Loop) void {}
        fn run(started: bool) !void {
            const alloc = testing.allocator;
            const loop = try TimerLoop.init(alloc);
            defer loop.deinit();
            if (started) try loop.start();
            loop.stop();
            loop.stop();
            loop.join();
            loop.join();
            var ctx: u8 = 0;
            try testing.expectError(error.Stopped, loop.addTimer(1, &ctx, tick));
            try testing.expectError(error.Stopped, loop.addWakeHandler(&ctx, wake));
            try testing.expectError(error.Stopped, loop.start());
            loop.notify();
        }
    }.run;
    try f(false);
    try f(true);
}

test "shutdown TimerLoop detaches wake handlers and embedded deadlines while other owners run" {
    const Owner = struct {
        io: std.Io,
        armed: std.Io.Event = .unset,
        timer: xev.Timer,
        completion: xev.Completion = .{},
        cancelCompletion: xev.Completion = .{},
        armedOnce: bool = false,

        fn wake(ctx: *anyopaque, loop: *xev.Loop) void {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            if (self.armedOnce) return;
            self.armedOnce = true;
            // Keep the deadline armed until detach cancels it, independent of machine speed.
            self.timer.run(loop, &self.completion, std.time.ms_per_hour, @This(), self, callback);
            self.armed.set(self.io);
        }

        fn callback(_: ?*@This(), _: *xev.Loop, _: *xev.Completion, result: xev.Timer.RunError!void) xev.CallbackAction {
            std.debug.assert(result == error.Canceled);
            return .disarm;
        }

        fn drain(ctx: *anyopaque, loop: *xev.Loop) bool {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            return TimerLoop.cancelTimer(loop, self.timer, &self.completion, &self.cancelCompletion);
        }

        fn run(startFirst: bool) !void {
            const alloc = testing.allocator;
            const io = testing.io;
            const loop = try TimerLoop.init(alloc);
            defer loop.deinit();
            const owner = try alloc.create(@This());
            owner.* = .{ .io = io, .timer = try xev.Timer.init() };
            if (startFirst) try loop.start();
            try loop.addWakeHandler(owner, wake);
            if (!startFirst) try loop.start();
            loop.notify();
            owner.armed.waitUncancelable(io);
            loop.detach(io, owner, owner, drain);
            try testing.expectEqualDeep(.{ xev.CompletionState.dead, xev.CompletionState.dead }, .{ owner.completion.state(), owner.cancelCompletion.state() });
            alloc.destroy(owner);

            const Survivor = struct {
                io: std.Io,
                called: std.Io.Event = .unset,
                fn wake(ctx: *anyopaque, _: *xev.Loop) void {
                    const self: *@This() = @ptrCast(@alignCast(ctx));
                    self.called.set(self.io);
                }
                fn drain(_: *anyopaque, _: *xev.Loop) bool {
                    return true;
                }
            };
            var survivor: Survivor = .{ .io = io };
            try loop.addWakeHandler(&survivor, Survivor.wake);
            loop.notify();
            survivor.called.waitUncancelable(io);
            loop.detach(io, &survivor, &survivor, Survivor.drain);
        }
    };
    try Owner.run(false);
    try Owner.run(true);
}

test "shutdown TimerLoop detach waits for an executing callback with live or stopping driver" {
    const Owner = struct {
        io: std.Io,
        entered: std.Io.Event = .unset,
        release: std.Io.Event = .unset,
        detached: std.Io.Event = .unset,

        fn tick(ctx: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            self.entered.set(self.io);
            self.release.waitUncancelable(self.io);
        }
        fn drain(_: *anyopaque, _: *xev.Loop) bool {
            return true;
        }
        fn detach(self: *@This(), loop: *TimerLoop) void {
            loop.detach(self.io, self, self, drain);
            self.detached.set(self.io);
        }

        fn run(stop: bool) !void {
            const alloc = testing.allocator;
            const io = testing.io;
            const loop = try TimerLoop.init(alloc);
            defer {
                loop.stop();
                loop.join();
                loop.deinit();
            }

            var owner: @This() = .{ .io = io };
            defer owner.release.set(io);

            try loop.addTimer(std.time.ns_per_ms, &owner, tick);
            try loop.start();

            owner.entered.waitUncancelable(io);
            var detaching = try io.concurrent(detach, .{ &owner, loop });
            defer {
                owner.release.set(io);
                detaching.await(io);
            }

            // queued detach proves the other task reached the barrier while tick is blocked
            while (true) {
                TimerLoop.spinLock(&loop.pendingMx);
                const queued = loop.detaching != null;
                loop.pendingMx.unlock();
                if (queued or owner.detached.isSet()) break;
                std.atomic.spinLoopHint();
            }

            try testing.expect(!owner.detached.isSet());
            if (stop) loop.stop();
            try testing.expect(!owner.detached.isSet());
            owner.release.set(io);
            detaching.await(io);
            try testing.expect(owner.detached.isSet());
        }
    };
    try Owner.run(false);
    try Owner.run(true);
}

test "TimerLoop arms admitted timers without allocating on the driver thread" {
    const Owner = struct {
        io: std.Io,
        entered: std.Io.Event = .unset,
        release: std.Io.Event = .unset,
        armed: std.Io.Event = .unset,

        fn block(ctx: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            self.entered.set(self.io);
            self.release.waitUncancelable(self.io);
        }
        fn tick(_: *anyopaque) void {}
        fn wake(ctx: *anyopaque, _: *xev.Loop) void {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            self.armed.set(self.io);
        }

        fn run(batch: bool, count: usize) !void {
            const alloc = testing.allocator;
            const io = testing.io;
            var failing = testing.FailingAllocator.init(alloc, .{});
            const loop = try TimerLoop.init(failing.allocator());
            defer loop.deinit();
            var owner: @This() = .{ .io = io };
            defer owner.release.set(io);
            try loop.addWakeHandler(&owner, wake);
            try loop.addTimer(std.time.ns_per_ms, &owner, block);
            try loop.start();
            owner.entered.waitUncancelable(io);
            {
                TimerLoop.spinLock(&loop.pendingMx);
                defer loop.pendingMx.unlock();
                // Force the next accepted timers to require growth of the armed-entry list.
                try loop.entries.shrinkAndFreePrecise(failing.allocator(), loop.entries.items.len);
            }

            if (batch) {
                var registrations: [32]TimerLoop.Registration = undefined;
                for (registrations[0..count]) |*registration| {
                    registration.* = .{ .timer = .{ .intervalNs = std.time.ns_per_hour, .ctx = &owner, .tick = tick } };
                }
                try loop.register(registrations[0..count]);
            } else {
                for (0..count) |_| try loop.addTimer(std.time.ns_per_hour, &owner, tick);
            }
            {
                TimerLoop.spinLock(&loop.pendingMx);
                defer loop.pendingMx.unlock();
                try testing.expect(loop.entries.capacity >= loop.entries.items.len + loop.pendingEntries.items.len);
            }
            const allocations = failing.alloc_index;
            failing.fail_index = allocations;
            owner.release.set(io);
            owner.armed.waitUncancelable(io);
            loop.stop();
            loop.join();
            failing.fail_index = std.math.maxInt(usize);

            const State = struct { timers: usize, pending: usize, allocations: usize, allocationFailed: bool };
            try testing.expectEqualDeep(State{
                .timers = count + 1,
                .pending = 0,
                .allocations = allocations,
                .allocationFailed = false,
            }, State{
                .timers = loop.entries.items.len,
                .pending = loop.pendingEntries.items.len,
                .allocations = failing.alloc_index,
                .allocationFailed = failing.has_induced_failure,
            });
        }
    };
    try Owner.run(false, 1);
    try Owner.run(false, 3);
    try Owner.run(false, 17);
    try Owner.run(true, 1);
    try Owner.run(true, 3);
    try Owner.run(true, 17);
}

test "shutdown TimerLoop registration batch is atomic on allocation failure" {
    const Owner = struct {
        fn tick(_: *anyopaque) void {}
        fn wake(_: *anyopaque, _: *xev.Loop) void {}
        fn drain(_: *anyopaque, _: *xev.Loop) bool {
            return true;
        }
        fn run(failOffset: usize) !void {
            const alloc = testing.allocator;
            const io = testing.io;
            var failing = testing.FailingAllocator.init(alloc, .{});
            const loop = try TimerLoop.init(failing.allocator());
            defer loop.deinit();
            try loop.start();
            var owner: u8 = 0;
            // Exceed the pool's initial 32 entries to exercise pool growth rollback too.
            var registrations: [257]TimerLoop.Registration = undefined;
            registrations[0] = .{ .wake = .{ .ctx = &owner, .cb = wake } };
            for (registrations[1..]) |*registration| {
                registration.* = .{ .timer = .{ .intervalNs = std.time.ns_per_hour, .ctx = &owner, .tick = tick } };
            }
            failing.fail_index = failing.alloc_index + failOffset;
            try testing.expectError(error.OutOfMemory, loop.register(&registrations));
            failing.fail_index = std.math.maxInt(usize);
            TimerLoop.spinLock(&loop.pendingMx);
            const State = struct { timers: usize, wakes: usize };
            const state: State = .{
                .timers = loop.entries.items.len + loop.pendingEntries.items.len,
                .wakes = loop.wakeHandlers.items.len,
            };
            loop.pendingMx.unlock();
            try testing.expectEqualDeep(State{ .timers = 0, .wakes = 0 }, state);
            try loop.register(&registrations);
            loop.detach(io, &owner, &owner, drain);
        }
    };
    try Owner.run(0);
    try Owner.run(1);
    try Owner.run(2);
    try Owner.run(3);
    try Owner.run(4);
}

test "shutdown TimerLoop reclaims detached periodic registrations" {
    const Owner = struct {
        fn tick(_: *anyopaque) void {}
        fn drain(_: *anyopaque, _: *xev.Loop) bool {
            return true;
        }
        fn run(started: bool) !void {
            const alloc = testing.allocator;
            const io = testing.io;
            const loop = try TimerLoop.init(alloc);
            defer loop.deinit();
            if (started) try loop.start();
            var owner: u8 = 0;
            for (0..4) |_| {
                // A long deadline proves detach reclaims registrations without waiting for expiry.
                try loop.addTimer(std.time.ns_per_hour, &owner, tick);
                loop.detach(io, &owner, &owner, drain);
                TimerLoop.spinLock(&loop.pendingMx);
                const remaining = loop.entries.items.len + loop.pendingEntries.items.len;
                loop.pendingMx.unlock();
                try testing.expectEqual(0, remaining);
            }
        }
    };
    try Owner.run(false);
    try Owner.run(true);
}

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
