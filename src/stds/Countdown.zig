/// Countdown is a waitgroup with admission control,
/// so it restricts adding new tasks when it's closed
const std = @import("std");
const Io = std.Io;
const Countdown = @This();

// TODO: make a real waitgroup and use it here along the closed flag,
// then find all its usage and if needed just replace to a waitgroup
mx: Io.Mutex = .init,
changed: Io.Condition = .init,
closed: Io.Event = .unset,
pending: u32 = 0,

pub fn add(self: *Countdown, io: Io) bool {
    self.mx.lockUncancelable(io);
    defer self.mx.unlock(io);

    if (self.closed.isSet()) return false;
    self.pending += 1;
    return true;
}

pub fn close(self: *Countdown, io: Io) void {
    self.mx.lockUncancelable(io);
    defer self.mx.unlock(io);

    self.closed.set(io);
}

pub fn isClosed(self: *const Countdown, io: Io) bool {
    // keep io in order to keep the api more flexible in case of moving to another lock mechanic
    _ = io;
    return self.closed.isSet();
}

pub fn done(self: *Countdown, io: Io) void {
    self.mx.lockUncancelable(io);
    defer self.mx.unlock(io);

    const previous = self.pending;
    std.debug.assert(previous > 0);

    self.pending -= 1;
    if (self.pending == 0) self.changed.broadcast(io);
}

pub fn wait(self: *Countdown, io: Io) void {
    self.mx.lockUncancelable(io);
    defer self.mx.unlock(io);
    while (self.pending != 0) self.changed.waitUncancelable(io, &self.mx);
}

test "shutdown Countdown cancellation cannot complete a drain with reserved jobs" {
    const testing = std.testing;
    const Waiter = struct {
        gate: Countdown = .{},
        io: Io,
        cancelObserved: Io.Event = .unset,
        neverSet: Io.Event = .unset,
        result: ?anyerror = null,

        fn wait(self: *@This()) anyerror!void {
            try testing.expectError(error.Canceled, self.neverSet.wait(self.io));
            self.io.recancel();
            self.cancelObserved.set(self.io);
            self.gate.wait(self.io);
            try testing.expectEqual(0, self.gate.pending);
            try testing.expectError(error.Canceled, self.io.checkCancel());
        }

        fn cancel(self: *@This(), future: *Io.Future(anyerror!void)) void {
            future.cancel(self.io) catch |err| {
                self.result = err;
            };
        }

        fn run(count: usize) !void {
            const io = testing.io;
            var self: @This() = .{ .io = io };
            for (0..count) |_| try testing.expect(self.gate.add(io));
            self.gate.close(io);

            var future = try io.concurrent(@This().wait, .{&self});
            // cancelation of the future must be async,
            // otherwise it stucks on gate.wait
            var cancelFuture = try io.concurrent(cancel, .{ &self, &future });

            self.cancelObserved.waitUncancelable(io);
            try testing.expect(!self.gate.add(io));
            for (0..count) |_| self.gate.done(io);
            cancelFuture.await(io);

            try testing.expectEqual(null, self.result);
        }
    };
    try Waiter.run(1);
    try Waiter.run(3);
}

test "shutdown Countdown counts reserved work before scheduling" {
    const testing = std.testing;
    const f = struct {
        fn run(count: usize) !void {
            const io = testing.io;
            var gate: Countdown = .{};
            for (0..count) |_| try testing.expect(gate.add(io));

            gate.close(io);
            gate.close(io);

            try testing.expect(!gate.add(io));
            try testing.expectEqual(count, gate.pending);

            for (0..count) |_| gate.done(io);
            gate.wait(io);
            try testing.expectEqual(0, gate.pending);
        }
    }.run;
    try f(0);
    try f(1);
    try f(3);
}

// this rather confirms it's ok to listen broadcast time to time,
// but it's noop til it starts listening in .wait
test "Countdown condition only receives broadcasts made after listening" {
    const testing = std.testing;
    const Listener = struct {
        gate: Countdown = .{},
        ready: Io.Event = .unset,
        received: Io.Event = .unset,

        fn listen(self: *@This(), io: Io) !void {
            self.gate.mx.lockUncancelable(io);
            defer self.gate.mx.unlock(io);

            self.ready.set(io);
            try self.gate.changed.wait(io, &self.gate.mx);
            self.received.set(io);
        }

        fn run(earlyBroadcasts: usize) !void {
            const io = testing.io;
            var self: @This() = .{};

            for (0..earlyBroadcasts) |_| self.gate.changed.broadcast(io);

            var future = try io.concurrent(listen, .{ &self, io });
            defer future.cancel(io) catch {};
            self.ready.waitUncancelable(io);
            {
                // taking the mutex after ready ensures the listener has registered its wait
                // AND the condition starts listening, inside it unlocks it before yielding to futex
                self.gate.mx.lockUncancelable(io);
                defer self.gate.mx.unlock(io);

                try testing.expect(!self.received.isSet());
                self.gate.changed.broadcast(io);
            }
            try future.await(io);
            try testing.expect(self.received.isSet());
        }
    };
    try Listener.run(1);
    try Listener.run(3);
}
