const std = @import("std");
const Io = std.Io;
const xev = @import("xev");
const testing = std.testing;
const AccumulatorPool = @import("AccumulatorPool.zig");
const Store = @import("Store.zig");
const Runtime = @import("Runtime.zig");
const Layout = @import("Layout.zig");
const Conf = @import("Conf.zig");
const Field = @import("store/lines.zig").Field;

const WakeBarrier = struct {
    io: Io,
    entered: Io.Event = .unset,

    fn wake(ctx: *anyopaque, _: *xev.Loop) void {
        const self: *WakeBarrier = @ptrCast(@alignCast(ctx));
        self.entered.set(self.io);
    }

    fn drain(_: *anyopaque, _: *xev.Loop) bool {
        return true;
    }
};

test "shutdown accumulator pool detaches pending and armed slot deadlines" {
    const f = struct {
        fn run(started: bool, slots: usize) !void {
            const alloc = testing.allocator;
            const io = testing.io;
            var tmp = testing.tmpDir(.{});
            defer tmp.cleanup();
            const root = try tmp.dir.realPathFileAlloc(io, ".", alloc);
            defer alloc.free(root);
            const conf = Conf.getConf();
            const runtime = try Runtime.init(io, alloc, root, conf.app.maxCachePortion);
            defer runtime.deinit(alloc);
            var pathBuf: [std.fs.max_path_bytes]u8 = undefined;
            const layout = try Layout.make(io, root, &pathBuf);
            defer layout.partitionsDir.close(io);
            var store = try Store.init(io, alloc, &conf, runtime, layout);
            defer store.deinit(io, alloc);
            const pool = try AccumulatorPool.init(io, alloc, &store, store.timerLoop, slots);

            var barrier: WakeBarrier = .{ .io = io };
            defer store.timerLoop.detach(io, &barrier, &barrier, WakeBarrier.drain);
            {
                defer pool.deinit(alloc);
                for (0..slots) |_| {
                    const slot = try pool.acquire(io, 0);
                    defer pool.release(io, slot);
                    var tags = [_]Field{.{ .key = "service", .value = "shutdown" }};
                    try slot.accumulator.reinit(alloc, &tags, 0);
                    var fields = [_]Field{.{ .key = "message", .value = "buffered" }};
                    const nowNs: u64 = @intCast(Io.Timestamp.now(io, .real).nanoseconds);
                    try slot.accumulator.tryAppendLine(io, alloc, nowNs, &fields);
                    try pool.afterAppend(io, slot);
                }

                // Registered after the pool, so acknowledgement proves its arm queue was drained.
                try store.timerLoop.addWakeHandler(&barrier, WakeBarrier.wake);
                if (started) {
                    try store.start(io, alloc);
                    store.timerLoop.notify();
                    barrier.entered.waitUncancelable(io);
                }
                pool.halt(io);
                pool.halt(io);
                try testing.expectError(error.Stopped, pool.acquire(io, 0));
                try testing.expectError(error.Stopped, pool.flushAll(io));
                try pool.shutdown(io);
                for (pool.slots) |slot| {
                    try testing.expectEqual(0, slot.accumulator.lines.items.len);
                }
            }
            // Other owners must still be able to use the loop after pool destruction.
            barrier.entered.reset();
            try store.timerLoop.start();
            store.timerLoop.notify();
            barrier.entered.waitUncancelable(io);
        }
    }.run;
    try f(false, 1);
    try f(false, 3);
    try f(true, 1);
    try f(true, 3);
}
