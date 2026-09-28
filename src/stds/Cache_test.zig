const std = @import("std");
const builtin = @import("builtin");
const Allocator = std.mem.Allocator;
const Thread = std.Thread;

const Cache = @import("Cache.zig").Cache;

const testing = std.testing;

test "StreamCache handles concurrent set and contains" {
    if (builtin.single_threaded) return error.SkipZigTest;
    const io = testing.io;

    const Worker = struct {
        fn run(cache: *Cache(void), workerId: usize) !void {
            var keyBuf: [64]u8 = undefined;

            var i: usize = 0;
            while (i < 1000) : (i += 1) {
                const key = try std.fmt.bufPrint(&keyBuf, "tenant-42-stream-{d}-worker-{d}", .{ i % 64, workerId % 2 });

                try cache.put(io, key, {});
                _ = cache.contains(io, key);
            }
        }
    };

    const cache = try Cache(void).init(testing.io, testing.allocator, .{ .meter = .{ .name = "" } });
    defer cache.deinit();

    var threads: [4]Thread = undefined;
    for (0..threads.len) |i| {
        threads[i] = try Thread.spawn(.{}, Worker.run, .{ cache, i });
    }
    for (threads) |t| {
        t.join();
    }

    try testing.expect(cache.contains(io, "tenant-42-stream-1-worker-1"));
}

test "Cache.set keeps first non-void value on duplicate insert" {
    const Value = struct {
        val: u8,

        fn init(alloc: Allocator, val: u8) !*@This() {
            const self = try alloc.create(@This());
            self.* = .{ .val = val };
            return self;
        }

        pub fn deinit(self: *@This(), alloc: Allocator) void {
            alloc.destroy(self);
        }
    };

    const alloc = testing.allocator;
    const io = testing.io;

    const ValueCache = Cache(*Value);
    const cache = try ValueCache.init(io, alloc, .{ .meter = .{ .name = "" } });
    defer cache.deinit();

    const first = try Value.init(alloc, 1);
    _ = try cache.put(io, "same-key", first);

    const second = try Value.init(alloc, 2);
    _ = try cache.put(io, "same-key", second);

    const storedVal = cache.get(io, "same-key").?;
    try testing.expect(cache.contains(io, "same-key"));
    try testing.expectEqual(first, storedVal);
    try testing.expectEqual(1, storedVal.val);
}

test "Cache.getOrElse creates non-void value only on miss" {
    const Value = struct {
        val: u8,

        fn init(alloc: Allocator, val: u8) !*@This() {
            const self = try alloc.create(@This());
            self.* = .{ .val = val };
            return self;
        }

        pub fn deinit(self: *@This(), alloc: Allocator) void {
            alloc.destroy(self);
        }
    };

    const CreateCtx = struct {
        alloc: Allocator,
        val: u8,
        calls: *usize,

        fn run(ctx: @This()) !*Value {
            ctx.calls.* += 1;
            return Value.init(ctx.alloc, ctx.val);
        }
    };

    const alloc = testing.allocator;
    const io = testing.io;

    const ValueCache = Cache(*Value);
    const cache = try ValueCache.init(io, alloc, .{ .meter = .{ .name = "" } });
    defer cache.deinit();

    var calls: usize = 0;
    const first = try cache.getOrElsePinned(io, "same-key", CreateCtx{
        .alloc = alloc,
        .val = 1,
        .calls = &calls,
    }, CreateCtx.run);
    defer first.pinned.release();

    // the vaue already there, so val 2 is ignored
    const second = try cache.getOrElsePinned(io, "same-key", CreateCtx{
        .alloc = alloc,
        .val = 2,
        .calls = &calls,
    }, CreateCtx.run);
    defer second.pinned.release();

    try testing.expectEqualDeep(ValueCache.GetOrElsePinnedRes{ .pinned = first.pinned, .elseHit = true }, first);
    try testing.expectEqualDeep(ValueCache.GetOrElsePinnedRes{ .pinned = first.pinned, .elseHit = false }, second);
    try testing.expectEqual(1, calls);
    try testing.expectEqual(1, second.pinned.value().val);
}

test "Cache.clean evicts shadow entries and keeps recently used entries" {
    const alloc = testing.allocator;
    const io = testing.io;

    const C = struct {
        fn fakeElse(_: void) !void {}
    };

    const p1 = struct {
        fn promote(c: *Cache(void)) anyerror!bool {
            return c.contains(io, "a");
        }
    }.promote;
    const p2 = struct {
        fn promote(c: *Cache(void)) anyerror!bool {
            var res = try c.getOrElsePinned(io, "a", {}, C.fakeElse);
            // means the value was discovered
            res.pinned.release();
            return res.elseHit == false;
        }
    }.promote;
    const p3 = struct {
        fn promote(c: *Cache(void)) anyerror!bool {
            const res = c.get(io, "a");
            return res != null;
        }
    }.promote;
    const promoters = [_]*const fn (*Cache(void)) anyerror!bool{ p1, p2, p3 };

    for (promoters) |promote| {
        const cache = try Cache(void).init(io, alloc, .{ .meter = .{ .name = "" } });
        defer cache.deinit();

        try cache.put(io, "a", {});
        try cache.put(io, "b", {});

        // moves both to shadow list
        cache.clean();
        // promotes "a"
        try testing.expect(try promote(cache));

        cache.clean();
        // "a" persist because it was promoted from shadow to active
        try testing.expect(cache.contains(io, "a"));
        try testing.expect(!cache.contains(io, "b"));
    }
}

test "Cache pinned value survives eviction until released" {
    const Value = struct {
        val: u8,
        deinits: *usize,

        fn init(alloc: Allocator, val: u8, deinits: *usize) !*@This() {
            const self = try alloc.create(@This());
            self.* = .{ .val = val, .deinits = deinits };
            return self;
        }

        pub fn deinit(self: *@This(), alloc: Allocator) void {
            self.deinits.* += 1;
            alloc.destroy(self);
        }
    };

    const CreateCtx = struct {
        alloc: Allocator,
        deinits: *usize,

        fn run(ctx: @This()) !*Value {
            return Value.init(ctx.alloc, 1, ctx.deinits);
        }
    };

    const alloc = testing.allocator;
    const io = testing.io;

    const ValueCache = Cache(*Value);
    const cache = try ValueCache.init(io, alloc, .{ .meter = .{ .name = "" } });
    defer cache.deinit();

    var deinits: usize = 0;
    var pinned = (try cache.getOrElsePinned(io, "a", CreateCtx{
        .alloc = alloc,
        .deinits = &deinits,
    }, CreateCtx.run)).pinned;

    cache.clean();
    cache.clean();

    try testing.expect(!cache.contains(io, "a"));
    try testing.expectEqual(0, deinits);
    try testing.expectEqual(1, pinned.value().val);

    pinned.release();
    try testing.expectEqual(1, deinits);
}
