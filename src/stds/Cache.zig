const std = @import("std");
const Allocator = std.mem.Allocator;
const Io = std.Io;
const Thread = std.Thread;
const builtin = @import("builtin");
const Mutex = std.Io.Mutex;

// TODO: make it support comptime meter misses/total
// TODO: redesign the cache, it feels Node is too large to hold especially for small key
// as 64 bytes it's doubling the size
// TODO: try a simple TTL cache if it consumes much less memory and gives enough miss/total ratio
// TODO: define several comptime buckets up to cpus count to reduce lock contention
// TODO: make a mechanic to clean on closing a table/partition
pub fn Cache(comptime V: type) type {
    return struct {
        const Self = @This();

        const ListKind = enum { active, shadow };

        const Node = struct {
            key: []const u8,
            value: V,
            refCounter: std.atomic.Value(u32) = .init(1),
            list: ListKind,
            prev: ?*@This() = null,
            next: ?*@This() = null,

            fn retain(self: *@This()) void {
                _ = self.refCounter.fetchAdd(1, .monotonic);
            }

            fn release(self: *@This(), alloc: Allocator) void {
                const prev = self.refCounter.fetchSub(1, .acq_rel);
                std.debug.assert(prev > 0);

                if (prev != 1) return;

                if (V != void) self.value.deinit(alloc);
                alloc.destroy(self);
            }
        };

        pub const Pinned = struct {
            node: *Node,
            alloc: Allocator,

            pub fn release(self: *const Pinned) void {
                self.node.release(self.alloc);
            }

            pub fn value(self: *const Pinned) V {
                return self.node.value;
            }
        };

        map: std.StringHashMap(*Node),
        alloc: Allocator,
        mx: Mutex = .init,
        activeHead: ?*Node = null,
        activeTail: ?*Node = null,
        shadowHead: ?*Node = null,
        shadowTail: ?*Node = null,

        pub const GetOrElseRes = struct {
            value: V,
            elseHit: bool,
        };
        pub const GetOrElsePinnedRes = struct {
            pinned: Pinned,
            elseHit: bool,
        };
        pub const Entry = struct {
            key: []const u8,
            value: V,
        };

        pub fn init(alloc: Allocator) !*Self {
            const map = std.StringHashMap(*Node).init(alloc);
            const c = try alloc.create(Self);
            c.* = .{
                .map = map,
                .alloc = alloc,
            };
            return c;
        }

        pub fn deinit(self: *Self) void {
            var it = self.map.iterator();
            while (it.next()) |e| {
                const node = e.value_ptr.*;
                self.alloc.free(node.key);
                node.release(self.alloc);
            }
            self.map.deinit();
            self.alloc.destroy(self);
        }

        pub fn put(self: *Self, io: Io, key: []const u8, value: V) !V {
            self.mx.lockUncancelable(io);
            defer self.mx.unlock(io);

            const k = try self.alloc.dupe(u8, key);
            errdefer self.alloc.free(k);

            const gop = try self.map.getOrPut(k);
            if (gop.found_existing) {
                self.alloc.free(k);
                if (V != void) value.deinit(self.alloc);
                self.promote(gop.value_ptr.*);
                return gop.value_ptr.*.value;
            }
            errdefer _ = self.map.remove(k);

            const node = n: {
                errdefer if (V != void) value.deinit(self.alloc);
                const node = try self.alloc.create(Node);
                node.* = .{
                    .key = k,
                    .value = value,
                    .list = .active,
                };
                break :n node;
            };
            errdefer node.release(self.alloc);

            gop.value_ptr.* = node;
            self.prependActive(node);
            return value;
        }

        pub fn getOrElsePinned(
            self: *Self,
            io: Io,
            key: []const u8,
            ctx: anytype,
            comptime action: *const fn (@TypeOf(ctx)) anyerror!V,
        ) !GetOrElsePinnedRes {
            self.mx.lockUncancelable(io);
            defer self.mx.unlock(io);

            if (self.map.get(key)) |node| {
                self.promote(node);
                node.retain();
                return .{
                    .pinned = .{ .node = node, .alloc = self.alloc },
                    .elseHit = false,
                };
            }

            const k = try self.alloc.dupe(u8, key);
            errdefer self.alloc.free(k);

            const value = try action(ctx);
            const node = n: {
                errdefer if (V != void) value.deinit(self.alloc);

                const node = try self.alloc.create(Node);
                node.* = .{
                    .key = k,
                    .value = value,
                    .list = .active,
                };
                break :n node;
            };
            errdefer node.release(self.alloc);

            try self.map.put(k, node);
            self.prependActive(node);

            node.retain();
            return .{
                .pinned = .{ .node = node, .alloc = self.alloc },
                .elseHit = true,
            };
        }

        pub fn contains(self: *Self, io: Io, key: []const u8) bool {
            self.mx.lockUncancelable(io);
            defer self.mx.unlock(io);

            if (self.map.get(key)) |node| {
                self.promote(node);
                return true;
            }
            return false;
        }

        pub fn get(self: *Self, io: Io, key: []const u8) ?V {
            self.mx.lockUncancelable(io);
            defer self.mx.unlock(io);

            if (self.map.get(key)) |node| {
                self.promote(node);
                return node.value;
            }
            return null;
        }

        pub fn clean(self: *Self, io: Io) void {
            self.mx.lockUncancelable(io);
            defer self.mx.unlock(io);

            while (self.shadowTail) |node| {
                self.evictNode(node);
            }

            self.shadowHead = self.activeHead;
            self.shadowTail = self.activeTail;
            self.activeHead = null;
            self.activeTail = null;

            var node = self.shadowHead;
            while (node) |n| : (node = n.next) {
                n.list = .shadow;
            }
        }

        fn evictNode(self: *Self, node: *Node) void {
            self.unlink(node);
            _ = self.map.remove(node.key);
            self.alloc.free(node.key);
            node.release(self.alloc);
        }

        fn promote(self: *Self, node: *Node) void {
            self.unlink(node);
            node.list = .active;
            self.prependActive(node);
        }

        fn prependActive(self: *Self, node: *Node) void {
            std.debug.assert(node.prev == null);
            std.debug.assert(node.next == null);

            node.list = .active;
            node.next = self.activeHead;
            if (self.activeHead) |head| {
                head.prev = node;
            } else {
                self.activeTail = node;
            }
            self.activeHead = node;
        }

        fn unlink(self: *Self, node: *Node) void {
            const head = switch (node.list) {
                .active => &self.activeHead,
                .shadow => &self.shadowHead,
            };
            const tail = switch (node.list) {
                .active => &self.activeTail,
                .shadow => &self.shadowTail,
            };

            if (node.prev) |prev| {
                prev.next = node.next;
            } else {
                head.* = node.next;
            }

            if (node.next) |next| {
                next.prev = node.prev;
            } else {
                tail.* = node.prev;
            }

            node.prev = null;
            node.next = null;
        }
    };
}

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

    const cache = try Cache(void).init(testing.allocator);
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

        fn deinit(self: *@This(), alloc: Allocator) void {
            alloc.destroy(self);
        }
    };

    const alloc = testing.allocator;
    const io = testing.io;

    const ValueCache = Cache(*Value);
    const cache = try ValueCache.init(alloc);
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

        fn deinit(self: *@This(), alloc: Allocator) void {
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
    const cache = try ValueCache.init(alloc);
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
        const cache = try Cache(void).init(alloc);
        defer cache.deinit();

        try cache.put(io, "a", {});
        try cache.put(io, "b", {});

        // moves both to shadow list
        cache.clean(io);
        // promotes "a"
        try testing.expect(try promote(cache));

        cache.clean(io);
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

        fn deinit(self: *@This(), alloc: Allocator) void {
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
    const cache = try ValueCache.init(alloc);
    defer cache.deinit();

    var deinits: usize = 0;
    var pinned = (try cache.getOrElsePinned(io, "a", CreateCtx{
        .alloc = alloc,
        .deinits = &deinits,
    }, CreateCtx.run)).pinned;

    cache.clean(io);
    cache.clean(io);

    try testing.expect(!cache.contains(io, "a"));
    try testing.expectEqual(0, deinits);
    try testing.expectEqual(1, pinned.value().val);

    pinned.release();
    try testing.expectEqual(1, deinits);
}
