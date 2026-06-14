const std = @import("std");
const Allocator = std.mem.Allocator;
const Io = std.Io;
const Thread = std.Thread;
const builtin = @import("builtin");
const Mutex = std.Io.Mutex;

// TODO: it's complete fake,
// we must implemented LRU or something
pub fn Cache(comptime V: type) type {
    return struct {
        const Self = @This();
        // cache data itself
        map: std.StringHashMap(V),
        alloc: Allocator,
        mx: Mutex = .init,

        pub const PutIfAbsentResult = struct {
            value: V,
            inserted: bool,
        };

        pub fn init(alloc: Allocator) !*Self {
            const map = std.StringHashMap(V).init(alloc);
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
                self.alloc.free(e.key_ptr.*);
                if (V != void) e.value_ptr.*.deinit(self.alloc);
            }
            self.map.deinit();
            self.alloc.destroy(self);
        }

        pub fn set(self: *Self, io: Io, key: []const u8, value: V) !void {
            self.mx.lockUncancelable(io);
            defer self.mx.unlock(io);

            const k = try self.alloc.dupe(u8, key);
            errdefer self.alloc.free(k);

            const gop = try self.map.getOrPut(k);
            if (gop.found_existing) {
                self.alloc.free(k);
                if (V != void) gop.value_ptr.*.deinit(self.alloc);
            }
            gop.value_ptr.* = value;
        }

        pub fn putIfAbsent(self: *Self, io: Io, key: []const u8, value: V) !PutIfAbsentResult {
            self.mx.lockUncancelable(io);
            defer self.mx.unlock(io);

            const k = try self.alloc.dupe(u8, key);
            errdefer self.alloc.free(k);

            const gop = try self.map.getOrPut(k);
            if (gop.found_existing) {
                self.alloc.free(k);
                return .{
                    .value = gop.value_ptr.*,
                    .inserted = false,
                };
            }

            gop.value_ptr.* = value;
            return .{
                .value = value,
                .inserted = true,
            };
        }

        pub fn contains(self: *Self, io: Io, key: []const u8) bool {
            self.mx.lockUncancelable(io);
            defer self.mx.unlock(io);

            return self.map.contains(key);
        }

        pub fn get(self: *Self, io: Io, key: []const u8) ?V {
            self.mx.lockUncancelable(io);
            defer self.mx.unlock(io);

            return self.map.get(key);
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

                try cache.set(io, key, {});
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

test "Cache.putIfAbsent keeps first non-void value on duplicate insert" {
    const Value = struct {
        buf: []u8,

        fn init(alloc: Allocator) !*@This() {
            const self = try alloc.create(@This());
            errdefer alloc.destroy(self);

            self.* = .{ .buf = try alloc.alloc(u8, 8) };
            return self;
        }

        fn deinit(self: *@This(), alloc: Allocator) void {
            alloc.free(self.buf);
            alloc.destroy(self);
        }
    };

    const alloc = testing.allocator;
    const io = testing.io;

    const ValueCache = Cache(*Value);
    const cache = try ValueCache.init(alloc);
    defer cache.deinit();

    const first = try Value.init(alloc);
    const firstResult = try cache.putIfAbsent(io, "same-key", first);
    try testing.expectEqualDeep(ValueCache.PutIfAbsentResult{ .value = first, .inserted = true }, firstResult);

    const second = try Value.init(alloc);
    const secondResult = try cache.putIfAbsent(io, "same-key", second);
    try testing.expectEqualDeep(ValueCache.PutIfAbsentResult{ .value = first, .inserted = false }, secondResult);
    second.deinit(alloc);

    try testing.expect(cache.contains(io, "same-key"));
}
