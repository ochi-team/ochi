const std = @import("std");
const Allocator = std.mem.Allocator;
const Io = std.Io;
const Thread = std.Thread;
const builtin = @import("builtin");
const Mutex = std.Io.Mutex;

pub const StreamCache = Cache(void);

// TODO: it's complete fake,
// we must implemented LRU or something
pub fn Cache(comptime V: type) type {
    return struct {
        const Self = @This();
        // cache data itself
        map: std.StringHashMap(V),
        // allocator for keys ownership
        alloc: Allocator,
        mx: Mutex = .init,

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
            var it = self.map.keyIterator();
            while (it.next()) |key| {
                self.alloc.free(key.*);
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
            }
            gop.value_ptr.* = value;
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
        fn run(cache: *StreamCache, workerId: usize) !void {
            var keyBuf: [64]u8 = undefined;

            var i: usize = 0;
            while (i < 1000) : (i += 1) {
                const key = try std.fmt.bufPrint(&keyBuf, "tenant-42-stream-{d}-worker-{d}", .{ i % 64, workerId % 2 });

                try cache.set(io, key, {});
                _ = cache.contains(io, key);
            }
        }
    };

    const cache = try StreamCache.init(testing.allocator);
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
