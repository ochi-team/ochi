const std = @import("std");
const Allocator = std.mem.Allocator;

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
            const it = self.map.keyIterator();
            for (it.next()) |key| {
                self.alloc.free(key);
            }
            self.map.deinit();
            self.alloc.destroy(self);
        }

        pub fn set(self: *Self, key: []const u8, value: V) !void {
            const k = try self.alloc.dupe(u8, key);
            try self.map.put(k, value);
        }

        pub fn contains(self: *Self, key: []const u8) bool {
            return self.map.contains(key);
        }
    };
}

