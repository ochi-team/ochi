/// (box)sack defiens soak model testing.
/// generates datasets, ingests it and validates differnet queries against it
const std = @import("std");
const Allocator = std.mem.Allocator;
const DefaultPrng = std.Random.DefaultPrng;

const Conf = struct {
    maxKeys: u16,
    maxTags: u16,
    minKeys: u16,
    minTags: u16,
    len: u32,
};

const tagsSet: []const []const u8 = &.{
    "os",
    "namespace",
    "pod",
    "region",
    "provider",
    "source",
    "host",
    "version",
    "commit",
    "service",
};

fn stealKey(size: usize, rand: *DefaultPrng, dst: *std.ArrayList([]const u8), src: *std.ArrayList([]const u8)) !void {
    for (0..size) |_| {
        const r = rand.next() % src.items.len - 1;

        const k = src.items[r];
        try dst.appendAssumeCapacity(k);
        _ = src.swapRemove(r);
    }
}

const Generator = struct {
    conf: *const Conf,
    keys: std.ArrayList([]const u8),
    tags: std.ArrayList([]const u8),

    fn init(alloc: Allocator, conf: *const Conf, rand: *DefaultPrng) !Generator {
        var availableKeys: std.ArrayList([]const u8) = .initBuffer(tagsSet);

        var keys: std.ArrayList([]const u8) = try .initCapacity(alloc, conf.maxKeys);
        var tags: std.ArrayList([]const u8) = try .initCapacity(alloc, conf.maxTags);

        stealKey(conf.maxKeys, rand, &keys, &availableKeys);
        stealKey(conf.maxTags, rand, &tags, &availableKeys);

        return .{
            .conf = conf,
            .keys = keys,
            .tags = tags,
        };
    }

    pub fn deinit(self: *Generator, alloc: Allocator) void {
        self.keys.deinit(alloc);
        self.tags.deinit(alloc);
    }

    pub fn make() void {}
};
