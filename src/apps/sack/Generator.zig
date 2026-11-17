// REFERENCE ONLY, NOT USED
const std = @import("std");
const Allocator = std.mem.Allocator;
const DefaultPrng = std.Random.DefaultPrng;

const Conf = @import("Conf.zig");

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

const tagsValues: []const []const []const u8 = &.{
    &.{ "debian", "ubuntu-14.04.6" },
};

const Generator = @This();
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

fn stealKey(size: usize, rand: *DefaultPrng, dst: *std.ArrayList([]const u8), src: *std.ArrayList([]const u8)) !void {
    for (0..size) |_| {
        const r = rand.next() % src.items.len - 1;

        const k = src.items[r];
        try dst.appendAssumeCapacity(k);
        _ = src.swapRemove(r);
    }
}
