const std = @import("std");

const bloom = @import("bloom.zig");
const bucketsSize = bloom.bucketsSize;

const Bucket = bloom.Bucket;

const HashTokenizer = @import("HashTokenizer.zig");

pub const BloomFilter = @import("BloomFilter.zig");

const testing = std.testing;

// TODO: this test is mediocre, we must test a hit rate here >90% to confirm the hash is viable,
// testing readiability for the data is not very useful
test "BloomFilter" {
    const allocator = testing.allocator;
    const Case = struct {
        tokens: []const []const u8,
        expectedEncoded: ?[]const u8,
    };

    const thousandTokens = try allocator.alloc([]u8, 1000);
    defer allocator.free(thousandTokens);
    for (0..1000) |i| {
        thousandTokens[i] = try std.fmt.allocPrint(allocator, "{d}", .{i + 1000});
    }
    defer {
        for (0..1000) |i| {
            allocator.free(thousandTokens[i]);
        }
    }
    const cases = [_]Case{
        .{
            .tokens = &[_][]const u8{"foo"},
            .expectedEncoded = "\x00\x00\x00\x82\x40\x18\x00\x04",
        },
        .{
            .tokens = &[_][]const u8{ "foo", "bar", "baz" },
            .expectedEncoded = "\x00\x00\x81\xA3\x48\x5C\x10\x26",
        },
        .{
            .tokens = &[_][]const u8{ "foo", "bar", "baz", "foo" },
            .expectedEncoded = "\x00\x00\x81\xA3\x48\x5C\x10\x26",
        },
        .{
            .tokens = thousandTokens,
            .expectedEncoded = null,
        },
    };

    for (cases) |case| {
        // init
        var buckets: [bucketsSize]Bucket = undefined;
        var tokenizer = try HashTokenizer.init(allocator, &buckets);
        defer tokenizer.deinit(allocator);
        var hashes = try tokenizer.tokenizeValues(allocator, case.tokens);
        defer hashes.deinit(allocator);

        const buf = try allocator.alloc(u8, BloomFilter.boundHashes(hashes.items));
        defer allocator.free(buf);
        BloomFilter.writeBits(buf, hashes.items);

        if (case.expectedEncoded) |expected| {
            try testing.expectEqualStrings(expected, buf);
        }
    }
}

test "containsAll" {
    const f = struct {
        fn f(size: usize, written: []const []const u8, queried: []const []const u8, expected: bool) !void {
            const alloc = std.testing.allocator;
            var buf: [64]u8 = undefined;
            const dst = buf[0..size];

            var base: [4]u64 = undefined;
            for (written, 0..) |token, i| base[i] = std.hash.XxHash64.hash(0, token);
            BloomFilter.writeBits(dst, base[0..written.len]);

            var hashes: std.ArrayList(u64) = .empty;
            defer hashes.deinit(alloc);
            for (queried) |token| {
                const h = try bloom.tokenHashes(alloc, token);
                defer alloc.free(h);
                try hashes.appendSlice(alloc, h);
            }

            try std.testing.expectEqual(expected, BloomFilter.containsAll(dst, hashes.items));
        }
    }.f;

    try f(0, &.{}, &.{"a"}, true);
    try f(8, &.{}, &.{}, true);
    try f(8, &.{}, &.{"a"}, false);
    try f(64, &.{"foo"}, &.{"foo"}, true);
    try f(64, &.{ "foo", "bar" }, &.{ "bar", "foo" }, true);
    try f(64, &.{ "foo", "bar" }, &.{"bar"}, true);
    try f(64, &.{"foo"}, &.{ "foo", "missing" }, false);
}
