const std = @import("std");

const tracy = @import("tracy");

const bloom = @import("bloom.zig");
const bucketsSize = bloom.bucketsSize;
const hashRounds = bloom.hashRounds;

const Bucket = bloom.Bucket;

const HashTokenizer = @import("HashTokenizer.zig");

pub const BloomFilter = @This();
bits: []u64,

const bitsPerEntry = 16;

pub inline fn boundHashes(hashes: []const u64) usize {
    // +63 to have a gap rounding to upper value
    const len = (hashes.len * bitsPerEntry + 63) / 64;
    return @sizeOf(u64) * len;
}

pub fn deinit(self: *BloomFilter, allocator: std.mem.Allocator) void {
    allocator.free(self.bits);
}

pub fn writeBits(dst: []u8, src: []const u64) void {
    const z = tracy.Zone.begin(.{
        .src = @src(),
        .name = "writeBits",
    });
    defer z.end();

    @memset(dst, 0);
    var buf: [8]u8 align(@alignOf(u64)) = undefined;
    const p: *u64 = @ptrCast(&buf);

    // TODO: measure the hitmap of the maxBits,
    // if the value makes sense - vectorize the calculation,
    // or document why we can't
    const maxBits = dst.len << 3; // * 8
    for (src) |srcHash| {
        p.* = srcHash;

        inline for (0..hashRounds) |_| {
            const hash = std.hash.XxHash64.hash(0, &buf);
            p.* += 1;

            const idx = hash % maxBits;
            const iHash = idx / 64;
            const bitOrder: u6 = @intCast(idx % 64);
            setEncodedBit(dst, iHash, bitOrder);
        }
    }
}

fn setEncodedBit(dst: []u8, wordIndex: usize, bitOrder: u6) void {
    const byteIndex = wordIndex * @sizeOf(u64) + (7 - bitOrder / 8);
    const byteBit: u3 = @intCast(bitOrder % 8);
    dst[byteIndex] |= @as(u8, 1) << byteBit;
}

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
