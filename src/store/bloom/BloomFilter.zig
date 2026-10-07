const std = @import("std");

const tracy = @import("tracy");

const bloom = @import("bloom.zig");
const hashRounds = bloom.hashRounds;

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
