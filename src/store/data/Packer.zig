const std = @import("std");
const Allocator = std.mem.Allocator;
const encoding = @import("encoding");
const tracy = @import("tracy");
const Encoder = encoding.Encoder;
const CompressionPool = @import("../compression/CompressionPool.zig");
const Io = std.Io;

const Width = struct {
    max: u64,
    size: usize,
    block: u8,
    blockInvariant: u8,
};
pub const uintBlockType8: u8 = 0;
pub const uintBlockType16: u8 = 1;
pub const uintBlockType32: u8 = 2;
pub const uintBlockType64: u8 = 3;
pub const uintBlockTypeInvariant8: u8 = 4;
pub const uintBlockTypeInvariant16: u8 = 5;
pub const uintBlockTypeInvariant32: u8 = 6;
pub const uintBlockTypeInvariant64: u8 = 7;

const widths = [_]Width{
    .{ .max = (1 << 8), .block = uintBlockType8, .blockInvariant = uintBlockTypeInvariant8, .size = @sizeOf(u8) },
    .{ .max = (1 << 16), .block = uintBlockType16, .blockInvariant = uintBlockTypeInvariant16, .size = @sizeOf(u16) },
    .{ .max = (1 << 32), .block = uintBlockType32, .blockInvariant = uintBlockTypeInvariant32, .size = @sizeOf(u32) },
    .{ .max = ~@as(u64, 0), .block = uintBlockType64, .blockInvariant = uintBlockTypeInvariant64, .size = @sizeOf(u64) },
};
fn pickWidth(maxLen: u64) Width {
    for (widths) |w| {
        if (maxLen < w.max) return w;
    }
    // TODO: audit all the panics usage and find if we can apply total functions well
    std.debug.panic("unexpected int width, given len={}", .{maxLen});
}

pub const compressionKindPlain: u8 = 0;
pub const compressionKindZstd: u8 = 1;

const Packer = @This();

allocator: Allocator,
lengths: std.ArrayList(u64),

pub fn init(allocator: Allocator) !Packer {
    return .{
        .allocator = allocator,
        // TODO: reuse a buffer from values encoder,
        // parsed holds same amount of data in case of u64 parsing
        .lengths = std.ArrayList(u64).empty,
    };
}

pub fn deinit(self: *Packer) void {
    self.lengths.deinit(self.allocator);
}

pub fn reset(self: *Packer) void {
    self.lengths.clearRetainingCapacity();
}

const PackBound = struct {
    lensBuf: []u8,
    lensBound: usize,
    valuesBuf: []u8,
    valuesBound: usize,

    pub fn deinit(self: *PackBound, alloc: Allocator) void {
        alloc.free(self.lensBuf);
        alloc.free(self.valuesBuf);
    }
};

pub fn packValuesInterBound(self: *Packer, values: []const []const u8) !PackBound {
    const z = tracy.Zone.begin(.{
        .src = @src(),
        .name = "packValuesInterBound",
    });
    defer z.end();

    defer self.lengths.clearRetainingCapacity();
    try self.lengths.ensureUnusedCapacity(self.allocator, values.len);
    var lenSum: usize = 0;
    for (values) |v| {
        self.lengths.appendAssumeCapacity(@intCast(v.len));
        lenSum += v.len;
    }

    var maxLen: u64 = 0;
    for (self.lengths.items) |n| {
        if (n > maxLen) maxLen = n;
    }

    var lensBuf: []u8 = &[_]u8{};
    errdefer {
        if (lensBuf.len > 0) self.allocator.free(lensBuf);
    }
    const areInvariants = (self.lengths.items.len >= 2) and areNumbersSame(self.lengths.items[0..]);
    const w = pickWidth(maxLen);
    if (areInvariants) {
        lensBuf = try self.allocator.alloc(u8, 1 + w.size);
        var enc = Encoder.init(lensBuf);
        enc.writeInt(u8, w.blockInvariant);
        enc.writeIntBytes(w.size, self.lengths.items[0]);
    } else {
        lensBuf = try self.allocator.alloc(u8, 1 + w.size * self.lengths.items.len);
        var enc = Encoder.init(lensBuf);
        _ = enc.writeInt(u8, w.block);
        for (self.lengths.items) |n| _ = enc.writeIntBytes(w.size, n);
    }

    // Optimize: if all values are the same, only pack the first one
    const valuesAreSame = (values.len >= 2) and areValuesSame(values);
    const valuesToPack = if (valuesAreSame) values[0..1] else values;
    const packSum = if (valuesAreSame) values[0].len else lenSum;

    const valuesBuf = try self.allocator.alloc(u8, packSum);
    errdefer self.allocator.free(valuesBuf);
    var bufOffset: usize = 0;
    for (valuesToPack) |value| {
        @memcpy(valuesBuf[bufOffset .. bufOffset + value.len], value);
        bufOffset += value.len;
    }

    // Calculate bounds for both encoded parts
    const lensBound = try packBytesBound(lensBuf.len);
    const valuesBound = try packBytesBound(valuesBuf.len);
    return .{
        .lensBuf = lensBuf,
        .lensBound = lensBound,
        .valuesBuf = valuesBuf,
        .valuesBound = valuesBound,
    };
}

pub fn packValues(pool: *CompressionPool, io: Io, dst: []u8, bound: PackBound) !usize {
    const z = tracy.Zone.begin(.{
        .src = @src(),
        .name = "packValues",
    });
    defer z.end();

    // Pack lengths and values into different slices of the same buffer
    const encodedLensSize = try packBytes(pool, io, dst[0..bound.lensBound], bound.lensBuf);
    const encodedValuesSize = try packBytes(pool, io, dst[encodedLensSize..], bound.valuesBuf);
    return encodedLensSize + encodedValuesSize;
}

fn packBytesBound(srcLen: usize) !usize {
    if (srcLen < 128) {
        // 1 compression kind, 1 len, len of the buf
        return 2 + srcLen;
    }
    const compressSize = try encoding.compressBound(srcLen);
    // 1 byte is a compression kind
    return 1 + Encoder.varIntBound(compressSize) + compressSize;
}

fn packBytes(pool: *CompressionPool, io: Io, dest: []u8, src: []u8) !usize {
    const z = tracy.Zone.begin(.{
        .src = @src(),
        .name = "packBytes",
    });
    z.text("src.len");
    z.value(src.len);
    defer z.end();

    if (src.len < 128) {
        // skip compression, up to 127 can be in a single byte to be compatible with leb128
        // 1 compression kind, 1 len, len of the buf
        var enc = Encoder.init(dest);
        enc.writeInt(u8, compressionKindPlain);
        enc.writeInt(u8, @intCast(src.len));
        enc.writeBytes(src);
        return enc.offset;
    }

    var enc = Encoder.init(dest);
    // 1 compression kind
    enc.writeInt(u8, compressionKindZstd);
    const compressSize = try encoding.compressBound(src.len);
    const compressedOffset = enc.offset + Encoder.varIntBound(compressSize);
    const compressedSize = try pool.compressAuto(io, enc.buf[compressedOffset..][0..compressSize], src);
    enc.writeVarInt(compressedSize);
    if (enc.offset != compressedOffset) {
        // the actual compressed content size is known only after compression,
        // so we have to move the written data if the variable size doesn't match the expectation
        std.mem.copyForwards(
            u8,
            enc.buf[enc.offset..][0..compressedSize],
            enc.buf[compressedOffset..][0..compressedSize],
        );
    }
    enc.offset += compressedSize;
    return enc.offset;
}

pub fn areNumbersSame(a: []const u64) bool {
    if (a.len == 0) return false;
    const v = a[0];
    for (a[1..]) |x| if (x != v) return false;
    return true;
}

fn areValuesSame(values: []const []const u8) bool {
    if (values.len == 0) return false;
    const first = values[0];
    for (values[1..]) |v| {
        if (!std.mem.eql(u8, v, first)) return false;
    }
    return true;
}
