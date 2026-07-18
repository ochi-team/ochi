const std = @import("std");
const Io = std.Io;

const zint = @import("zint");
const Locked = @import("../../stds/Locked.zig").Locked;
const Ring = @import("../../stds/Ring.zig").Ring;

pub const EncodingType = enum(u8) {
    // Unedfined means it's not initialized and not expected to be as a real value
    Undefined = 0,
    ZDeltapack = 1,
};

pub const EncodedTimestamps = struct {
    encodingType: EncodingType,
    offset: usize,
};

// TODO: benchmark against gorilla and deltas
const Self = @This();
// TODO: we should inline zint package, it must reduce the build size
// and give a leverate to make it more optimal
const zType = zint.Zint(u64);

ctx: zint.Ctx,

pub const TimestampsEncoderPool = struct {
    const LockedEncoder = Locked(Self);

    encoders: []LockedEncoder,
    ring: Ring(LockedEncoder),

    pub fn init(allocator: std.mem.Allocator, count: usize) !*TimestampsEncoderPool {
        std.debug.assert(count > 0);

        const encoders = try allocator.alloc(LockedEncoder, count);
        var inited: usize = 0;
        errdefer {
            for (encoders[0..inited]) |*encoder| {
                encoder.val.ctx.deinit(allocator);
            }
            allocator.free(encoders);
        }

        for (0..encoders.len) |i| {
            encoders[i] = .{
                .val = .{ .ctx = try zint.Ctx.init(allocator) },
            };
            inited += 1;
        }

        const pool = try allocator.create(TimestampsEncoderPool);
        pool.* = .{
            .encoders = encoders,
            .ring = Ring(LockedEncoder).init(encoders),
        };
        return pool;
    }

    pub fn deinit(self: *TimestampsEncoderPool, allocator: std.mem.Allocator) void {
        for (self.encoders) |*encoder| {
            encoder.val.ctx.deinit(allocator);
        }
        allocator.free(self.encoders);
        allocator.destroy(self);
    }

    pub fn next(self: *TimestampsEncoderPool) *LockedEncoder {
        return self.ring.next();
    }

    pub fn bound(_: *const TimestampsEncoderPool, len: u32) u32 {
        return zint.Zint(u64).deltapack_compress_bound(len);
    }

    pub fn encode(pool: *TimestampsEncoderPool, io: Io, dst: []u8, tss: []const u64) !EncodedTimestamps {
        const locked = pool.next();
        locked.mx.lockUncancelable(io);
        defer locked.mx.unlock(io);

        return locked.val.encode(dst, tss);
    }

    pub fn decode(pool: *TimestampsEncoderPool, io: Io, dst: []u64, src: []const u8) !void {
        const locked = pool.next();
        locked.mx.lockUncancelable(io);
        defer locked.mx.unlock(io);

        return locked.val.decode(dst, src);
    }
};

pub fn init(allocator: std.mem.Allocator) !*Self {
    const ctx = try zint.Ctx.init(allocator);
    errdefer ctx.deinit(allocator);
    const s = try allocator.create(Self);
    s.* = .{ .ctx = ctx };
    return s;
}
pub fn deinit(self: *Self, allocator: std.mem.Allocator) void {
    self.ctx.deinit(allocator);
    allocator.destroy(self);
}

pub fn encode(self: *Self, dst: []u8, tss: []const u64) !EncodedTimestamps {
    const compressedSize = try zType.deltapack_compress(self.ctx, tss, dst);

    return .{
        .encodingType = .ZDeltapack,
        .offset = compressedSize,
    };
}
pub fn decode(self: *Self, dst: []u64, src: []const u8) !void {
    _ = try zType.deltapack_decompress(self.ctx, src, dst);
}

const testing = std.testing;

test "TimestampsEncoder" {
    const alloc = testing.allocator;
    const Case = struct {
        input: []const u64,
    };
    const cases = &[_]Case{
        .{ .input = &[_]u64{ 1, 2, 3, 4 } },
        .{ .input = &[_]u64{} },
        .{ .input = &[_]u64{std.math.maxInt(u64)} },
        .{ .input = &[_]u64{ std.math.maxInt(u64), 0 } },
        .{ .input = &[_]u64{ 0, std.math.maxInt(u64) } },
    };

    for (cases) |case| {
        const enc = try Self.init(alloc);
        defer enc.deinit(alloc);

        var buf: [64]u8 = undefined;
        const res = try enc.encode(&buf, case.input);
        try testing.expectEqual(EncodingType.ZDeltapack, res.encodingType);

        var decoded: [8]u64 = undefined;
        try enc.decode(decoded[0..case.input.len], buf[0..res.offset]);
        try std.testing.expectEqualSlices(u64, case.input, decoded[0..case.input.len]);
    }
}
