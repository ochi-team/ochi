const std = @import("std");

const zint = @import("zint");

pub const EncodingType = enum(u8) {
    Undefined = 0,
    ZDeltapack = 1,
};

pub const EncodedTimestamps = struct {
    encodingType: EncodingType,
    offset: usize,
    buf: []u8,
};

const Self = @This();
const zType = zint.Zint(u64);

ctx: zint.Ctx,

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

// TODO: rename to encodeAlloc, implement 2 methods as replacement: bound and encode.
// the purpose is to being able to reuse a buffer across encodings
pub fn encode(self: *Self, allocator: std.mem.Allocator, tss: []u64) !EncodedTimestamps {
    const len: u32 = @intCast(tss.len);

    const compress_buf = try allocator.alloc(u8, zType.deltapack_compress_bound(len));
    const compressed_size = try zType.deltapack_compress(self.ctx, tss, compress_buf);

    return .{
        .encodingType = .ZDeltapack,
        .buf = compress_buf,
        .offset = compressed_size,
    };
}
pub fn decode(self: *Self, dst: []u64, src: []u8) !void {
    _ = try zType.deltapack_decompress(self.ctx, src, dst);
}

test "TimestampsEncoder" {
    const alloc = std.testing.allocator;
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

        const res = try enc.encode(alloc, @constCast(case.input));
        defer alloc.free(res.buf);
        try std.testing.expectEqual(EncodingType.ZDeltapack, res.encodingType);

        const dst = try alloc.alloc(u64, case.input.len);
        defer alloc.free(dst);
        try enc.decode(dst, res.buf[0..res.offset]);
        try std.testing.expectEqualSlices(u64, dst, case.input);
    }
}
