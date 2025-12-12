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

pub fn TimestampsEncoder(intType: type) type {
    return struct {
        const Self = @This();
        const zType = zint.Zint(intType);

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

        pub fn encode(self: *Self, allocator: std.mem.Allocator, tss: []u64) !EncodedTimestamps {
            if (tss.len > std.math.maxInt(u32)) {
                return error.InputTooLarge;
            }
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
    };
}
