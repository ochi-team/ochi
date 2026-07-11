const std = @import("std");
const Io = std.Io;

const encoding = @import("encoding");
const Locked = @import("../stds/Locked.zig").Locked;
const Ring = @import("../stds/Ring.zig").Ring;

const Self = @This();

const Context = struct {
    cctx: encoding.CCtx,
    dctx: encoding.DCtx,
};

const LockedContext = Locked(Context);

contexts: []LockedContext,
ring: Ring(LockedContext),

pub fn init(allocator: std.mem.Allocator, count: usize) !*Self {
    std.debug.assert(count > 0);

    const contexts = try allocator.alloc(LockedContext, count);
    var inited: usize = 0;
    errdefer {
        for (contexts[0..inited]) |*ctx| {
            encoding.freeCCtx(ctx.val.cctx);
            encoding.freeDCtx(ctx.val.dctx);
        }
        allocator.free(contexts);
    }

    for (0..contexts.len) |i| {
        contexts[i] = .{
            .val = .{
                .cctx = try encoding.createCCtx(),
                .dctx = try encoding.createDCtx(),
            },
        };
        inited += 1;
    }

    const pool = try allocator.create(Self);
    pool.* = .{
        .contexts = contexts,
        .ring = Ring(LockedContext).init(contexts),
    };
    return pool;
}

pub fn deinit(self: *Self, allocator: std.mem.Allocator) void {
    for (self.contexts) |*ctx| {
        encoding.freeCCtx(ctx.val.cctx);
        encoding.freeDCtx(ctx.val.dctx);
    }
    allocator.free(self.contexts);
    allocator.destroy(self);
}

pub fn next(self: *Self) *LockedContext {
    return self.ring.next();
}

pub fn compressAuto(self: *Self, io: Io, dst: []u8, src: []const u8) !usize {
    const locked = self.next();
    locked.mx.lockUncancelable(io);
    defer locked.mx.unlock(io);

    return encoding.compressAuto(locked.val.cctx, dst, src);
}

pub fn decompress(self: *Self, io: Io, dst: []u8, src: []const u8) !usize {
    const locked = self.next();
    locked.mx.lockUncancelable(io);
    defer locked.mx.unlock(io);

    return encoding.decompress(locked.val.dctx, dst, src);
}
