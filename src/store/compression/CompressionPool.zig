const std = @import("std");
const Io = std.Io;

const encoding = @import("encoding");
const Locked = @import("../../stds/Locked.zig").Locked;
const Ring = @import("../../stds/Ring.zig").Ring;

const Self = @This();

const LockedContext = Locked(encoding.StaticCCtx);

// TODO: there are 3 same equal objects implementations:
// compression pool, decompression pool, timestamps encoders
contexts: []LockedContext,
ring: Ring(LockedContext),

pub fn init(allocator: std.mem.Allocator, count: usize) !*Self {
    std.debug.assert(count > 0);

    const contexts = try allocator.alloc(LockedContext, count);
    var inited: usize = 0;
    errdefer {
        for (contexts[0..inited]) |*ctx| {
            allocator.free(ctx.val.workspace);
        }
        allocator.free(contexts);
    }

    for (0..contexts.len) |i| {
        contexts[i] = .{
            .val = try encoding.createStaticCCtx(allocator),
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
        allocator.free(ctx.val.workspace);
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

    return encoding.compressAuto(locked.val.ctx, dst, src);
}
