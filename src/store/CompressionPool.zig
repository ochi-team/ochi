const std = @import("std");
const builtin = @import("builtin");
const Io = std.Io;

const encoding = @import("encoding");
const Locked = @import("../stds/Locked.zig").Locked;
const Ring = @import("../stds/Ring.zig").Ring;

pub const CompressionPool = struct {
    const Self = @This();
    const LockedContext = Locked(encoding.CCtx);
    const LockedDecompressionContext = Locked(encoding.DCtx);

    contexts: []LockedContext,
    ring: Ring(LockedContext),
    dcontexts: if (builtin.is_test) []LockedDecompressionContext else void,
    dring: if (builtin.is_test) Ring(LockedDecompressionContext) else void,

    pub fn init(allocator: std.mem.Allocator, count: usize) !*Self {
        std.debug.assert(count > 0);

        const contexts = try allocator.alloc(LockedContext, count);
        var inited: usize = 0;
        errdefer {
            for (contexts[0..inited]) |*ctx| {
                encoding.freeCCtx(ctx.val);
            }
            allocator.free(contexts);
        }

        for (0..contexts.len) |i| {
            contexts[i] = .{
                .val = try encoding.createCCtx(),
            };
            inited += 1;
        }

        const dcontexts = if (builtin.is_test) blk: {
            const items = try allocator.alloc(LockedDecompressionContext, count);
            var dinited: usize = 0;
            errdefer {
                for (items[0..dinited]) |*ctx| {
                    encoding.freeDCtx(ctx.val);
                }
                allocator.free(items);
            }

            for (0..items.len) |i| {
                items[i] = .{
                    .val = try encoding.createDCtx(),
                };
                dinited += 1;
            }
            break :blk items;
        } else {};
        errdefer if (builtin.is_test) {
            for (dcontexts) |*ctx| {
                encoding.freeDCtx(ctx.val);
            }
            allocator.free(dcontexts);
        };

        const pool = try allocator.create(Self);
        pool.* = .{
            .contexts = contexts,
            .ring = Ring(LockedContext).init(contexts),
            .dcontexts = dcontexts,
            .dring = if (builtin.is_test) Ring(LockedDecompressionContext).init(dcontexts) else {},
        };
        return pool;
    }

    pub fn deinit(self: *Self, allocator: std.mem.Allocator) void {
        for (self.contexts) |*ctx| {
            encoding.freeCCtx(ctx.val);
        }
        allocator.free(self.contexts);
        if (builtin.is_test) {
            for (self.dcontexts) |*ctx| {
                encoding.freeDCtx(ctx.val);
            }
            allocator.free(self.dcontexts);
        }
        allocator.destroy(self);
    }

    pub fn next(self: *Self) *LockedContext {
        return self.ring.next();
    }

    pub fn compressAuto(self: *Self, io: Io, dst: []u8, src: []const u8) !usize {
        const locked = self.next();
        locked.mx.lockUncancelable(io);
        defer locked.mx.unlock(io);

        return encoding.compressAuto(locked.val, dst, src);
    }

    pub fn decompress(self: *Self, io: Io, dst: []u8, src: []const u8) !usize {
        if (!builtin.is_test) @compileError("use DecompressionPool for decompression");

        const locked = self.dring.next();
        locked.mx.lockUncancelable(io);
        defer locked.mx.unlock(io);

        return encoding.decompress(locked.val, dst, src);
    }
};

pub const DecompressionPoolImpl = struct {
    const Self = @This();
    const LockedContext = Locked(encoding.DCtx);

    contexts: []LockedContext,
    ring: Ring(LockedContext),

    pub fn init(allocator: std.mem.Allocator, count: usize) !*Self {
        std.debug.assert(count > 0);

        const contexts = try allocator.alloc(LockedContext, count);
        var inited: usize = 0;
        errdefer {
            for (contexts[0..inited]) |*ctx| {
                encoding.freeDCtx(ctx.val);
            }
            allocator.free(contexts);
        }

        for (0..contexts.len) |i| {
            contexts[i] = .{
                .val = try encoding.createDCtx(),
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
            encoding.freeDCtx(ctx.val);
        }
        allocator.free(self.contexts);
        allocator.destroy(self);
    }

    pub fn next(self: *Self) *LockedContext {
        return self.ring.next();
    }

    pub fn decompress(self: *Self, io: Io, dst: []u8, src: []const u8) !usize {
        const locked = self.next();
        locked.mx.lockUncancelable(io);
        defer locked.mx.unlock(io);

        return encoding.decompress(locked.val, dst, src);
    }
};

pub const DecompressionPool = if (builtin.is_test) CompressionPool else DecompressionPoolImpl;
