const std = @import("std");
const Allocator = std.mem.Allocator;
const Io = std.Io;
const Consts = @import("Consts.zig");

const Self = @This();

pub const Buffer = struct {
    bytes: []u8,
    pool: *Self,
    refs: std.atomic.Value(usize),
    next: ?*Buffer = null,

    /// Retains the backing bytes independently of the accumulator and other shards.
    pub fn retain(self: *Buffer) void {
        const previous = self.refs.fetchAdd(1, .monotonic);
        std.debug.assert(previous > 0);
    }

    /// Recycles the buffer only after its last reader has finished encoding.
    pub fn release(self: *Buffer) void {
        const previous = self.refs.fetchSub(1, .acq_rel);
        std.debug.assert(previous > 0);
        if (previous != 1) return;

        const pool = self.pool;
        pool.mx.lockUncancelable(pool.io);
        defer pool.mx.unlock(pool.io);
        self.next = pool.free;
        pool.free = self;
    }
};

io: Io,
alloc: Allocator,
mx: Io.Mutex = .init,
buffers: []Buffer,
created: usize = 0,
free: ?*Buffer = null,

/// Allocates stable buffer handles; backing bytes are allocated lazily up to capacity.
pub fn init(io: Io, alloc: Allocator, capacity: usize) !*Self {
    std.debug.assert(capacity > 0);
    const buffers = try alloc.alloc(Buffer, capacity);
    errdefer alloc.free(buffers);
    const pool = try alloc.create(Self);
    pool.* = .{ .io = io, .alloc = alloc, .buffers = buffers };
    return pool;
}

/// All accumulators and shards must release their buffers before pool destruction.
pub fn deinit(self: *Self) void {
    for (self.buffers[0..self.created]) |*buffer| {
        std.debug.assert(buffer.refs.load(.acquire) == 0);
        self.alloc.free(buffer.bytes);
    }
    self.alloc.free(self.buffers);
    self.alloc.destroy(self);
}

/// Returns null at capacity so the caller can flush shards without blocking timers.
pub fn tryAcquire(self: *Self) !?*Buffer {
    self.mx.lockUncancelable(self.io);
    defer self.mx.unlock(self.io);

    if (self.free) |buffer| {
        self.free = buffer.next;
        buffer.next = null;
        buffer.refs.store(1, .release);
        return buffer;
    }
    if (self.created == self.buffers.len) return null;

    const bytes = try self.alloc.alloc(u8, Consts.maxBlockSize);
    const buffer = &self.buffers[self.created];
    buffer.* = .{ .bytes = bytes, .pool = self, .refs = .init(1) };
    self.created += 1;
    return buffer;
}
