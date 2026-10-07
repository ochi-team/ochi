const std = @import("std");
const Allocator = std.mem.Allocator;
const Io = std.Io;

pub const maxCallstack = 32;

const SamplerUnit = enum {
    bytes,
    count,
};

// extern because the fields are ordered by access order, not alignment
pub const StackBucket = extern struct {
    addrs: [maxCallstack]usize = undefined,
    addrsLen: u8 = 0,
    allocObjects: i64 = 0,
    allocBytes: i64 = 0,
    inuseObjects: i64 = 0,
    inuseBytes: i64 = 0,

    pub fn getAddrs(self: *const StackBucket) []const usize {
        return self.addrs[0..self.addrsLen];
    }
};

const Ptr = struct {
    stackHash: u64,
    size: usize,
};

const PprofAllocator = @This();

child: Allocator,
io: Io,
mx: Io.Mutex = .init,

samplerUnit: SamplerUnit = .bytes,
sampleRatio: usize = 0,
sampleState: std.atomic.Value(usize) = .init(0),

// state
stacks: std.AutoHashMapUnmanaged(u64, StackBucket) = .empty,
ptrs: std.AutoHashMapUnmanaged(usize, Ptr) = .empty,

pub fn deinit(self: *PprofAllocator) void {
    self.stacks.deinit(self.child);
    self.ptrs.deinit(self.child);
}

pub fn allocator(self: *PprofAllocator) Allocator {
    return .{
        .ptr = self,
        .vtable = &.{
            .alloc = alloc,
            .free = free,
            .resize = resize,
            .remap = remap,
        },
    };
}

pub fn snapshot(self: *PprofAllocator, a: Allocator) ![]StackBucket {
    try self.mx.lock(self.io);
    const bucketsCount = self.stacks.count();
    self.mx.unlock(self.io);

    const res = try a.alloc(StackBucket, bucketsCount);
    errdefer a.free(res);

    // incoming allocator may be pprof one, it locks during allocations to write the stack,
    // therefore it uses separate locks to let it allocate and not to deadlock
    try self.mx.lock(self.io);
    defer self.mx.unlock(self.io);
    var it = self.stacks.valueIterator();
    var i: usize = 0;
    while (it.next()) |next| : (i += 1) {
        if (i >= res.len) break;
        res[i] = next.*;
    }

    return res;
}

fn alloc(ctx: *anyopaque, len: usize, alignment: std.mem.Alignment, ra: usize) ?[*]u8 {
    const self: *PprofAllocator = @ptrCast(@alignCast(ctx));
    const res = self.child.rawAlloc(len, alignment, ra);
    if (res) |ptr| self.recordAlloc(ptr, len, ra);
    return res;
}

fn free(ctx: *anyopaque, buf: []u8, alignment: std.mem.Alignment, ra: usize) void {
    const self: *PprofAllocator = @ptrCast(@alignCast(ctx));
    self.recordFree(buf.ptr);
    self.child.rawFree(buf, alignment, ra);
}

fn resize(ctx: *anyopaque, buf: []u8, alignment: std.mem.Alignment, len: usize, ra: usize) bool {
    const self: *PprofAllocator = @ptrCast(@alignCast(ctx));
    const ok = self.child.rawResize(buf, alignment, len, ra);
    if (ok) {
        self.recordFree(buf.ptr);
        self.recordAlloc(buf.ptr, len, ra);
    }
    return ok;
}

fn remap(ctx: *anyopaque, buf: []u8, alignment: std.mem.Alignment, len: usize, ra: usize) ?[*]u8 {
    const self: *PprofAllocator = @ptrCast(@alignCast(ctx));
    const res = self.child.rawRemap(buf, alignment, len, ra);
    if (res) |remapped| {
        self.recordFree(buf
            .ptr);
        self.recordAlloc(remapped, len, ra);
    }
    return res;
}

fn recordAlloc(self: *PprofAllocator, ptr: [*]u8, len: usize, ra: usize) void {
    if (!self.sample(len)) return;

    var stackBuf: [maxCallstack]usize = undefined;
    const trace = std.debug.captureCurrentStackTrace(.{ .first_address = ra }, &stackBuf);
    const hash = hashStack(trace.return_addresses);

    self.mx.lock(self.io) catch return;
    defer self.mx.unlock(self.io);

    const entry = self.stacks.getOrPut(self.child, hash) catch {
        std.log.err("failed to store allocation stack", .{});
        return;
    };

    if (!entry.found_existing) {
        entry.value_ptr.* = .{};
        const n: u8 = @intCast(trace.return_addresses.len);
        @memcpy(entry.value_ptr.addrs[0..n], trace.return_addresses);
        entry.value_ptr.addrsLen = n;
    }
    entry.value_ptr.allocObjects += 1;
    entry.value_ptr.allocBytes += @intCast(len);
    entry.value_ptr.inuseObjects += 1;
    entry.value_ptr.inuseBytes += @intCast(len);

    self.ptrs.put(
        self.child,
        @intFromPtr(ptr),
        .{ .stackHash = hash, .size = len },
    ) catch {
        std.log.err("failed to store allocation ptr", .{});
        return;
    };
}

fn recordFree(self: *PprofAllocator, ptr: [*]u8) void {
    self.mx.lock(self.io) catch return;
    defer self.mx.unlock(self.io);

    const removed = self.ptrs.fetchRemove(@intFromPtr(ptr)) orelse {
        std.log.err("ptr not found to record free={d}", .{@intFromPtr(ptr)});
        return;
    };
    if (self.stacks.getPtr(removed.value.stackHash)) |bucket| {
        bucket.inuseObjects -= 1;
        bucket.inuseBytes -= @intCast(removed.value.size);
    }
}

fn hashStack(addrs: []usize) u64 {
    return std.hash.Wyhash.hash(0, std.mem.sliceAsBytes(addrs));
}

pub fn sample(self: *PprofAllocator, len: usize) bool {
    if (self.sampleRatio == 0) return true;

    const new = switch (self.samplerUnit) {
        .bytes => self.sampleState.fetchAdd(len, .monotonic) + len,
        .count => self.sampleState.fetchAdd(1, .monotonic) + 1,
    };

    const res = new >= self.sampleRatio;
    if (res) {
        _ = self.sampleState.fetchSub(self.sampleRatio, .monotonic);
    }
    return res;
}
