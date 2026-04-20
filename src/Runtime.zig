const std = @import("std");
const Allocator = std.mem.Allocator;

const builtin = @import("builtin");

const C = @import("c").C;

const DiskSpace = struct {
    total: u64,
    free: u64,
    updatedAtMs: u64,
};

const Runtime = @This();

// RAM available in bytes
maxMem: u64,
// calculated property from maxMem and maxCachePortion
cacheSize: u64,
// disk space for the store
diskSpace: DiskSpace,
diskStatsMx: std.Thread.Mutex = .{},
// path is a disk mount point for the store to get a disk space
path: []const u8,

// cpus available
cpus: u16,
// http threads available, derived from cpus
httpThreads: u16,
// worker threads available, derived from cpus
workerThreads: u16,

pub fn init(alloc: Allocator, path: []const u8, maxCachePortition: f64) !*Runtime {
    // TODO: create a designated testing runtime and restrict this init to use inside tests
    const cpus = getCpuCount();
    // 4 is a minimum amount of threads for workers
    const totalThreads: u16 = @intCast(@max(cpus, 8));
    // TODO: the numbers must be tuned further to balance between http and workers
    const workers = totalThreads / 2;
    // TODO: http threads are not used yet
    const https = totalThreads - workers;

    std.debug.assert(workers >= 4);

    const maxMem = blk: switch (builtin.os.tag) {
        .macos => {
            var memsize: u64 = 0;
            var len: usize = @sizeOf(u64);
            try std.posix.sysctlbynameZ("hw.memsize", &memsize, &len, null, 0);

            break :blk memsize;
        },
        .linux => {
            var info: std.os.linux.Sysinfo = undefined;
            const result: usize = std.os.linux.sysinfo(&info);
            if (std.os.linux.E.init(result) != .SUCCESS) {
                return error.UnknownTotalSystemMemory;
            }

            const maxMem: u64 = @as(u64, info.totalram) * info.mem_unit;
            break :blk maxMem;
        },
        else => return error.UnsupportedOS,
    };

    const maxMemF: f64 = @floatFromInt(maxMem);

    const r = try alloc.create(Runtime);
    r.* = .{
        .maxMem = maxMem,
        .cacheSize = @intFromFloat(maxMemF * maxCachePortition),
        .diskSpace = getDiskSpace(path, @intCast(std.time.milliTimestamp())),
        .path = path,
        .cpus = @intCast(cpus),
        .httpThreads = https,
        .workerThreads = workers,
    };
    return r;
}

pub fn deinit(self: *Runtime, alloc: Allocator) void {
    alloc.destroy(self);
}

pub fn getFreeDiskSpace(self: *Runtime) u64 {
    self.diskStatsMx.lock();
    defer self.diskStatsMx.unlock();

    const nowMs: u64 = @intCast(std.time.milliTimestamp());
    if ((nowMs - self.diskSpace.updatedAtMs) < 15 * std.time.ms_per_s) {
        return self.diskSpace.free;
    }

    return self.updateDiskSpace(nowMs).free;
}

fn updateDiskSpace(self: *Runtime, nowMs: u64) DiskSpace {
    const space = getDiskSpace(self.path, nowMs);
    // TODO: log an error and return the old value, then signal to shutdown
    self.diskSpace = space;
    return space;
}

fn getDiskSpace(path: []const u8, nowMs: u64) DiskSpace {
    const pathZ = std.posix.toPosixPath(path) catch {
        std.debug.panic("disk space path too long: '{s}'", .{path});
    };
    var stat: C.struct_statvfs = .{};
    switch (std.posix.errno(C.statvfs(&pathZ, &stat))) {
        .SUCCESS => {},
        else => |err| std.debug.panic("failed to get disk space for path='{s}': {s}", .{ path, @tagName(err) }),
    }

    const blockSize: u64 = if (stat.f_frsize > 0) @intCast(stat.f_frsize) else @intCast(stat.f_bsize);
    return .{
        .total = @as(u64, @intCast(stat.f_blocks)) * blockSize,
        .free = @as(u64, @intCast(stat.f_bavail)) * blockSize,
        .updatedAtMs = nowMs,
    };
}

fn getCpuCount() usize {
    const cpus = std.Thread.getCpuCount() catch |err| {
        std.debug.print("[WARN] failed to get CPU count, defaulting to 4 threads: {s}", .{@errorName(err)});
        return 4;
    };
    return cpus;
}

const testing = std.testing;

test "getFreeDiskSpace returns same positive value" {
    const r = try Runtime.init(testing.allocator, ".", 0.5);
    defer r.deinit(testing.allocator);

    const first = r.getFreeDiskSpace();
    const second = r.getFreeDiskSpace();

    try std.testing.expect(first > 0);
    try std.testing.expectEqual(first, second);
}
