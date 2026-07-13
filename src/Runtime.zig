const std = @import("std");
const Allocator = std.mem.Allocator;
const Io = std.Io;

const builtin = @import("builtin");

const C = @import("c").C;
const Logger = @import("logging");

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
diskStatsMx: Io.Mutex = .init,
// path is a disk mount point for the store to get a disk space
path: []const u8,

// cpus available
cpus: u16,

pub fn init(io: Io, alloc: Allocator, path: []const u8, maxCachePortition: f64) !*Runtime {
    std.debug.assert(path[path.len - 1] != std.fs.path.sep);

    const cpus = getCpuCount();

    const maxMem = blk: switch (builtin.os.tag) {
        .macos => {
            var memsize: u64 = 0;
            var len: usize = @sizeOf(u64);
            const no = std.posix.system.sysctlbyname("hw.memsize", &memsize, &len, null, 0);
            if (std.posix.errno(no) != .SUCCESS) {
                return error.UnknownTotalSystemMemory;
            }

            break :blk memsize;
        },
        .linux => {
            var info: std.os.linux.Sysinfo = undefined;
            const result: usize = std.os.linux.sysinfo(&info);
            if (std.os.linux.errno(result) != .SUCCESS) {
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
        .diskSpace = getDiskSpace(path, @intCast(Io.Timestamp.now(io, .real).toMilliseconds())),
        .path = path,
        .cpus = @intCast(cpus),
    };
    return r;
}

pub fn deinit(self: *Runtime, alloc: Allocator) void {
    alloc.destroy(self);
}

pub fn getFreeDiskSpace(self: *Runtime, io: Io) u64 {
    // TODO: revisit all the lockUncancelable, perhaps we should use a regular one,
    // read path must be cancelable 100%
    self.diskStatsMx.lockUncancelable(io);
    defer self.diskStatsMx.unlock(io);

    const nowMs: u64 = @intCast(Io.Timestamp.now(io, .real).toMilliseconds());
    if ((nowMs - self.diskSpace.updatedAtMs) < 15 * std.time.ms_per_s) {
        return self.diskSpace.free;
    }

    return self.updateDiskSpace(nowMs).free;
}

fn updateDiskSpace(self: *Runtime, nowMs: u64) DiskSpace {
    const space = getDiskSpace(self.path, nowMs);
    self.diskSpace = space;
    return space;
}

fn getDiskSpace(path: []const u8, nowMs: u64) DiskSpace {
    const pathZ = std.posix.toPosixPath(path) catch |err| switch (err) {
        error.NameTooLong => std.debug.panic("disk space path too long: '{s}'", .{path}),
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
        Logger.log(.warn, "failed to get CPU count, defaulting to 4 threads", .{ .err = err });
        return 4;
    };
    return cpus;
}

const testing = std.testing;

test "getFreeDiskSpace returns same positive value" {
    const r = try Runtime.init(testing.io, testing.allocator, ".", 0.5);
    defer r.deinit(testing.allocator);

    const first = r.getFreeDiskSpace(testing.io);
    const second = r.getFreeDiskSpace(testing.io);

    try testing.expect(first > 0);
    try testing.expectEqual(first, second);
}
