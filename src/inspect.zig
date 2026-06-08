const std = @import("std");
const Io = std.Io;
const builtin = @import("builtin");

const maxTableSize = @import("Conf.zig").maxTableSize;
const minOpenFileDescriptors = 1 << 16;
const requiredOvercommit = "2";
const overcommitPath = "/proc/sys/vm/overcommit_memory";

pub const Error = error{
    NotReleaseMode,
    NotLinux,
    InvalidOvercommit,
};

pub fn inspect(strict: bool, io: Io) !void {
    switch (builtin.os.tag) {
        .linux => {},
        else => {
            if (builtin.mode == .Debug) {
                if (strict) return error.NotReleaseMode;
                return;
            }

            std.debug.print("Ochi must run only on Linux", .{});
            if (strict) return error.NotLinux;
        },
    }

    try inspectFds(strict);
    try inspectFsize(strict);
    try inspectOvercommit(strict, io);
}

fn inspectFds(strict: bool) !void {
    const limits = std.posix.getrlimit(.NOFILE) catch |err| {
        std.debug.print("failed to read max open file descriptor limit: {}", .{err});
        if (strict) return err;
        return;
    };

    if (limits.cur >= minOpenFileDescriptors) return;

    std.debug.print(
        "max open file descriptor limit is too low: current={d}, required={d}",
        .{ limits.cur, minOpenFileDescriptors },
    );

    const minLimit: std.posix.rlim_t = minOpenFileDescriptors;
    const nextLimits: std.posix.rlimit = .{
        .cur = minLimit,
        .max = @max(limits.max, minLimit),
    };

    std.posix.setrlimit(.NOFILE, nextLimits) catch |err| {
        std.debug.print(
            "failed to raise max open file descriptor limit: current={d}, hard={d}, required={d}, err={}",
            .{ limits.cur, limits.max, minOpenFileDescriptors, err },
        );
        if (strict) return err;
        return;
    };
}

fn inspectFsize(strict: bool) !void {
    const limits = std.posix.getrlimit(.FSIZE) catch |err| {
        std.debug.print("failed to read max file size limit: {}", .{err});
        if (strict) return err;
        return;
    };

    const minLimit: std.posix.rlim_t = maxTableSize;
    if (limits.cur == std.posix.RLIM.INFINITY or limits.cur >= minLimit) return;

    std.debug.print(
        "max file size limit is too low: current={d}, required={d}",
        .{ limits.cur, maxTableSize },
    );

    const nextLimits: std.posix.rlimit = .{
        .cur = minLimit,
        .max = @max(limits.max, minLimit),
    };

    std.posix.setrlimit(.FSIZE, nextLimits) catch |err| {
        std.debug.print(
            "failed to raise max file size limit: current={d}, hard={d}, required={d}, err={}",
            .{ limits.cur, limits.max, maxTableSize, err },
        );
        if (strict) return err;
        return;
    };
}

fn inspectOvercommit(strict: bool, io: Io) !void {
    const f = std.Io.Dir.cwd().openFile(io, overcommitPath, .{ .mode = .read_only }) catch |err| {
        std.debug.print("failed to read overcommit setting from {s}: {}", .{ overcommitPath, err });
        if (strict) return err;
        return;
    };
    defer f.close(io);

    var buf: [8]u8 = undefined;
    const n = f.readPositionalAll(io, &buf, 0) catch |err| {
        std.debug.print("failed to read overcommit setting from {s}: {}", .{ overcommitPath, err });
        if (strict) return err;
        return;
    };

    const value = std.mem.trim(u8, buf[0..n], &std.ascii.whitespace);
    if (std.mem.eql(u8, value, requiredOvercommit)) return;

    std.debug.print(
        "overcommit setting is invalid: path={s}, current={s}, required={s}",
        .{ overcommitPath, value, requiredOvercommit },
    );
    if (strict) return error.InvalidOvercommit;
}
