const std = @import("std");
const Io = std.Io;
const builtin = @import("builtin");
const Logger = @import("logging");

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

            Logger.log(.err, "Ochi must run only on Linux", .{});
            if (strict) return error.NotLinux;
        },
    }

    try inspectFds(strict);
    try inspectFsize(strict);
    try inspectOvercommit(strict, io);
}

fn inspectFds(strict: bool) !void {
    const limits = std.posix.getrlimit(.NOFILE) catch |err| {
        Logger.log(.err, "failed to read max open file descriptor limit", .{ .err = err });
        if (strict) return err;
        return;
    };

    if (limits.cur >= minOpenFileDescriptors) return;

    Logger.log(.warn, "max open file descriptor limit is too low", .{
        .current = limits.cur,
        .required = minOpenFileDescriptors,
    });

    const minLimit: std.posix.rlim_t = minOpenFileDescriptors;
    const nextLimits: std.posix.rlimit = .{
        .cur = minLimit,
        .max = @max(limits.max, minLimit),
    };

    std.posix.setrlimit(.NOFILE, nextLimits) catch |err| {
        Logger.log(.err, "failed to raise max open file descriptor limit", .{
            .current = limits.cur,
            .hard = limits.max,
            .required = minOpenFileDescriptors,
            .err = err,
        });
        if (strict) return err;
        return;
    };
}

fn inspectFsize(strict: bool) !void {
    const limits = std.posix.getrlimit(.FSIZE) catch |err| {
        Logger.log(.err, "failed to read max file size limit", .{ .err = err });
        if (strict) return err;
        return;
    };

    const minLimit: std.posix.rlim_t = maxTableSize;
    if (limits.cur == std.posix.RLIM.INFINITY or limits.cur >= minLimit) return;

    Logger.log(.warn, "max file size limit is too low", .{
        .current = limits.cur,
        .required = maxTableSize,
    });

    const nextLimits: std.posix.rlimit = .{
        .cur = minLimit,
        .max = @max(limits.max, minLimit),
    };

    std.posix.setrlimit(.FSIZE, nextLimits) catch |err| {
        Logger.log(.err, "failed to raise max file size limit", .{
            .current = limits.cur,
            .hard = limits.max,
            .required = maxTableSize,
            .err = err,
        });
        if (strict) return err;
        return;
    };
}

fn inspectOvercommit(strict: bool, io: Io) !void {
    const f = std.Io.Dir.cwd().openFile(io, overcommitPath, .{ .mode = .read_only }) catch |err| {
        Logger.log(.err, "failed to read overcommit setting", .{ .path = overcommitPath, .err = err });
        if (strict) return err;
        return;
    };
    defer f.close(io);

    var buf: [8]u8 = undefined;
    const n = f.readPositionalAll(io, &buf, 0) catch |err| {
        Logger.log(.err, "failed to read overcommit setting", .{ .path = overcommitPath, .err = err });
        if (strict) return err;
        return;
    };

    const value = std.mem.trim(u8, buf[0..n], &std.ascii.whitespace);
    if (std.mem.eql(u8, value, requiredOvercommit)) return;

    Logger.log(.err, "overcommit setting is invalid", .{
        .path = overcommitPath,
        .current = value,
        .required = requiredOvercommit,
    });
    if (strict) return error.InvalidOvercommit;
}
