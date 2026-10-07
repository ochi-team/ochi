const std = @import("std");
const Io = std.Io;
const Dir = Io.Dir;

const fs = @import("fs.zig");
const filenames = @import("filenames.zig");

pub const Layout = @This();
partitionsDir: Dir,
partitionsPath: []const u8,

pub fn make(io: Io, path: []const u8, buf: []u8) !Layout {
    std.debug.assert(std.fs.path.isAbsolute(path));

    const partitionsPath = try std.fmt.bufPrint(
        buf,
        "{s}{c}{s}",
        .{ path, std.fs.path.sep, filenames.partitions },
    );
    const dir = try createStoreDirIfNotExists(io, path, partitionsPath);
    return .{
        .partitionsDir = dir,
        .partitionsPath = partitionsPath,
    };
}

pub fn createStoreDirIfNotExists(io: Io, path: []const u8, partitionsPath: []const u8) !Dir {
    Dir.accessAbsolute(io, path, .{}) catch |err| {
        switch (err) {
            error.FileNotFound => {
                try createDir(io, path, partitionsPath);
                return Dir.openDirAbsolute(io, partitionsPath, .{ .iterate = true });
            },
            else => return err,
        }
    };

    return Dir.openDirAbsolute(io, partitionsPath, .{ .iterate = true }) catch |err| switch (err) {
        error.FileNotFound => {
            try fs.createDirAssert(io, partitionsPath);
            try fs.syncPathAndParentDir(io, partitionsPath);
            return Dir.openDirAbsolute(io, partitionsPath, .{ .iterate = true });
        },
        else => return err,
    };
}

pub fn createDir(io: Io, path: []const u8, partitionsPath: []const u8) !void {
    try fs.createDirAssert(io, path);
    try fs.createDirAssert(io, partitionsPath);

    try fs.syncPathAndParentDir(io, path);
}
