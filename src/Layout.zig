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

const testing = std.testing;

test "createStoreDirIfNotExists ensures store and partitions dirs exist" {
    const alloc = testing.allocator;
    const io = testing.io;

    const Case = struct {
        createStoreDir: bool,
        createPartitionsDir: bool,
    };

    const cases = [_]Case{
        .{ .createStoreDir = false, .createPartitionsDir = false },
        .{ .createStoreDir = true, .createPartitionsDir = false },
        .{ .createStoreDir = true, .createPartitionsDir = true },
    };

    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    const rootPath = try tmp.dir.realPathFileAlloc(io, ".", alloc);
    defer alloc.free(rootPath);

    for (cases, 0..) |case, i| {
        var storeNameBuf: [32]u8 = undefined;
        const storeName = try std.fmt.bufPrint(&storeNameBuf, "store-{d}", .{i});

        var storePathBuf: [std.fs.max_path_bytes]u8 = undefined;
        var storePathWriter = std.Io.Writer.fixed(&storePathBuf);
        try std.fs.path.fmtJoin(&.{ rootPath, storeName }).format(&storePathWriter);
        const storePath = storePathWriter.buffered();

        var partitionsPathBuf: [std.fs.max_path_bytes]u8 = undefined;
        var partitionsPathWriter = std.Io.Writer.fixed(&partitionsPathBuf);
        try std.fs.path.fmtJoin(&.{ storePath, filenames.partitions }).format(&partitionsPathWriter);
        const partitionsPath = partitionsPathWriter.buffered();

        if (case.createStoreDir) {
            try Dir.createDirAbsolute(io, storePath, .default_dir);
        }
        if (case.createPartitionsDir) {
            try Dir.createDirAbsolute(io, partitionsPath, .default_dir);
        }

        _ = try createStoreDirIfNotExists(io, storePath, partitionsPath);

        try testing.expect(try fs.pathExists(io, storePath));
        try testing.expect(try fs.pathExists(io, partitionsPath));
    }
}
