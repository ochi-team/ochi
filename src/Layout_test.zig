const std = @import("std");
const Io = std.Io;
const Dir = Io.Dir;

const fs = @import("fs.zig");
const filenames = @import("filenames.zig");

pub const Layout = @import("Layout.zig");
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

        _ = try Layout.createStoreDirIfNotExists(io, storePath, partitionsPath);

        try testing.expect(try fs.pathExists(io, storePath));
        try testing.expect(try fs.pathExists(io, partitionsPath));
    }
}
