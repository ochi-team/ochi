const std = @import("std");
const Allocator = std.mem.Allocator;
const Io = std.Io;
const Dir = Io.Dir;

// TODO: perhaps worth replacing all the panics in the package,
// and return regular error and handle them outside.
// It allows testing its capabilities better
// and handle the crash safer

var tmpFileNum = std.atomic.Value(u64).init(0);

pub fn pathExists(io: Io, path: []const u8) !bool {
    Dir.accessAbsolute(io, path, .{}) catch |err| {
        switch (err) {
            error.FileNotFound => return false,
            else => return err,
        }
    };

    return true;
}

pub fn syncPathAndParentDir(io: Io, path: []const u8) void {
    syncPath(io, path);

    const parent = std.fs.path.dirname(path) orelse std.debug.panic("path has no parent directory: '{s}'", .{path});
    syncPath(io, parent);
}

fn syncPath(io: Io, path: []const u8) void {
    // TODO: handle the error and write data to a recovery log,
    // panicking here means data loss
    if (Dir.openFileAbsolute(io, path, .{})) |file| {
        var f = file;
        defer f.close(io);
        f.sync(io) catch |err| {
            std.debug.panic(
                "FATAL: cannot flush '{s}' to storage: {s}",
                .{ path, @errorName(err) },
            );
        };
        return;
    } else |err| {
        std.debug.panic(
            "FATAL: cannot flush '{s}' to storage: {s}",
            .{ path, @errorName(err) },
        );
    }
}

pub fn createDirAssert(io: Io, path: []const u8) void {
    const e = Dir.accessAbsolute(io, path, .{});
    std.debug.assert(e == error.FileNotFound);
    Dir.createDirAbsolute(io, path, .default_dir) catch |err| {
        std.debug.panic("failed to make dir {s}: {s}", .{ path, @errorName(err) });
    };
}

pub fn writeBufferValToFile(
    io: Io,
    path: []const u8,
    bufferVal: []const u8,
) !void {
    var file = try Dir.createFileAbsolute(
        io,
        path,
        .{ .truncate = true },
    );
    defer file.close(io);

    try file.writeStreamingAll(io, bufferVal);
    try file.sync(io);
}

// TODO: take a look at std.Io.Dir.cwd().atomicFile
pub fn writeBufferToFileAtomic(
    io: Io,
    path: []const u8,
    bufferVal: []const u8,
    truncate: bool,
) !void {
    if (!truncate) {
        if (Dir.accessAbsolute(io, path, .{})) {
            std.debug.panic("failed to write atomic file, path '{s}' already exists", .{path});
        } else |err| switch (err) {
            error.FileNotFound => {},
            else => std.debug.panic("failed to access file '{s}': {s}", .{ path, @errorName(err) }),
        }
    }

    const n = tmpFileNum.fetchAdd(1, .monotonic);
    // keep the temp path absolute to use createFileAbsolute/openFileAbsolute
    var buf: [std.fs.max_path_bytes]u8 = undefined;
    const tmpPath = try std.fmt.bufPrint(&buf, "{s}-tmp-{d}", .{ path, n });

    try writeBufferValToFile(tmpPath, bufferVal);
    errdefer std.fs.deleteFileAbsolute(tmpPath) catch {};

    try std.fs.renameAbsolute(tmpPath, path);

    // This is because fsync() does not guarantee that
    // the directory entry of the given file has also reached the disk;
    // it only synchronizes the file's data and inode.
    // Consequently, it is possible that a power outage
    // could render a new file inaccessible even if you properly synchronized it.
    // If you did not just create the file, there is no need to synchronize its directory.
    const parent = std.fs.path.dirname(path) orelse return error.PathHasNoParent;
    syncPath(io, parent);
}

pub fn readAll(io: Io, alloc: Allocator, path: []const u8) ![]u8 {
    var file = try Dir.openFileAbsolute(io, path, .{});
    defer file.close(io);
    const size = (try file.stat(io)).size;

    const dst = try alloc.alloc(u8, size);
    errdefer alloc.free(dst);

    _ = try file.readPositionalAll(io, dst, 0);
    return dst;
}

test "pathExists returns true for existing paths and false for missing path" {
    const alloc = std.testing.allocator;
    const io = std.testing.io;

    const Case = struct {
        path: []const u8,
        expected: bool,
    };

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    try tmp.dir.makePath("nested");
    {
        var file = try tmp.dir.createFile(io, "existing.txt", .{});
        defer file.close(io);
        try file.writeStreamingAll(io, "content");
    }

    const tmp_path = try tmp.dir.realPathFileAlloc(io, alloc, ".");
    defer alloc.free(tmp_path);
    const existing_file = try std.fs.path.join(alloc, &.{ tmp_path, "existing.txt" });
    defer alloc.free(existing_file);
    const existing_dir = try std.fs.path.join(alloc, &.{ tmp_path, "nested" });
    defer alloc.free(existing_dir);
    const missing = try std.fs.path.join(alloc, &.{ tmp_path, "missing.txt" });
    defer alloc.free(missing);

    const cases = [_]Case{
        .{ .path = existing_file, .expected = true },
        .{ .path = existing_dir, .expected = true },
        .{ .path = missing, .expected = false },
    };

    for (cases) |case| {
        const actual = try pathExists(case.path);
        try std.testing.expectEqual(case.expected, actual);
    }
}

test "syncPathAndParentDir fsync file and parent directory" {
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const file_name = "test.txt";
    {
        var f = try tmp.dir.createFile(io, file_name, .{});
        defer f.close(io);
        try f.writeStreamingAll(io, "hello");
    }

    const abs_path = try tmp.dir.realPathFileAlloc(io, std.testing.allocator, file_name);
    defer std.testing.allocator.free(abs_path);

    syncPathAndParentDir(abs_path);
}

test "syncPathAndParentDir fsync directory and parent directory" {
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    try tmp.dir.makePath("nested");
    const abs_path = try tmp.dir.realPathFileAlloc(io, std.testing.allocator, "nested");
    defer std.testing.allocator.free(abs_path);

    syncPathAndParentDir(abs_path);
}

test "readAll reads full file content from tmp directory" {
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const file_name = "read-all.txt";
    const content = "simple content";

    {
        var f = try tmp.dir.createFile(io, file_name, .{});
        defer f.close(io);
        try f.writeStreamingAll(io, content);
    }

    const abs_path = try tmp.dir.realPathFileAlloc(io, std.testing.allocator, file_name);
    defer std.testing.allocator.free(abs_path);

    const actual = try readAll(std.testing.allocator, abs_path);
    defer std.testing.allocator.free(actual);

    try std.testing.expectEqualStrings(content, actual);
}

test "writeBufferValToFileAtomic writes and overwrites atomically" {
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const tmpPath = try tmp.dir.realPathFileAlloc(io, std.testing.allocator, ".");
    defer std.testing.allocator.free(tmpPath);
    const absPath = try std.fs.path.join(std.testing.allocator, &.{ tmpPath, "atomic.txt" });
    defer std.testing.allocator.free(absPath);

    {
        try writeBufferToFileAtomic(absPath, "first", false);
        const actual = try readAll(std.testing.allocator, absPath);
        defer std.testing.allocator.free(actual);
        try std.testing.expectEqualStrings("first", actual);
    }

    {
        try writeBufferToFileAtomic(absPath, "second", true);
        const actual = try readAll(std.testing.allocator, absPath);
        defer std.testing.allocator.free(actual);
        try std.testing.expectEqualStrings("second", actual);
    }
}
