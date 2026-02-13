const std = @import("std");
const Allocator = std.mem.Allocator;

// TODO: perhaps worth replacing all the panics in the package,
// and return regular error and handle them outside.
// It allows testing its capabilities better
// and handle the crash safer

var tmpFileNum = std.atomic.Value(u64).init(0);

pub fn syncPathAndParentDir(path: []const u8) void {
    syncPath(path);

    const parent = std.fs.path.dirname(path) orelse std.debug.panic("path has no parent directory: '{s}'", .{path});
    syncPath(parent);
}

fn syncPath(path: []const u8) void {
    // TODO: handle the error and write data to a recovery log,
    // panicking here means data loss
    if (std.fs.openFileAbsolute(path, .{})) |file| {
        var f = file;
        defer f.close();
        f.sync() catch |err| {
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

pub fn makeDirAssert(path: []const u8) void {
    const e = std.fs.accessAbsolute(path, .{});
    std.debug.assert(e == error.FileNotFound);
    std.fs.makeDirAbsolute(path) catch |err| {
        std.debug.panic("failed to make dir {s}: {s}", .{ path, @errorName(err) });
    };
}

pub fn writeBufferValToFile(
    path: []const u8,
    bufferVal: []const u8,
) !void {
    var file = try std.fs.createFileAbsolute(
        path,
        .{ .truncate = true },
    );
    defer file.close();

    try file.writeAll(bufferVal);
    try file.sync();
}

pub fn writeBufferToFileAtomic(
    alloc: Allocator,
    path: []const u8,
    bufferVal: []const u8,
    truncate: bool,
) !void {
    if (!truncate) {
        if (std.fs.accessAbsolute(path, .{})) {
            std.debug.panic("failed to write atomic file, path '{s}' already exists", .{path});
        } else |err| switch (err) {
            error.FileNotFound => {},
            else => std.debug.panic("failed to access file '{s}': {s}", .{ path, @errorName(err) }),
        }
    }

    const n = tmpFileNum.fetchAdd(1, .monotonic);
    // keep the temp path absolute to use createFileAbsolute/openFileAbsolute
    const tmpPath = try std.fmt.allocPrint(alloc, "{s}-tmp-{d}", .{ path, n });
    defer alloc.free(tmpPath);

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
    syncPath(parent);
}

pub fn readAll(alloc: Allocator, path: []const u8) ![]u8 {
    var file = try std.fs.openFileAbsolute(path, .{});
    defer file.close();
    const size = (try file.stat()).size;

    const dst = try alloc.alloc(u8, size);
    errdefer alloc.free(dst);

    _ = try file.readAll(dst);
    return dst;
}

test "syncPathAndParentDir fsync file and parent directory" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const file_name = "test.txt";
    {
        var f = try tmp.dir.createFile(file_name, .{});
        defer f.close();
        try f.writeAll("hello");
    }

    const abs_path = try tmp.dir.realpathAlloc(std.testing.allocator, file_name);
    defer std.testing.allocator.free(abs_path);

    syncPathAndParentDir(abs_path);
}

test "syncPathAndParentDir fsync directory and parent directory" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    try tmp.dir.makePath("nested");
    const abs_path = try tmp.dir.realpathAlloc(std.testing.allocator, "nested");
    defer std.testing.allocator.free(abs_path);

    syncPathAndParentDir(abs_path);
}

test "readAll reads full file content from tmp directory" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const file_name = "read-all.txt";
    const content = "simple content";

    {
        var f = try tmp.dir.createFile(file_name, .{});
        defer f.close();
        try f.writeAll(content);
    }

    const abs_path = try tmp.dir.realpathAlloc(std.testing.allocator, file_name);
    defer std.testing.allocator.free(abs_path);

    const actual = try readAll(std.testing.allocator, abs_path);
    defer std.testing.allocator.free(actual);

    try std.testing.expectEqualStrings(content, actual);
}

test "writeBufferValToFileAtomic writes and overwrites atomically" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const tmpPath = try tmp.dir.realpathAlloc(std.testing.allocator, ".");
    defer std.testing.allocator.free(tmpPath);
    const absPath = try std.fs.path.join(std.testing.allocator, &.{ tmpPath, "atomic.txt" });
    defer std.testing.allocator.free(absPath);

    {
        try writeBufferToFileAtomic(std.testing.allocator, absPath, "first", false);
        const actual = try readAll(std.testing.allocator, absPath);
        defer std.testing.allocator.free(actual);
        try std.testing.expectEqualStrings("first", actual);
    }

    {
        try writeBufferToFileAtomic(std.testing.allocator, absPath, "second", true);
        const actual = try readAll(std.testing.allocator, absPath);
        defer std.testing.allocator.free(actual);
        try std.testing.expectEqualStrings("second", actual);
    }
}
