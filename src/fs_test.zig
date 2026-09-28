const std = @import("std");

const fs = @import("fs.zig");
const testing = std.testing;

test "pathExists returns true for existing paths and false for missing path" {
    const alloc = testing.allocator;
    const io = testing.io;

    const Case = struct {
        path: []const u8,
        expected: bool,
    };

    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    try tmp.dir.createDirPath(io, "nested");
    {
        var file = try tmp.dir.createFile(io, "existing.txt", .{});
        defer file.close(io);
        try file.writeStreamingAll(io, "content");
    }

    const tmp_path = try tmp.dir.realPathFileAlloc(io, ".", alloc);
    defer alloc.free(tmp_path);
    var existingFileBuf: [std.fs.max_path_bytes]u8 = undefined;
    var existingFileWriter = std.Io.Writer.fixed(&existingFileBuf);
    try std.fs.path.fmtJoin(&.{ tmp_path, "existing.txt" }).format(&existingFileWriter);
    const existing_file = existingFileWriter.buffered();

    var existingDirBuf: [std.fs.max_path_bytes]u8 = undefined;
    var existingDirWriter = std.Io.Writer.fixed(&existingDirBuf);
    try std.fs.path.fmtJoin(&.{ tmp_path, "nested" }).format(&existingDirWriter);
    const existing_dir = existingDirWriter.buffered();

    var missingBuf: [std.fs.max_path_bytes]u8 = undefined;
    var missingWriter = std.Io.Writer.fixed(&missingBuf);
    try std.fs.path.fmtJoin(&.{ tmp_path, "missing.txt" }).format(&missingWriter);
    const missing = missingWriter.buffered();

    const cases = [_]Case{
        .{ .path = existing_file, .expected = true },
        .{ .path = existing_dir, .expected = true },
        .{ .path = missing, .expected = false },
    };

    for (cases) |case| {
        const actual = try fs.pathExists(io, case.path);
        try testing.expectEqual(case.expected, actual);
    }
}

test "syncPathAndParentDir fsync file and parent directory" {
    const io = testing.io;

    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    const file_name = "test.txt";
    {
        var f = try tmp.dir.createFile(io, file_name, .{});
        defer f.close(io);
        try f.writeStreamingAll(io, "hello");
    }

    const abs_path = try tmp.dir.realPathFileAlloc(io, file_name, testing.allocator);
    defer testing.allocator.free(abs_path);

    try fs.syncPathAndParentDir(io, abs_path);
}

test "syncPathAndParentDir fsync directory and parent directory" {
    const io = testing.io;

    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    try tmp.dir.createDirPath(io, "nested");
    const abs_path = try tmp.dir.realPathFileAlloc(io, "nested", testing.allocator);
    defer testing.allocator.free(abs_path);

    try fs.syncPathAndParentDir(io, abs_path);
}

test "readAll reads full file content from tmp directory" {
    const io = testing.io;

    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    const file_name = "read-all.txt";
    const content = "simple content";

    {
        var f = try tmp.dir.createFile(io, file_name, .{});
        defer f.close(io);
        try f.writeStreamingAll(io, content);
    }

    const abs_path = try tmp.dir.realPathFileAlloc(io, file_name, testing.allocator);
    defer testing.allocator.free(abs_path);

    const actual = try fs.readAll(testing.io, testing.allocator, abs_path);
    defer testing.allocator.free(actual);

    try testing.expectEqualStrings(content, actual);
}

test "writeBufferValToFileAtomicWritesAndOverwritesAtomically" {
    const io = testing.io;

    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    const tmpPath = try tmp.dir.realPathFileAlloc(io, ".", testing.allocator);
    defer testing.allocator.free(tmpPath);
    var absPathBuf: [std.fs.max_path_bytes]u8 = undefined;
    var absPathWriter = std.Io.Writer.fixed(&absPathBuf);
    try std.fs.path.fmtJoin(&.{ tmpPath, "atomic.txt" }).format(&absPathWriter);
    const absPath = absPathWriter.buffered();

    {
        try fs.writeBufferToFileAtomic(io, absPath, "first", false);
        const actual = try fs.readAll(testing.io, testing.allocator, absPath);
        defer testing.allocator.free(actual);
        try testing.expectEqualStrings("first", actual);
    }

    {
        try fs.writeBufferToFileAtomic(io, absPath, "second", true);
        const actual = try fs.readAll(testing.io, testing.allocator, absPath);
        defer testing.allocator.free(actual);
        try testing.expectEqualStrings("second", actual);
    }
}
