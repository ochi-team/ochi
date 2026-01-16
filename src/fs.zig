const std = @import("std");

pub fn syncPathAndParentDir(path: []const u8) void {
    syncPath(path);

    const parent = std.fs.path.dirname(path) orelse std.debug.panic("path has no parent directory: '{s}'", .{path});
    syncPath(parent);
}

fn syncPath(path: []const u8) void {
    // TODO: handle the error and write data to a recovery log,
    // panicking here means data loss
    var file = std.fs.openFileAbsolute(path, .{}) catch std.debug.panic("failed to sync path={s}", .{path});
    defer file.close();

    file.sync() catch |err| {
        std.debug.panic(
            "FATAL: cannot flush '{s}' to storage: {s}",
            .{ path, @errorName(err) },
        );
    };
}

pub fn writeBufferValToFile(
    path: []const u8,
    buffer_val: []const u8,
) !void {
    var file = try std.fs.createFileAbsolute(
        path,
        .{ .truncate = true },
    );
    defer file.close();

    try file.writeAll(buffer_val);
    try file.sync();
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
