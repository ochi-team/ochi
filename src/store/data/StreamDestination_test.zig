const std = @import("std");
const Io = std.Io;
const Dir = Io.Dir;

pub const FileDestination = struct {
    file: Io.File,
    len: usize,
    /// buffer holds allocated chunk to reuse between write operations
    /// it's borrowed and shared between others destinations, so we don't clean it
    buf: *std.ArrayList(u8),
};

const StreamDestination = @import("StreamDestination.zig").StreamDestination;

const testing = std.testing;

test "StreamDestination buffer destination" {
    const alloc = testing.allocator;
    const io = testing.io;

    var buf = try std.ArrayList(u8).initCapacity(alloc, 8);
    defer buf.deinit(alloc);
    var dst = StreamDestination.initBuffer(&buf);
    defer dst.deinit(io);

    try dst.appendSlice(io, alloc, "abc");
    try dst.appendSlice(io, alloc, "1234");

    try testing.expectEqual(7, dst.len());

    const all = try dst.readAll(io, alloc);
    defer alloc.free(all);
    try testing.expectEqualStrings("abc1234", all);
}

test "StreamDestination file destination" {
    const alloc = testing.allocator;
    const io = testing.io;

    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    const rootPath = try tmp.dir.realPathFileAlloc(io, ".", alloc);
    defer alloc.free(rootPath);
    var filePathBuf: [std.fs.max_path_bytes]u8 = undefined;
    var filePathWriter = std.Io.Writer.fixed(&filePathBuf);
    try std.fs.path.fmtJoin(&.{ rootPath, "timestamps.bin" }).format(&filePathWriter);
    const filePath = filePathWriter.buffered();

    const file = try Dir.createFileAbsolute(io, filePath, .{ .truncate = true, .read = true });
    var buf = std.ArrayList(u8).empty;
    defer buf.deinit(alloc);
    var dst = try StreamDestination.initFile(io, file, &buf);
    defer dst.deinit(io);

    const res = "hello-world";
    try dst.appendSlice(io, alloc, "hello");
    try dst.appendSlice(io, alloc, "-world");
    try testing.expectEqual(res.len, dst.len());

    const all = try dst.readAll(io, alloc);
    defer alloc.free(all);
    try testing.expectEqualStrings(res, all);

    var verify = try Dir.openFileAbsolute(io, filePath, .{});
    defer verify.close(io);

    var verify_reader = file.reader(io, &.{});
    const onDisk = try verify_reader.interface.allocRemaining(alloc, .unlimited);
    defer alloc.free(onDisk);
    try testing.expectEqualStrings(res, onDisk);
}
