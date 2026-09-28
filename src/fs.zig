const std = @import("std");
const Allocator = std.mem.Allocator;
const Io = std.Io;
const Dir = Io.Dir;

const Logger = @import("logging");

// a global tmp file counter in order to create unique file names suffixes
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

// Calling fsync() does not necessarily ensure that the entry in the directory
// containing the file has also reached disk.
// For that an explicit fsync() on a file descriptor for the directory is also needed.
// https://linux.die.net/man/2/fsync
pub fn syncPathAndParentDir(io: Io, path: []const u8) !void {
    try syncPath(io, path);

    const parent = std.fs.path.dirname(path) orelse return error.PathHasNoParent;
    try syncPath(io, parent);
}

fn syncPath(io: Io, path: []const u8) !void {
    // TODO: handle the error and write data to a recovery log
    const f = try Dir.openFileAbsolute(io, path, .{});
    defer f.close(io);
    try f.sync(io);
}

pub fn createDirAssert(io: Io, path: []const u8) !void {
    const e = Dir.accessAbsolute(io, path, .{});
    std.debug.assert(e == error.FileNotFound);
    try Dir.createDirAbsolute(io, path, .default_dir);
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

// TODO: take a look at std.Io.Dir.cwd().createFileAtomic
pub fn writeBufferToFileAtomic(
    io: Io,
    path: []const u8,
    bufferVal: []const u8,
    truncate: bool,
) !void {
    if (!truncate) {
        if (Dir.accessAbsolute(io, path, .{})) {
            return error.FileAlreadyExists;
        } else |err| {
            switch (err) {
                // the only successful case,
                // if we don't allow truncation then the file must not exist
                error.FileNotFound => {},
                else => return err,
            }
        }
    }

    const n = tmpFileNum.fetchAdd(1, .monotonic);
    // keep the temp path absolute to use createFileAbsolute/openFileAbsolute
    var buf: [std.fs.max_path_bytes]u8 = undefined;
    const tmpPath = try std.fmt.bufPrint(&buf, "{s}-tmp-{d}", .{ path, n });

    try writeBufferValToFile(io, tmpPath, bufferVal);
    errdefer Dir.deleteFileAbsolute(io, tmpPath) catch {};

    try Dir.renameAbsolute(tmpPath, path, io);

    // This is because fsync() does not guarantee that
    // the directory entry of the given file has also reached the disk;
    // it only synchronizes the file's data and inode.
    // Consequently, it is possible that a power outage
    // could render a new file inaccessible even if you properly synchronized it.
    // If you did not just create the file, there is no need to synchronize its directory.
    const parent = std.fs.path.dirname(path) orelse return error.PathHasNoParent;
    try syncPath(io, parent);
}

pub fn readAll(io: Io, alloc: Allocator, path: []const u8) ![]u8 {
    var file = try Dir.openFileAbsolute(io, path, .{});
    defer file.close(io);
    const size = (try file.stat(io)).size;

    const dst = try alloc.alloc(u8, size);
    errdefer alloc.free(dst);
    Logger.log(.debug, "read full file", .{
        .size = size,
        .path = path,
    });

    _ = try file.readPositionalAll(io, dst, 0);
    return dst;
}

pub fn deleteTreeAbsolute(io: Io, absolute_path: []const u8) !void {
    std.debug.assert(std.fs.path.isAbsolute(absolute_path));
    const dirname = std.fs.path.dirname(absolute_path) orelse return error{
        /// Attempt to remove the root file system path.
        /// This error is unreachable if `absolute_path` is relative.
        CannotDeleteRootDirectory,
    }.CannotDeleteRootDirectory;

    var dir = try Dir.cwd().openDir(io, dirname, .{});
    defer dir.close(io);

    return dir.deleteTree(io, std.fs.path.basename(absolute_path));
}

const testing = std.testing;

test {
    _ = @import("fs_test.zig");
}
