const std = @import("std");
const Io = std.Io;
const Allocator = std.mem.Allocator;
const Dir = Io.Dir;
const File = Io.File;

pub const DebugIo = @This();
const maxTraceFrames = 32;

pub const IoLeakError = error{
    FileDescriptorLeak,
};

/// defines the resource it track in order to report on a leakage
const OpenResource = struct {
    const Kind = enum {
        dir,
        file,
    };

    kind: Kind,
    path: []const u8,

    // holds a buffer used in stackTrace
    addrsBuf: [maxTraceFrames]usize = undefined,
    stackTrace: std.debug.StackTrace,

    fn captureStackTrace(self: *OpenResource, kind: Kind, path: []const u8) void {
        self.kind = kind;
        self.path = path;
        self.stackTrace = std.debug.captureCurrentStackTrace(.{
            .first_address = @returnAddress(),
            .allow_unsafe_unwind = false,
        }, &self.addrsBuf);
    }
};

base: Io,
alloc: Allocator,

/// holds open resources to track leakage
openMap: std.AutoHashMap(File.Handle, OpenResource),
openMapMutex: Io.Mutex = .init,
/// mutated copy of a base Io.VTable
vtable: Io.VTable,

fn debug(userdata: ?*anyopaque) *DebugIo {
    return @ptrCast(@alignCast(userdata.?));
}

pub fn io(self: *DebugIo) Io {
    return .{
        .userdata = self,
        .vtable = &self.vtable,
    };
}

pub fn init(base: Io, alloc: Allocator) DebugIo {
    var dio: DebugIo = .{
        .base = base,
        .alloc = alloc,
        .openMap = .init(alloc),
        .vtable = base.vtable.*,
    };

    dio.vtable.dirCreateDirPathOpen = dirCreateDirPathOpen;
    dio.vtable.dirOpenDir = dirOpenDir;
    dio.vtable.dirClose = dirClose;
    dio.vtable.dirCreateFile = dirCreateFile;
    dio.vtable.dirOpenFile = dirOpenFile;
    dio.vtable.fileClose = fileClose;
    return dio;
}

pub fn checkNoLeaks(self: *DebugIo) !void {
    if (self.openMap.count() == 0) return;

    const stderr = std.debug.lockStderr(&.{});
    defer std.debug.unlockStderr();

    const terminal = stderr.terminal();
    const writer = terminal.writer;

    var it = self.openMap.iterator();
    while (it.next()) |*entry| {
        try writer.print("========= io resource leaked ========\nleaked {s} fd={} path={s}\n", .{
            @tagName(entry.value_ptr.kind),
            entry.key_ptr.*,
            entry.value_ptr.path,
        });
        try std.debug.writeStackTrace(&entry.value_ptr.stackTrace, terminal);
    }

    return IoLeakError.FileDescriptorLeak;
}

pub fn deinit(self: *DebugIo) void {
    self.openMapMutex.lockUncancelable(self.base);
    defer self.openMapMutex.unlock(self.base);

    self.checkNoLeaks() catch {};

    var it = self.openMap.iterator();
    while (it.next()) |entry| {
        self.alloc.free(entry.value_ptr.path);
    }
    self.openMap.deinit();
}

fn trackOpen(self: *DebugIo, comptime kind: OpenResource.Kind, handle: File.Handle, path: []const u8) Allocator.Error!void {
    const pathCopy = try self.alloc.dupe(u8, path);
    errdefer self.alloc.free(pathCopy);

    self.openMapMutex.lockUncancelable(self.base);
    defer self.openMapMutex.unlock(self.base);

    var resource = try self.openMap.getOrPut(handle);
    resource.value_ptr.captureStackTrace(kind, pathCopy);
}

fn untrackOpen(self: *DebugIo, handle: File.Handle) void {
    self.openMapMutex.lockUncancelable(self.base);
    defer self.openMapMutex.unlock(self.base);

    if (self.openMap.fetchRemove(handle)) |entry| {
        self.alloc.free(entry.value.path);
    }
}

fn dirOpenDir(userdata: ?*anyopaque, dir: Dir, path: []const u8, options: Dir.OpenOptions) Dir.OpenError!Dir {
    const s = debug(userdata);
    const result = try s.base.vtable.dirOpenDir(s.base.userdata, dir, path, options);
    errdefer s.base.vtable.dirClose(s.base.userdata, (&result)[0..1]);

    s.trackOpen(.dir, result.handle, path) catch unreachable;
    return result;
}

fn dirCreateDirPathOpen(userdata: ?*anyopaque, dir: Dir, path: []const u8, permissions: Dir.Permissions, options: Dir.OpenOptions) Dir.CreateDirPathOpenError!Dir {
    const s = debug(userdata);
    const result = try s.base.vtable.dirCreateDirPathOpen(s.base.userdata, dir, path, permissions, options);
    errdefer s.base.vtable.dirClose(s.base.userdata, (&result)[0..1]);

    s.trackOpen(.dir, result.handle, path) catch unreachable;
    return result;
}

fn dirClose(userdata: ?*anyopaque, dirs: []const Dir) void {
    const s = debug(userdata);
    for (dirs) |dir| {
        s.untrackOpen(dir.handle);
    }
    return s.base.vtable.dirClose(s.base.userdata, dirs);
}

fn dirCreateFile(userdata: ?*anyopaque, dir: Dir, path: []const u8, options: Dir.CreateFileOptions) File.OpenError!File {
    const s = debug(userdata);
    const result = try s.base.vtable.dirCreateFile(s.base.userdata, dir, path, options);
    errdefer s.base.vtable.fileClose(s.base.userdata, (&result)[0..1]);

    s.trackOpen(.file, result.handle, path) catch unreachable;
    return result;
}

fn dirOpenFile(userdata: ?*anyopaque, dir: Dir, path: []const u8, options: Dir.OpenFileOptions) File.OpenError!File {
    const s = debug(userdata);
    const result = try s.base.vtable.dirOpenFile(s.base.userdata, dir, path, options);
    errdefer s.base.vtable.fileClose(s.base.userdata, (&result)[0..1]);

    s.trackOpen(.file, result.handle, path) catch unreachable;
    return result;
}

fn fileClose(userdata: ?*anyopaque, files: []const File) void {
    const s = debug(userdata);
    for (files) |file| {
        s.untrackOpen(file.handle);
    }
    return s.base.vtable.fileClose(s.base.userdata, files);
}

test "DebugIoReportsLeakedFds" {
    const alloc = std.testing.allocator;

    var debugIo = DebugIo.init(std.testing.io, alloc);
    defer debugIo.deinit();
    const testingIo = debugIo.io();

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const file = try tmp.dir.createFile(testingIo, "leaked", .{});
    try std.testing.expectEqual(1, debugIo.openMap.count());
    const err = debugIo.checkNoLeaks();
    try std.testing.expectError(error.FileDescriptorLeak, err);

    file.close(testingIo);
    try debugIo.checkNoLeaks();
}
