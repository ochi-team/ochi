const std = @import("std");
const Io = std.Io;
const Allocator = std.mem.Allocator;
const Dir = Io.Dir;
const File = Io.File;
const net = Io.net;

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
writer: ?*std.Io.Writer,

fn debug(userdata: ?*anyopaque) *DebugIo {
    return @ptrCast(@alignCast(userdata.?));
}

pub fn io(self: *DebugIo) Io {
    return .{
        .userdata = self,
        .vtable = &self.vtable,
    };
}

pub fn init(base: Io, alloc: Allocator, writer: ?*std.Io.Writer) DebugIo {
    var dio: DebugIo = .{
        .base = base,
        .alloc = alloc,
        .openMap = .init(alloc),
        .vtable = base.vtable.*,
        .writer = writer,
    };

    // implemented
    dio.vtable.dirCreateDirPathOpen = dirCreateDirPathOpen;
    dio.vtable.dirOpenDir = dirOpenDir;
    dio.vtable.dirClose = dirClose;
    dio.vtable.dirCreateFile = dirCreateFile;
    dio.vtable.dirOpenFile = dirOpenFile;
    dio.vtable.fileClose = fileClose;

    // proxied
    dio.vtable.crashHandler = crashHandler;
    dio.vtable.async = async;
    dio.vtable.concurrent = concurrent;
    dio.vtable.await = await;
    dio.vtable.cancel = cancel;
    dio.vtable.groupAsync = groupAsync;
    dio.vtable.groupConcurrent = groupConcurrent;
    dio.vtable.groupAwait = groupAwait;
    dio.vtable.groupCancel = groupCancel;
    dio.vtable.recancel = recancel;
    dio.vtable.swapCancelProtection = swapCancelProtection;
    dio.vtable.checkCancel = checkCancel;
    dio.vtable.futexWait = futexWait;
    dio.vtable.futexWaitUncancelable = futexWaitUncancelable;
    dio.vtable.futexWake = futexWake;
    dio.vtable.operate = operate;
    dio.vtable.batchAwaitAsync = batchAwaitAsync;
    dio.vtable.batchAwaitConcurrent = batchAwaitConcurrent;
    dio.vtable.batchCancel = batchCancel;
    dio.vtable.dirCreateDir = dirCreateDir;
    dio.vtable.dirCreateDirPath = dirCreateDirPath;
    dio.vtable.dirStat = dirStat;
    dio.vtable.dirStatFile = dirStatFile;
    dio.vtable.dirAccess = dirAccess;
    dio.vtable.dirCreateFileAtomic = dirCreateFileAtomic;
    dio.vtable.dirRead = dirRead;
    dio.vtable.dirRealPath = dirRealPath;
    dio.vtable.dirRealPathFile = dirRealPathFile;
    dio.vtable.dirDeleteFile = dirDeleteFile;
    dio.vtable.dirDeleteDir = dirDeleteDir;
    dio.vtable.dirRename = dirRename;
    dio.vtable.dirRenamePreserve = dirRenamePreserve;
    dio.vtable.dirSymLink = dirSymLink;
    dio.vtable.dirReadLink = dirReadLink;
    dio.vtable.dirSetOwner = dirSetOwner;
    dio.vtable.dirSetFileOwner = dirSetFileOwner;
    dio.vtable.dirSetPermissions = dirSetPermissions;
    dio.vtable.dirSetFilePermissions = dirSetFilePermissions;
    dio.vtable.dirSetTimestamps = dirSetTimestamps;
    dio.vtable.dirHardLink = dirHardLink;
    dio.vtable.fileStat = fileStat;
    dio.vtable.fileLength = fileLength;
    dio.vtable.fileWritePositional = fileWritePositional;
    dio.vtable.fileWriteFileStreaming = fileWriteFileStreaming;
    dio.vtable.fileWriteFilePositional = fileWriteFilePositional;
    dio.vtable.fileReadPositional = fileReadPositional;
    dio.vtable.fileSeekBy = fileSeekBy;
    dio.vtable.fileSeekTo = fileSeekTo;
    dio.vtable.fileSync = fileSync;
    dio.vtable.fileIsTty = fileIsTty;
    dio.vtable.fileEnableAnsiEscapeCodes = fileEnableAnsiEscapeCodes;
    dio.vtable.fileSupportsAnsiEscapeCodes = fileSupportsAnsiEscapeCodes;
    dio.vtable.fileSetLength = fileSetLength;
    dio.vtable.fileSetOwner = fileSetOwner;
    dio.vtable.fileSetPermissions = fileSetPermissions;
    dio.vtable.fileSetTimestamps = fileSetTimestamps;
    dio.vtable.fileLock = fileLock;
    dio.vtable.fileTryLock = fileTryLock;
    dio.vtable.fileUnlock = fileUnlock;
    dio.vtable.fileDowngradeLock = fileDowngradeLock;
    dio.vtable.fileRealPath = fileRealPath;
    dio.vtable.fileHardLink = fileHardLink;
    dio.vtable.fileMemoryMapCreate = fileMemoryMapCreate;
    dio.vtable.fileMemoryMapDestroy = fileMemoryMapDestroy;
    dio.vtable.fileMemoryMapSetLength = fileMemoryMapSetLength;
    dio.vtable.fileMemoryMapRead = fileMemoryMapRead;
    dio.vtable.fileMemoryMapWrite = fileMemoryMapWrite;
    dio.vtable.processExecutableOpen = processExecutableOpen;
    dio.vtable.processExecutablePath = processExecutablePath;
    dio.vtable.lockStderr = lockStderr;
    dio.vtable.tryLockStderr = tryLockStderr;
    dio.vtable.unlockStderr = unlockStderr;
    dio.vtable.processCurrentPath = processCurrentPath;
    dio.vtable.processSetCurrentDir = processSetCurrentDir;
    dio.vtable.processSetCurrentPath = processSetCurrentPath;
    dio.vtable.processReplace = processReplace;
    dio.vtable.processReplacePath = processReplacePath;
    dio.vtable.processSpawn = processSpawn;
    dio.vtable.processSpawnPath = processSpawnPath;
    dio.vtable.childWait = childWait;
    dio.vtable.childKill = childKill;
    dio.vtable.progressParentFile = progressParentFile;
    dio.vtable.now = now;
    dio.vtable.clockResolution = clockResolution;
    dio.vtable.sleep = sleep;
    dio.vtable.random = random;
    dio.vtable.randomSecure = randomSecure;
    dio.vtable.netListenIp = netListenIp;
    dio.vtable.netAccept = netAccept;
    dio.vtable.netBindIp = netBindIp;
    dio.vtable.netConnectIp = netConnectIp;
    dio.vtable.netListenUnix = netListenUnix;
    dio.vtable.netConnectUnix = netConnectUnix;
    dio.vtable.netSocketCreatePair = netSocketCreatePair;
    dio.vtable.netSend = netSend;
    dio.vtable.netRead = netRead;
    dio.vtable.netWrite = netWrite;
    dio.vtable.netWriteFile = netWriteFile;
    dio.vtable.netClose = netClose;
    dio.vtable.netShutdown = netShutdown;
    dio.vtable.netInterfaceNameResolve = netInterfaceNameResolve;
    dio.vtable.netInterfaceName = netInterfaceName;
    dio.vtable.netLookup = netLookup;
    return dio;
}

pub fn checkNoLeaks(self: *DebugIo) !void {
    if (self.openMap.count() == 0) return;

    const stderr = std.debug.lockStderr(&.{});
    defer std.debug.unlockStderr();

    const termWriter: struct { term: std.Io.Terminal, writer: *std.Io.Writer } = blk: {
        if (self.writer) |w| {
            break :blk .{ .term = .{ .mode = .no_color, .writer = w }, .writer = w };
        } else {
            const terminal = stderr.terminal();
            break :blk .{ .term = terminal, .writer = terminal.writer };
        }
    };

    var it = self.openMap.iterator();
    while (it.next()) |*entry| {
        try termWriter.writer.print("========= io resource leaked ========\nleaked {s} fd={} path={s}\n", .{
            @tagName(entry.value_ptr.kind),
            entry.key_ptr.*,
            entry.value_ptr.path,
        });
        try std.debug.writeStackTrace(&entry.value_ptr.stackTrace, termWriter.term);
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

fn trackOpen(
    self: *DebugIo,
    comptime kind: OpenResource.Kind,
    handle: File.Handle,
    path: []const u8,
) Allocator.Error!void {
    const pathCopy = try self.alloc.dupe(u8, path);
    errdefer self.alloc.free(pathCopy);

    self.openMapMutex.lockUncancelable(self.base);
    defer self.openMapMutex.unlock(self.base);

    // we don't really look for a potentially existing key,
    // the goal is to allocate OpenResource.addrsBuf deeper in the stack not to make it bogus
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

fn crashHandler(userdata: ?*anyopaque) void {
    const s = debug(userdata);
    return s.base.vtable.crashHandler(s.base.userdata);
}

fn async(
    userdata: ?*anyopaque,
    result: []u8,
    result_alignment: std.mem.Alignment,
    context: []const u8,
    context_alignment: std.mem.Alignment,
    start: *const fn (context: *const anyopaque, result: *anyopaque) void,
) ?*Io.AnyFuture {
    const s = debug(userdata);
    return s.base.vtable.async(s.base.userdata, result, result_alignment, context, context_alignment, start);
}

fn concurrent(
    userdata: ?*anyopaque,
    result_len: usize,
    result_alignment: std.mem.Alignment,
    context: []const u8,
    context_alignment: std.mem.Alignment,
    start: *const fn (context: *const anyopaque, result: *anyopaque) void,
) Io.ConcurrentError!*Io.AnyFuture {
    const s = debug(userdata);
    return s.base.vtable.concurrent(s.base.userdata, result_len, result_alignment, context, context_alignment, start);
}

fn await(userdata: ?*anyopaque, any_future: *Io.AnyFuture, result: []u8, result_alignment: std.mem.Alignment) void {
    const s = debug(userdata);
    return s.base.vtable.await(s.base.userdata, any_future, result, result_alignment);
}

fn cancel(userdata: ?*anyopaque, any_future: *Io.AnyFuture, result: []u8, result_alignment: std.mem.Alignment) void {
    const s = debug(userdata);
    return s.base.vtable.cancel(s.base.userdata, any_future, result, result_alignment);
}

fn groupAsync(
    userdata: ?*anyopaque,
    group: *Io.Group,
    context: []const u8,
    context_alignment: std.mem.Alignment,
    start: *const fn (context: *const anyopaque) void,
) void {
    const s = debug(userdata);
    return s.base.vtable.groupAsync(s.base.userdata, group, context, context_alignment, start);
}

fn groupConcurrent(
    userdata: ?*anyopaque,
    group: *Io.Group,
    context: []const u8,
    context_alignment: std.mem.Alignment,
    start: *const fn (context: *const anyopaque) void,
) Io.ConcurrentError!void {
    const s = debug(userdata);
    return s.base.vtable.groupConcurrent(s.base.userdata, group, context, context_alignment, start);
}

fn groupAwait(userdata: ?*anyopaque, group: *Io.Group, token: *anyopaque) Io.Cancelable!void {
    const s = debug(userdata);
    return s.base.vtable.groupAwait(s.base.userdata, group, token);
}

fn groupCancel(userdata: ?*anyopaque, group: *Io.Group, token: *anyopaque) void {
    const s = debug(userdata);
    return s.base.vtable.groupCancel(s.base.userdata, group, token);
}

fn recancel(userdata: ?*anyopaque) void {
    const s = debug(userdata);
    return s.base.vtable.recancel(s.base.userdata);
}

fn swapCancelProtection(userdata: ?*anyopaque, new: Io.CancelProtection) Io.CancelProtection {
    const s = debug(userdata);
    return s.base.vtable.swapCancelProtection(s.base.userdata, new);
}

fn checkCancel(userdata: ?*anyopaque) Io.Cancelable!void {
    const s = debug(userdata);
    return s.base.vtable.checkCancel(s.base.userdata);
}

fn futexWait(userdata: ?*anyopaque, ptr: *const u32, expected: u32, timeout: Io.Timeout) Io.Cancelable!void {
    const s = debug(userdata);
    return s.base.vtable.futexWait(s.base.userdata, ptr, expected, timeout);
}

fn futexWaitUncancelable(userdata: ?*anyopaque, ptr: *const u32, expected: u32) void {
    const s = debug(userdata);
    return s.base.vtable.futexWaitUncancelable(s.base.userdata, ptr, expected);
}

fn futexWake(userdata: ?*anyopaque, ptr: *const u32, max_waiters: u32) void {
    const s = debug(userdata);
    return s.base.vtable.futexWake(s.base.userdata, ptr, max_waiters);
}

fn operate(userdata: ?*anyopaque, operation: Io.Operation) Io.Cancelable!Io.Operation.Result {
    const s = debug(userdata);
    return s.base.vtable.operate(s.base.userdata, operation);
}

fn batchAwaitAsync(userdata: ?*anyopaque, batch: *Io.Batch) Io.Cancelable!void {
    const s = debug(userdata);
    return s.base.vtable.batchAwaitAsync(s.base.userdata, batch);
}

fn batchAwaitConcurrent(userdata: ?*anyopaque, batch: *Io.Batch, timeout: Io.Timeout) Io.Batch.AwaitConcurrentError!void {
    const s = debug(userdata);
    return s.base.vtable.batchAwaitConcurrent(s.base.userdata, batch, timeout);
}

fn batchCancel(userdata: ?*anyopaque, batch: *Io.Batch) void {
    const s = debug(userdata);
    return s.base.vtable.batchCancel(s.base.userdata, batch);
}

fn dirCreateDir(userdata: ?*anyopaque, dir: Dir, path: []const u8, permissions: Dir.Permissions) Dir.CreateDirError!void {
    const s = debug(userdata);
    return s.base.vtable.dirCreateDir(s.base.userdata, dir, path, permissions);
}

fn dirCreateDirPath(userdata: ?*anyopaque, dir: Dir, path: []const u8, permissions: Dir.Permissions) Dir.CreateDirPathError!Dir.CreatePathStatus {
    const s = debug(userdata);
    return s.base.vtable.dirCreateDirPath(s.base.userdata, dir, path, permissions);
}

fn dirStat(userdata: ?*anyopaque, dir: Dir) Dir.StatError!Dir.Stat {
    const s = debug(userdata);
    return s.base.vtable.dirStat(s.base.userdata, dir);
}

fn dirStatFile(userdata: ?*anyopaque, dir: Dir, path: []const u8, options: Dir.StatFileOptions) Dir.StatFileError!File.Stat {
    const s = debug(userdata);
    return s.base.vtable.dirStatFile(s.base.userdata, dir, path, options);
}

fn dirAccess(userdata: ?*anyopaque, dir: Dir, path: []const u8, options: Dir.AccessOptions) Dir.AccessError!void {
    const s = debug(userdata);
    return s.base.vtable.dirAccess(s.base.userdata, dir, path, options);
}

fn dirCreateFileAtomic(userdata: ?*anyopaque, dir: Dir, path: []const u8, options: Dir.CreateFileAtomicOptions) Dir.CreateFileAtomicError!File.Atomic {
    const s = debug(userdata);
    var result = try s.base.vtable.dirCreateFileAtomic(s.base.userdata, dir, path, options);
    errdefer result.deinit(s.base);

    if (result.file_open) {
        s.trackOpen(.file, result.file.handle, path) catch unreachable;
    }
    if (result.close_dir_on_deinit) {
        s.trackOpen(.dir, result.dir.handle, path) catch unreachable;
    }
    return result;
}

fn dirRead(userdata: ?*anyopaque, reader: *Dir.Reader, entries: []Dir.Entry) Dir.Reader.Error!usize {
    const s = debug(userdata);
    return s.base.vtable.dirRead(s.base.userdata, reader, entries);
}

fn dirRealPath(userdata: ?*anyopaque, dir: Dir, out_buffer: []u8) Dir.RealPathError!usize {
    const s = debug(userdata);
    return s.base.vtable.dirRealPath(s.base.userdata, dir, out_buffer);
}

fn dirRealPathFile(userdata: ?*anyopaque, dir: Dir, path_name: []const u8, out_buffer: []u8) Dir.RealPathFileError!usize {
    const s = debug(userdata);
    return s.base.vtable.dirRealPathFile(s.base.userdata, dir, path_name, out_buffer);
}

fn dirDeleteFile(userdata: ?*anyopaque, dir: Dir, path: []const u8) Dir.DeleteFileError!void {
    const s = debug(userdata);
    return s.base.vtable.dirDeleteFile(s.base.userdata, dir, path);
}

fn dirDeleteDir(userdata: ?*anyopaque, dir: Dir, path: []const u8) Dir.DeleteDirError!void {
    const s = debug(userdata);
    return s.base.vtable.dirDeleteDir(s.base.userdata, dir, path);
}

fn dirRename(userdata: ?*anyopaque, old_dir: Dir, old_sub_path: []const u8, new_dir: Dir, new_sub_path: []const u8) Dir.RenameError!void {
    const s = debug(userdata);
    return s.base.vtable.dirRename(s.base.userdata, old_dir, old_sub_path, new_dir, new_sub_path);
}

fn dirRenamePreserve(userdata: ?*anyopaque, old_dir: Dir, old_sub_path: []const u8, new_dir: Dir, new_sub_path: []const u8) Dir.RenamePreserveError!void {
    const s = debug(userdata);
    return s.base.vtable.dirRenamePreserve(s.base.userdata, old_dir, old_sub_path, new_dir, new_sub_path);
}

fn dirSymLink(userdata: ?*anyopaque, dir: Dir, target_path: []const u8, sym_link_path: []const u8, flags: Dir.SymLinkFlags) Dir.SymLinkError!void {
    const s = debug(userdata);
    return s.base.vtable.dirSymLink(s.base.userdata, dir, target_path, sym_link_path, flags);
}

fn dirReadLink(userdata: ?*anyopaque, dir: Dir, sub_path: []const u8, buffer: []u8) Dir.ReadLinkError!usize {
    const s = debug(userdata);
    return s.base.vtable.dirReadLink(s.base.userdata, dir, sub_path, buffer);
}

fn dirSetOwner(userdata: ?*anyopaque, dir: Dir, uid: ?File.Uid, gid: ?File.Gid) Dir.SetOwnerError!void {
    const s = debug(userdata);
    return s.base.vtable.dirSetOwner(s.base.userdata, dir, uid, gid);
}

fn dirSetFileOwner(userdata: ?*anyopaque, dir: Dir, path: []const u8, uid: ?File.Uid, gid: ?File.Gid, options: Dir.SetFileOwnerOptions) Dir.SetFileOwnerError!void {
    const s = debug(userdata);
    return s.base.vtable.dirSetFileOwner(s.base.userdata, dir, path, uid, gid, options);
}

fn dirSetPermissions(userdata: ?*anyopaque, dir: Dir, permissions: Dir.Permissions) Dir.SetPermissionsError!void {
    const s = debug(userdata);
    return s.base.vtable.dirSetPermissions(s.base.userdata, dir, permissions);
}

fn dirSetFilePermissions(userdata: ?*anyopaque, dir: Dir, path: []const u8, permissions: File.Permissions, options: Dir.SetFilePermissionsOptions) Dir.SetFilePermissionsError!void {
    const s = debug(userdata);
    return s.base.vtable.dirSetFilePermissions(s.base.userdata, dir, path, permissions, options);
}

fn dirSetTimestamps(userdata: ?*anyopaque, dir: Dir, path: []const u8, options: Dir.SetTimestampsOptions) Dir.SetTimestampsError!void {
    const s = debug(userdata);
    return s.base.vtable.dirSetTimestamps(s.base.userdata, dir, path, options);
}

fn dirHardLink(userdata: ?*anyopaque, old_dir: Dir, old_sub_path: []const u8, new_dir: Dir, new_sub_path: []const u8, options: Dir.HardLinkOptions) Dir.HardLinkError!void {
    const s = debug(userdata);
    return s.base.vtable.dirHardLink(s.base.userdata, old_dir, old_sub_path, new_dir, new_sub_path, options);
}

fn fileStat(userdata: ?*anyopaque, file: File) File.StatError!File.Stat {
    const s = debug(userdata);
    return s.base.vtable.fileStat(s.base.userdata, file);
}

fn fileLength(userdata: ?*anyopaque, file: File) File.LengthError!u64 {
    const s = debug(userdata);
    return s.base.vtable.fileLength(s.base.userdata, file);
}

fn fileWritePositional(userdata: ?*anyopaque, file: File, header: []const u8, data: []const []const u8, splat: usize, offset: u64) File.WritePositionalError!usize {
    const s = debug(userdata);
    return s.base.vtable.fileWritePositional(s.base.userdata, file, header, data, splat, offset);
}

fn fileWriteFileStreaming(userdata: ?*anyopaque, file: File, header: []const u8, reader: *Io.File.Reader, limit: Io.Limit) File.Writer.WriteFileError!usize {
    const s = debug(userdata);
    return s.base.vtable.fileWriteFileStreaming(s.base.userdata, file, header, reader, limit);
}

fn fileWriteFilePositional(userdata: ?*anyopaque, file: File, header: []const u8, reader: *Io.File.Reader, limit: Io.Limit, offset: u64) File.WriteFilePositionalError!usize {
    const s = debug(userdata);
    return s.base.vtable.fileWriteFilePositional(s.base.userdata, file, header, reader, limit, offset);
}

fn fileReadPositional(userdata: ?*anyopaque, file: File, data: []const []u8, offset: u64) File.ReadPositionalError!usize {
    const s = debug(userdata);
    return s.base.vtable.fileReadPositional(s.base.userdata, file, data, offset);
}

fn fileSeekBy(userdata: ?*anyopaque, file: File, relative_offset: i64) File.SeekError!void {
    const s = debug(userdata);
    return s.base.vtable.fileSeekBy(s.base.userdata, file, relative_offset);
}

fn fileSeekTo(userdata: ?*anyopaque, file: File, absolute_offset: u64) File.SeekError!void {
    const s = debug(userdata);
    return s.base.vtable.fileSeekTo(s.base.userdata, file, absolute_offset);
}

fn fileSync(userdata: ?*anyopaque, file: File) File.SyncError!void {
    const s = debug(userdata);
    return s.base.vtable.fileSync(s.base.userdata, file);
}

fn fileIsTty(userdata: ?*anyopaque, file: File) Io.Cancelable!bool {
    const s = debug(userdata);
    return s.base.vtable.fileIsTty(s.base.userdata, file);
}

fn fileEnableAnsiEscapeCodes(userdata: ?*anyopaque, file: File) File.EnableAnsiEscapeCodesError!void {
    const s = debug(userdata);
    return s.base.vtable.fileEnableAnsiEscapeCodes(s.base.userdata, file);
}

fn fileSupportsAnsiEscapeCodes(userdata: ?*anyopaque, file: File) Io.Cancelable!bool {
    const s = debug(userdata);
    return s.base.vtable.fileSupportsAnsiEscapeCodes(s.base.userdata, file);
}

fn fileSetLength(userdata: ?*anyopaque, file: File, len: u64) File.SetLengthError!void {
    const s = debug(userdata);
    return s.base.vtable.fileSetLength(s.base.userdata, file, len);
}

fn fileSetOwner(userdata: ?*anyopaque, file: File, uid: ?File.Uid, gid: ?File.Gid) File.SetOwnerError!void {
    const s = debug(userdata);
    return s.base.vtable.fileSetOwner(s.base.userdata, file, uid, gid);
}

fn fileSetPermissions(userdata: ?*anyopaque, file: File, permissions: File.Permissions) File.SetPermissionsError!void {
    const s = debug(userdata);
    return s.base.vtable.fileSetPermissions(s.base.userdata, file, permissions);
}

fn fileSetTimestamps(userdata: ?*anyopaque, file: File, options: File.SetTimestampsOptions) File.SetTimestampsError!void {
    const s = debug(userdata);
    return s.base.vtable.fileSetTimestamps(s.base.userdata, file, options);
}

fn fileLock(userdata: ?*anyopaque, file: File, lock: File.Lock) File.LockError!void {
    const s = debug(userdata);
    return s.base.vtable.fileLock(s.base.userdata, file, lock);
}

fn fileTryLock(userdata: ?*anyopaque, file: File, lock: File.Lock) File.LockError!bool {
    const s = debug(userdata);
    return s.base.vtable.fileTryLock(s.base.userdata, file, lock);
}

fn fileUnlock(userdata: ?*anyopaque, file: File) void {
    const s = debug(userdata);
    return s.base.vtable.fileUnlock(s.base.userdata, file);
}

fn fileDowngradeLock(userdata: ?*anyopaque, file: File) File.DowngradeLockError!void {
    const s = debug(userdata);
    return s.base.vtable.fileDowngradeLock(s.base.userdata, file);
}

fn fileRealPath(userdata: ?*anyopaque, file: File, out_buffer: []u8) File.RealPathError!usize {
    const s = debug(userdata);
    return s.base.vtable.fileRealPath(s.base.userdata, file, out_buffer);
}

fn fileHardLink(userdata: ?*anyopaque, file: File, dir: Dir, path: []const u8, options: File.HardLinkOptions) File.HardLinkError!void {
    const s = debug(userdata);
    return s.base.vtable.fileHardLink(s.base.userdata, file, dir, path, options);
}

fn fileMemoryMapCreate(userdata: ?*anyopaque, file: File, options: File.MemoryMap.CreateOptions) File.MemoryMap.CreateError!File.MemoryMap {
    const s = debug(userdata);
    return s.base.vtable.fileMemoryMapCreate(s.base.userdata, file, options);
}

fn fileMemoryMapDestroy(userdata: ?*anyopaque, memory_map: *File.MemoryMap) void {
    const s = debug(userdata);
    return s.base.vtable.fileMemoryMapDestroy(s.base.userdata, memory_map);
}

fn fileMemoryMapSetLength(userdata: ?*anyopaque, memory_map: *File.MemoryMap, len: usize) File.MemoryMap.SetLengthError!void {
    const s = debug(userdata);
    return s.base.vtable.fileMemoryMapSetLength(s.base.userdata, memory_map, len);
}

fn fileMemoryMapRead(userdata: ?*anyopaque, memory_map: *File.MemoryMap) File.ReadPositionalError!void {
    const s = debug(userdata);
    return s.base.vtable.fileMemoryMapRead(s.base.userdata, memory_map);
}

fn fileMemoryMapWrite(userdata: ?*anyopaque, memory_map: *File.MemoryMap) File.WritePositionalError!void {
    const s = debug(userdata);
    return s.base.vtable.fileMemoryMapWrite(s.base.userdata, memory_map);
}

fn processExecutableOpen(userdata: ?*anyopaque, options: Dir.OpenFileOptions) std.process.OpenExecutableError!File {
    const s = debug(userdata);
    const result = try s.base.vtable.processExecutableOpen(s.base.userdata, options);
    errdefer s.base.vtable.fileClose(s.base.userdata, (&result)[0..1]);

    s.trackOpen(.file, result.handle, "<process executable>") catch unreachable;
    return result;
}

fn processExecutablePath(userdata: ?*anyopaque, buffer: []u8) std.process.ExecutablePathError!usize {
    const s = debug(userdata);
    return s.base.vtable.processExecutablePath(s.base.userdata, buffer);
}

fn lockStderr(userdata: ?*anyopaque, mode: ?Io.Terminal.Mode) Io.Cancelable!Io.LockedStderr {
    const s = debug(userdata);
    return s.base.vtable.lockStderr(s.base.userdata, mode);
}

fn tryLockStderr(userdata: ?*anyopaque, mode: ?Io.Terminal.Mode) Io.Cancelable!?Io.LockedStderr {
    const s = debug(userdata);
    return s.base.vtable.tryLockStderr(s.base.userdata, mode);
}

fn unlockStderr(userdata: ?*anyopaque) void {
    const s = debug(userdata);
    return s.base.vtable.unlockStderr(s.base.userdata);
}

fn processCurrentPath(userdata: ?*anyopaque, buffer: []u8) std.process.CurrentPathError!usize {
    const s = debug(userdata);
    return s.base.vtable.processCurrentPath(s.base.userdata, buffer);
}

fn processSetCurrentDir(userdata: ?*anyopaque, dir: Dir) std.process.SetCurrentDirError!void {
    const s = debug(userdata);
    return s.base.vtable.processSetCurrentDir(s.base.userdata, dir);
}

fn processSetCurrentPath(userdata: ?*anyopaque, path: []const u8) std.process.SetCurrentPathError!void {
    const s = debug(userdata);
    return s.base.vtable.processSetCurrentPath(s.base.userdata, path);
}

fn processReplace(userdata: ?*anyopaque, options: std.process.ReplaceOptions) std.process.ReplaceError {
    const s = debug(userdata);
    return s.base.vtable.processReplace(s.base.userdata, options);
}

fn processReplacePath(userdata: ?*anyopaque, dir: Dir, options: std.process.ReplaceOptions) std.process.ReplaceError {
    const s = debug(userdata);
    return s.base.vtable.processReplacePath(s.base.userdata, dir, options);
}

fn processSpawn(userdata: ?*anyopaque, options: std.process.SpawnOptions) std.process.SpawnError!std.process.Child {
    const s = debug(userdata);
    return s.base.vtable.processSpawn(s.base.userdata, options);
}

fn processSpawnPath(userdata: ?*anyopaque, dir: Dir, options: std.process.SpawnOptions) std.process.SpawnError!std.process.Child {
    const s = debug(userdata);
    return s.base.vtable.processSpawnPath(s.base.userdata, dir, options);
}

fn childWait(userdata: ?*anyopaque, child: *std.process.Child) std.process.Child.WaitError!std.process.Child.Term {
    const s = debug(userdata);
    return s.base.vtable.childWait(s.base.userdata, child);
}

fn childKill(userdata: ?*anyopaque, child: *std.process.Child) void {
    const s = debug(userdata);
    return s.base.vtable.childKill(s.base.userdata, child);
}

fn progressParentFile(userdata: ?*anyopaque) std.Progress.ParentFileError!File {
    const s = debug(userdata);
    const result = try s.base.vtable.progressParentFile(s.base.userdata);
    errdefer s.base.vtable.fileClose(s.base.userdata, (&result)[0..1]);

    s.trackOpen(.file, result.handle, "<progress parent>") catch unreachable;
    return result;
}

fn now(userdata: ?*anyopaque, clock: Io.Clock) Io.Timestamp {
    const s = debug(userdata);
    return s.base.vtable.now(s.base.userdata, clock);
}

fn clockResolution(userdata: ?*anyopaque, clock: Io.Clock) Io.Clock.ResolutionError!Io.Duration {
    const s = debug(userdata);
    return s.base.vtable.clockResolution(s.base.userdata, clock);
}

fn sleep(userdata: ?*anyopaque, timeout: Io.Timeout) Io.Cancelable!void {
    const s = debug(userdata);
    return s.base.vtable.sleep(s.base.userdata, timeout);
}

fn random(userdata: ?*anyopaque, buffer: []u8) void {
    const s = debug(userdata);
    return s.base.vtable.random(s.base.userdata, buffer);
}

fn randomSecure(userdata: ?*anyopaque, buffer: []u8) Io.RandomSecureError!void {
    const s = debug(userdata);
    return s.base.vtable.randomSecure(s.base.userdata, buffer);
}

fn netListenIp(userdata: ?*anyopaque, address: *const net.IpAddress, options: net.IpAddress.ListenOptions) net.IpAddress.ListenError!net.Socket {
    const s = debug(userdata);
    return s.base.vtable.netListenIp(s.base.userdata, address, options);
}

fn netAccept(userdata: ?*anyopaque, server: net.Socket.Handle, options: net.Server.AcceptOptions) net.Server.AcceptError!net.Socket {
    const s = debug(userdata);
    return s.base.vtable.netAccept(s.base.userdata, server, options);
}

fn netBindIp(userdata: ?*anyopaque, address: *const net.IpAddress, options: net.IpAddress.BindOptions) net.IpAddress.BindError!net.Socket {
    const s = debug(userdata);
    return s.base.vtable.netBindIp(s.base.userdata, address, options);
}

fn netConnectIp(userdata: ?*anyopaque, address: *const net.IpAddress, options: net.IpAddress.ConnectOptions) net.IpAddress.ConnectError!net.Socket {
    const s = debug(userdata);
    return s.base.vtable.netConnectIp(s.base.userdata, address, options);
}

fn netListenUnix(userdata: ?*anyopaque, address: *const net.UnixAddress, options: net.UnixAddress.ListenOptions) net.UnixAddress.ListenError!net.Socket.Handle {
    const s = debug(userdata);
    return s.base.vtable.netListenUnix(s.base.userdata, address, options);
}

fn netConnectUnix(userdata: ?*anyopaque, address: *const net.UnixAddress) net.UnixAddress.ConnectError!net.Socket.Handle {
    const s = debug(userdata);
    return s.base.vtable.netConnectUnix(s.base.userdata, address);
}

fn netSocketCreatePair(userdata: ?*anyopaque, options: net.Socket.CreatePairOptions) net.Socket.CreatePairError![2]net.Socket {
    const s = debug(userdata);
    return s.base.vtable.netSocketCreatePair(s.base.userdata, options);
}

fn netSend(userdata: ?*anyopaque, handle: net.Socket.Handle, messages: []net.OutgoingMessage, flags: net.SendFlags) struct { ?net.Socket.SendError, usize } {
    const s = debug(userdata);
    return s.base.vtable.netSend(s.base.userdata, handle, messages, flags);
}

fn netRead(userdata: ?*anyopaque, src: net.Socket.Handle, data: [][]u8) net.Stream.Reader.Error!usize {
    const s = debug(userdata);
    return s.base.vtable.netRead(s.base.userdata, src, data);
}

fn netWrite(userdata: ?*anyopaque, dest: net.Socket.Handle, header: []const u8, data: []const []const u8, splat: usize) net.Stream.Writer.Error!usize {
    const s = debug(userdata);
    return s.base.vtable.netWrite(s.base.userdata, dest, header, data, splat);
}

fn netWriteFile(userdata: ?*anyopaque, dest: net.Socket.Handle, header: []const u8, reader: *Io.File.Reader, limit: Io.Limit) net.Stream.Writer.WriteFileError!usize {
    const s = debug(userdata);
    return s.base.vtable.netWriteFile(s.base.userdata, dest, header, reader, limit);
}

fn netClose(userdata: ?*anyopaque, handle: []const net.Socket.Handle) void {
    const s = debug(userdata);
    return s.base.vtable.netClose(s.base.userdata, handle);
}

fn netShutdown(userdata: ?*anyopaque, handle: net.Socket.Handle, how: net.ShutdownHow) net.ShutdownError!void {
    const s = debug(userdata);
    return s.base.vtable.netShutdown(s.base.userdata, handle, how);
}

fn netInterfaceNameResolve(userdata: ?*anyopaque, name: *const net.Interface.Name) net.Interface.Name.ResolveError!net.Interface {
    const s = debug(userdata);
    return s.base.vtable.netInterfaceNameResolve(s.base.userdata, name);
}

fn netInterfaceName(userdata: ?*anyopaque, interface: net.Interface) net.Interface.NameError!net.Interface.Name {
    const s = debug(userdata);
    return s.base.vtable.netInterfaceName(s.base.userdata, interface);
}

fn netLookup(userdata: ?*anyopaque, host_name: net.HostName, results: *std.Io.Queue(net.HostName.LookupResult), options: net.HostName.LookupOptions) net.HostName.LookupError!void {
    const s = debug(userdata);
    return s.base.vtable.netLookup(s.base.userdata, host_name, results, options);
}

test "DebugIoReportsLeakedFds" {
    const alloc = std.testing.allocator;

    var buf: [4096]u8 = undefined;
    var w = std.Io.Writer.fixed(&buf);

    var debugIo = DebugIo.init(std.testing.io, alloc, &w);
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

    try std.testing.expect(w.buffered().len > 0);
}
