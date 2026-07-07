const std = @import("std");
const Io = std.Io;
const Dir = Io.Dir;
const File = Io.File;

pub const FailingIo = struct {
    base: Io,
    op_index: usize = 0,
    fail_index: usize = std.math.maxInt(usize),
    has_induced_failure: bool = false,
    vtable: Io.VTable,

    pub const Config = struct {
        fail_index: usize = std.math.maxInt(usize),
    };

    pub fn init(base: Io, config: Config) FailingIo {
        var fio = FailingIo{
            .base = base,
            .fail_index = config.fail_index,
            .vtable = base.vtable.*,
        };
        fio.vtable.dirCreateDir = dirCreateDir;
        fio.vtable.dirCreateDirPath = dirCreateDirPath;
        fio.vtable.dirCreateDirPathOpen = dirCreateDirPathOpen;
        fio.vtable.dirOpenDir = dirOpenDir;
        fio.vtable.dirStat = dirStat;
        fio.vtable.dirStatFile = dirStatFile;
        fio.vtable.dirAccess = dirAccess;
        fio.vtable.dirCreateFile = dirCreateFile;
        fio.vtable.dirCreateFileAtomic = dirCreateFileAtomic;
        fio.vtable.dirOpenFile = dirOpenFile;
        fio.vtable.dirRead = dirRead;
        fio.vtable.dirRealPath = dirRealPath;
        fio.vtable.dirRealPathFile = dirRealPathFile;
        fio.vtable.dirDeleteFile = dirDeleteFile;
        fio.vtable.dirDeleteDir = dirDeleteDir;
        fio.vtable.dirRename = dirRename;
        fio.vtable.dirRenamePreserve = dirRenamePreserve;
        fio.vtable.fileStat = fileStat;
        fio.vtable.fileLength = fileLength;
        fio.vtable.fileWritePositional = fileWritePositional;
        fio.vtable.fileReadPositional = fileReadPositional;
        fio.vtable.fileSync = fileSync;
        fio.vtable.fileSetLength = fileSetLength;
        fio.vtable.fileRealPath = fileRealPath;
        return fio;
    }

    pub fn io(fio: *FailingIo) Io {
        return .{
            .userdata = fio,
            .vtable = &fio.vtable,
        };
    }

    fn state(userdata: ?*anyopaque) *FailingIo {
        return @ptrCast(@alignCast(userdata.?));
    }

    fn fail(s: *FailingIo) bool {
        if (s.op_index == s.fail_index) {
            s.has_induced_failure = true;
            return true;
        }
        s.op_index += 1;
        return false;
    }

    fn dirCreateDir(userdata: ?*anyopaque, dir: Dir, path: []const u8, permissions: Dir.Permissions) Dir.CreateDirError!void {
        const s = state(userdata);
        if (s.fail()) return error.NoSpaceLeft;
        return s.base.vtable.dirCreateDir(s.base.userdata, dir, path, permissions);
    }

    fn dirCreateDirPath(userdata: ?*anyopaque, dir: Dir, path: []const u8, permissions: Dir.Permissions) Dir.CreateDirPathError!Dir.CreatePathStatus {
        const s = state(userdata);
        if (s.fail()) return error.NoSpaceLeft;
        return s.base.vtable.dirCreateDirPath(s.base.userdata, dir, path, permissions);
    }

    fn dirCreateDirPathOpen(userdata: ?*anyopaque, dir: Dir, path: []const u8, permissions: Dir.Permissions, options: Dir.OpenOptions) Dir.CreateDirPathOpenError!Dir {
        const s = state(userdata);
        if (s.fail()) return error.ProcessFdQuotaExceeded;
        return s.base.vtable.dirCreateDirPathOpen(s.base.userdata, dir, path, permissions, options);
    }

    fn dirOpenDir(userdata: ?*anyopaque, dir: Dir, path: []const u8, options: Dir.OpenOptions) Dir.OpenError!Dir {
        const s = state(userdata);
        if (s.fail()) return error.ProcessFdQuotaExceeded;
        return s.base.vtable.dirOpenDir(s.base.userdata, dir, path, options);
    }

    fn dirStat(userdata: ?*anyopaque, dir: Dir) Dir.StatError!Dir.Stat {
        const s = state(userdata);
        if (s.fail()) return error.AccessDenied;
        return s.base.vtable.dirStat(s.base.userdata, dir);
    }

    fn dirStatFile(userdata: ?*anyopaque, dir: Dir, path: []const u8, options: Dir.StatFileOptions) Dir.StatFileError!File.Stat {
        const s = state(userdata);
        if (s.fail()) return error.AccessDenied;
        return s.base.vtable.dirStatFile(s.base.userdata, dir, path, options);
    }

    fn dirAccess(userdata: ?*anyopaque, dir: Dir, path: []const u8, options: Dir.AccessOptions) Dir.AccessError!void {
        const s = state(userdata);
        if (s.fail()) return error.InputOutput;
        return s.base.vtable.dirAccess(s.base.userdata, dir, path, options);
    }

    fn dirCreateFile(userdata: ?*anyopaque, dir: Dir, path: []const u8, options: Dir.CreateFileOptions) File.OpenError!File {
        const s = state(userdata);
        if (s.fail()) return error.ProcessFdQuotaExceeded;
        return s.base.vtable.dirCreateFile(s.base.userdata, dir, path, options);
    }

    fn dirCreateFileAtomic(userdata: ?*anyopaque, dir: Dir, path: []const u8, options: Dir.CreateFileAtomicOptions) Dir.CreateFileAtomicError!File.Atomic {
        const s = state(userdata);
        if (s.fail()) return error.ProcessFdQuotaExceeded;
        return s.base.vtable.dirCreateFileAtomic(s.base.userdata, dir, path, options);
    }

    fn dirOpenFile(userdata: ?*anyopaque, dir: Dir, path: []const u8, options: Dir.OpenFileOptions) File.OpenError!File {
        const s = state(userdata);
        if (s.fail()) return error.ProcessFdQuotaExceeded;
        return s.base.vtable.dirOpenFile(s.base.userdata, dir, path, options);
    }

    fn dirRead(userdata: ?*anyopaque, reader: *Dir.Reader, entries: []Dir.Entry) Dir.Reader.Error!usize {
        const s = state(userdata);
        if (s.fail()) return error.AccessDenied;
        return s.base.vtable.dirRead(s.base.userdata, reader, entries);
    }

    fn dirRealPath(userdata: ?*anyopaque, dir: Dir, out_buffer: []u8) Dir.RealPathError!usize {
        const s = state(userdata);
        if (s.fail()) return error.InputOutput;
        return s.base.vtable.dirRealPath(s.base.userdata, dir, out_buffer);
    }

    fn dirRealPathFile(userdata: ?*anyopaque, dir: Dir, path: []const u8, out_buffer: []u8) Dir.RealPathFileError!usize {
        const s = state(userdata);
        if (s.fail()) return error.InputOutput;
        return s.base.vtable.dirRealPathFile(s.base.userdata, dir, path, out_buffer);
    }

    fn dirDeleteFile(userdata: ?*anyopaque, dir: Dir, path: []const u8) Dir.DeleteFileError!void {
        const s = state(userdata);
        if (s.fail()) return error.AccessDenied;
        return s.base.vtable.dirDeleteFile(s.base.userdata, dir, path);
    }

    fn dirDeleteDir(userdata: ?*anyopaque, dir: Dir, path: []const u8) Dir.DeleteDirError!void {
        const s = state(userdata);
        if (s.fail()) return error.AccessDenied;
        return s.base.vtable.dirDeleteDir(s.base.userdata, dir, path);
    }

    fn dirRename(userdata: ?*anyopaque, old_dir: Dir, old_path: []const u8, new_dir: Dir, new_path: []const u8) Dir.RenameError!void {
        const s = state(userdata);
        if (s.fail()) return error.NoSpaceLeft;
        return s.base.vtable.dirRename(s.base.userdata, old_dir, old_path, new_dir, new_path);
    }

    fn dirRenamePreserve(userdata: ?*anyopaque, old_dir: Dir, old_path: []const u8, new_dir: Dir, new_path: []const u8) Dir.RenamePreserveError!void {
        const s = state(userdata);
        if (s.fail()) return error.NoSpaceLeft;
        return s.base.vtable.dirRenamePreserve(s.base.userdata, old_dir, old_path, new_dir, new_path);
    }

    fn fileStat(userdata: ?*anyopaque, file: File) File.StatError!File.Stat {
        const s = state(userdata);
        if (s.fail()) return error.AccessDenied;
        return s.base.vtable.fileStat(s.base.userdata, file);
    }

    fn fileLength(userdata: ?*anyopaque, file: File) File.LengthError!u64 {
        const s = state(userdata);
        if (s.fail()) return error.AccessDenied;
        return s.base.vtable.fileLength(s.base.userdata, file);
    }

    fn fileWritePositional(userdata: ?*anyopaque, file: File, header: []const u8, data: []const []const u8, splat: usize, offset: u64) File.WritePositionalError!usize {
        const s = state(userdata);
        if (s.fail()) return error.NoSpaceLeft;
        return s.base.vtable.fileWritePositional(s.base.userdata, file, header, data, splat, offset);
    }

    fn fileReadPositional(userdata: ?*anyopaque, file: File, data: []const []u8, offset: u64) File.ReadPositionalError!usize {
        const s = state(userdata);
        if (s.fail()) return error.InputOutput;
        return s.base.vtable.fileReadPositional(s.base.userdata, file, data, offset);
    }

    fn fileSync(userdata: ?*anyopaque, file: File) File.SyncError!void {
        const s = state(userdata);
        if (s.fail()) return error.InputOutput;
        return s.base.vtable.fileSync(s.base.userdata, file);
    }

    fn fileSetLength(userdata: ?*anyopaque, file: File, len: u64) File.SetLengthError!void {
        const s = state(userdata);
        if (s.fail()) return error.InputOutput;
        return s.base.vtable.fileSetLength(s.base.userdata, file, len);
    }

    fn fileRealPath(userdata: ?*anyopaque, file: File, out_buffer: []u8) File.RealPathError!usize {
        const s = state(userdata);
        if (s.fail()) return error.InputOutput;
        return s.base.vtable.fileRealPath(s.base.userdata, file, out_buffer);
    }
};

pub fn checkAllIoFailures(
    base_io: Io,
    comptime test_fn: anytype,
    extra_args: CheckAllIoFailuresExtraArgs(@TypeOf(test_fn)),
) !void {
    const needed_op_count = x: {
        var failing_io = FailingIo.init(base_io, .{});
        try @call(.auto, test_fn, .{failing_io.io()} ++ extra_args);
        break :x failing_io.op_index;
    };

    for (0..needed_op_count) |fail_index| {
        var failing_io = FailingIo.init(base_io, .{ .fail_index = fail_index });

        if (@call(.auto, test_fn, .{failing_io.io()} ++ extra_args)) |_| {
            if (failing_io.has_induced_failure) {
                return error.SwallowedIoError;
            } else {
                return error.NondeterministicIoUsage;
            }
        } else |err| switch (err) {
            error.ProcessFdQuotaExceeded,
            error.SystemFdQuotaExceeded,
            error.NoSpaceLeft,
            error.InputOutput,
            error.AccessDenied,
            error.ReadFailed,
            error.WriteFailed,
            => {},
            else => |e| return e,
        }
    }
}

fn CheckAllIoFailuresExtraArgs(comptime TestFn: type) type {
    switch (@typeInfo(@typeInfo(TestFn).@"fn".return_type.?)) {
        .error_union => |info| {
            if (info.payload != void) {
                @compileError("Return type must be !void");
            }
        },
        else => @compileError("Return type must be !void"),
    }

    const ArgsTuple = std.meta.ArgsTuple(TestFn);

    const fields = @typeInfo(ArgsTuple).@"struct".fields;
    if (fields.len == 0 or fields[0].type != Io) {
        @compileError("The provided function must have an " ++ @typeName(Io) ++ " as its first argument");
    }

    var extra_args: [fields.len - 1]type = undefined;
    for (&extra_args, fields[1..]) |*arg, field| {
        arg.* = field.type;
    }

    return @Tuple(&extra_args);
}
