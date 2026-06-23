const std = @import("std");
const builtin = @import("builtin");

const logz = @import("logz");

pub const msgField = "msg";
pub const timestampField = "timestamp";
pub const Level = logz.Level;

pub const Instance = struct {
    pool: *logz.Pool,

    pub fn init(io: std.Io, allocator: std.mem.Allocator, config: logz.Config) !Instance {
        return .{
            .pool = try logz.Pool.init(io, allocator, config),
        };
    }

    pub fn deinit(self: *Instance) void {
        self.pool.deinit();
    }

    pub fn log(self: *Instance, comptime level: std.log.Level, comptime msg: []const u8, args: anytype) void {
        logWith(loggerFor(self.pool, level), msg, args);
    }
};

pub fn levelFromBuildMode() logz.Level {
    if (builtin.is_test) return .None;
    if (builtin.mode == .Debug) return .Debug;
    return .Info;
}

var done: bool = false;

pub fn setup(io: std.Io, allocator: std.mem.Allocator, config: logz.Config) !void {
    if (done) {
        std.debug.assert(!done);
    }
    done = true;
    try logz.setup(io, allocator, config);
}

pub fn deinit() void {
    logz.deinit();
    done = false;
}

pub fn log(comptime level: std.log.Level, comptime msg: []const u8, args: anytype) void {
    logWith(loggerForGlobal(level), msg, args);
}

pub fn logWithDiagnostic(
    comptime level: std.log.Level,
    comptime msg: []const u8,
    args: anytype,
    diag: *const Diagnostic,
) void {
    var e = loggerForGlobal(level);
    for (0..diag.len) |i| {
        e = e.string(diag.data[i].key, diag.data[i].value);
    }
    logWith(e, msg, args);
}

fn logWith(e0: logz.Logger, comptime msg: []const u8, args: anytype) void {
    var e = e0;
    e = e.string(msgField, msg);
    inline for (std.meta.fields(@TypeOf(args))) |field| {
        e = logField(e, field.name, @field(args, field.name));
    }
    e.log();
}

fn loggerForGlobal(comptime level: std.log.Level) logz.Logger {
    return switch (level) {
        .debug => logz.debug(),
        .info => logz.info(),
        .warn => logz.warn(),
        .err => logz.err(),
    };
}

fn loggerFor(pool: *logz.Pool, comptime level: std.log.Level) logz.Logger {
    return switch (level) {
        .debug => pool.debug(),
        .info => pool.info(),
        .warn => pool.warn(),
        .err => pool.err(),
    };
}

fn logField(e: logz.Logger, comptime key: []const u8, value: anytype) logz.Logger {
    const T = @TypeOf(value);

    switch (@typeInfo(T)) {
        .int, .comptime_int => return e.int(key, value),
        .float, .comptime_float => return e.float(key, value),
        .bool => return e.boolean(key, value),
        .error_set => return e.errK(key, value),
        .pointer => |ptr| switch (ptr.size) {
            .slice => if (ptr.child == u8) {
                return e.string(key, value);
            },
            .one => switch (@typeInfo(ptr.child)) {
                .array => |arr| if (arr.child == u8) {
                    return e.string(key, value);
                },
                else => {},
            },
            else => {},
        },
        .array => |arr| if (arr.child == u8) {
            return e.string(key, &value);
        },
        else => {},
    }

    @compileError("unsupported log field type for '" ++ key ++ "': " ++ @typeName(T));
}

const diagnosticBufferSize = 16;
const Field = struct { key: []const u8, value: []const u8 };

pub const Diagnostic = struct {
    len: u8 = 0,
    data: [diagnosticBufferSize]Field = undefined,

    pub fn set(diag: *Diagnostic, f: Field) void {
        if (diag.len >= diagnosticBufferSize) {
            log(.err, "diagnostic buffer is full, extend or fix it's usage", .{});
            return;
        }

        diag.data[diag.len] = f;
        diag.len += 1;
    }
};

test "Log accepts structured fields" {
    log(.debug, "do doing", .{
        .key = "value",
        .count = 1,
        .ratio = 1.5,
        .ok = true,
    });
}
