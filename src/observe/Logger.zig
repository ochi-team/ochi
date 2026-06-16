const std = @import("std");

const logz = @import("logz");

pub const msgField = "msg";
pub const timestampField = "timestamp";

pub fn log(comptime level: std.log.Level, comptime msg: []const u8, args: anytype) void {
    var e = switch (level) {
        .debug => logz.debug(),
        .info => logz.info(),
        .warn => logz.warn(),
        .err => logz.err(),
    };

    e = e.string(msgField, msg);
    inline for (std.meta.fields(@TypeOf(args))) |field| {
        e = logField(e, field.name, @field(args, field.name));
    }
    e.log();
}

fn logField(e: logz.Logger, comptime key: []const u8, value: anytype) logz.Logger {
    const T = @TypeOf(value);

    switch (@typeInfo(T)) {
        .int, .comptime_int => return e.int(key, value),
        .float, .comptime_float => return e.float(key, value),
        .bool => return e.boolean(key, value),
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

test "Log accepts structured fields" {
    log(.debug, "do doing", .{
        .key = "value",
        .count = 1,
        .ratio = 1.5,
        .ok = true,
    });
}
