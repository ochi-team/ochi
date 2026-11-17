const std = @import("std");
const assert = std.debug.assert;

/// parses the CLI args according to its types,
/// returns borrowed values from args.
/// supports only strings, only required arguments.
/// contract:
/// - only string values are supported
/// - keys, values length are limited to 250
/// - destination type must be a struct
/// - all the fields are required
pub fn parse(args: []const []const u8, T: type) !T {
    assert(@typeInfo(T) == .@"struct");

    var res: T = undefined;

    inline for (std.meta.fields(T)) |field| {
        if (comptime !std.mem.containsAtLeast(type, &.{ []u8, []const u8 }, 1, &.{field.type})) {
            @compileError("Options.parse supports only string fields");
        }

        var prefixedNameBuf: [256]u8 = undefined;
        @memcpy(prefixedNameBuf[0..2], "--");
        @memcpy(prefixedNameBuf[2 .. 2 + field.name.len], field.name);

        const i = findKey(args, prefixedNameBuf[0 .. 2 + field.name.len]) orelse return error.MissingKey;
        const value = args[i + 1];
        @field(res, field.name) = value;
        if (value.len == 0) return error.InvalidArguments;
    }

    return res;
}

fn findKey(args: []const []const u8, name: []const u8) ?usize {
    for (args, 0..) |arg, i| {
        if (std.mem.eql(u8, arg, name)) {
            return i;
        }
    }

    return null;
}

test "options require explicit data" {
    const testing = std.testing;

    const OptType = struct {
        url: []const u8,
        run_dir: []const u8,
    };

    try testing.expectError(error.MissingKey, parse(&.{}, OptType));
    try testing.expectError(error.MissingKey, parse(&.{ "--url", "http://localhost" }, OptType));
    try testing.expectEqualDeep(
        OptType{ .url = "http://localhost:9014", .run_dir = "new" },
        try parse(&.{ "--url", "http://localhost:9014", "--run_dir", "new" }, OptType),
    );
}
