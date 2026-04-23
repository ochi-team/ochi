const std = @import("std");

const zeit = @import("zeit");

const build = @import("build");
const Conf = @import("Conf.zig");
const server = @import("server.zig");

pub fn main() !void {
    std.debug.print("Ochi version {s}", .{build.version});

    const config = Conf.default(std.heap.page_allocator);
    const now = try zeit.instant(.{ .source = .now });
    var nowBuf: [32]u8 = undefined;
    const nowStr = try now.time().bufPrint(&nowBuf, .rfc3339);

    // TODO: introduce structured logger
    std.debug.print("Ochi in mono mode starting at port={d}, time={s}\n", .{ config.server.port, nowStr });

    try server.startServer(std.heap.page_allocator, config);
}
