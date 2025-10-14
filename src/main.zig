const std = @import("std");

const datetime = @import("datetime").datetime;

const build = @import("build");
const Conf = @import("conf.zig").Conf;
const server = @import("server.zig");

pub fn main() !void {
    std.debug.print("Seqor version {s}", .{build.version});

    const config = Conf.default();
    const now_str = try datetime.Datetime.now().formatISO8601(std.heap.page_allocator, false);

    // TODO: introduce structured logger
    std.debug.print("Seqor in mono mode starting at port={d}, time={s}\n", .{ config.port, now_str });

    try server.startServer(std.heap.page_allocator, config);
}
