const std = @import("std");

const zeit = @import("zeit");

const build = @import("build");
const Conf = @import("Conf.zig");
const server = @import("server.zig");

pub fn main() !void {
    // TODO set allocator based on build options
    var gpa_allocator: std.heap.DebugAllocator(.{}) = .init;
    defer _ = gpa_allocator.deinit();
    const gpa = gpa_allocator.allocator();

    // TODO set io based on build options
    var io_impl: std.Io.Threaded = .init(gpa, .{});
    defer io_impl.deinit();
    const io = io_impl.io();

    std.debug.print("Ochi version {s}", .{build.version});

    const config = Conf.default(gpa);
    const now = try zeit.instant(io, .{ .source = .now });
    var nowBuf: [32]u8 = undefined;
    const nowStr = try now.time().bufPrint(&nowBuf, .rfc3339);

    // TODO: introduce structured logger
    std.debug.print("Ochi in mono mode starting at port={d}, time={s}\n", .{ config.server.port, nowStr });

    try server.startServer(io, gpa, config);
}
