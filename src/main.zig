const std = @import("std");
const builtin = @import("builtin");

const zeit = @import("zeit");

const build = @import("build");
const Conf = @import("Conf.zig");
const server = @import("server.zig");

pub const tracy_impl = @import("tracy_impl");
pub const tracy = @import("tracy");
pub const tracy_options: tracy.Options = .{
    .on_demand = false,
    .no_broadcast = false,
    .only_localhost = false,
    .only_ipv4 = false,
    .delayed_init = false,
    .manual_lifetime = false,
    .verbose = false,
    .data_port = null,
    .broadcast_port = null,
    .default_callstack_depth = 32,
};

pub fn main() !void {
    var debugAlloc: ?std.heap.DebugAllocator(.{}) = null;

    var alloc: std.mem.Allocator = blk: {
        if (builtin.mode == .Debug) {
            debugAlloc = .init;
            break :blk debugAlloc.?.allocator();
        } else {
            break :blk std.heap.c_allocator;
        }
    };
    defer {
        if (debugAlloc) |*da| _ = da.deinit();
    }
    var tracyAlloc = tracy.Allocator{ .parent = alloc };
    alloc = tracyAlloc.allocator();

    // TODO replace IO API to evented/zio
    var ioImpl: std.Io.Threaded = .init(alloc, .{});
    defer ioImpl.deinit();
    const io = ioImpl.io();

    std.debug.print("Ochi version {s}", .{build.version});

    const config = Conf.default(alloc);
    const now = try zeit.instant(io, .{ .source = .now });
    var nowBuf: [32]u8 = undefined;
    const nowStr = try now.time().bufPrint(&nowBuf, .rfc3339);

    // TODO: introduce structured logger
    std.debug.print("Ochi in mono mode starting at port={d}, time={s}\n", .{ config.server.port, nowStr });

    try server.startServer(io, alloc, config);
}

test {
    _ = @import("tidy.zig");
    _ = @import("test/server.zig");
    std.testing.refAllDecls(server);
}

// TODO: good to move the packages to its places:
// - move data.zig to data/Data.zig
// - extract components from BlockData
// - separate data and data/MemTable packages
// - separate index and index/Memtable packages
