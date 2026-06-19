const std = @import("std");
const builtin = @import("builtin");

const zeit = @import("zeit");
const Logger = @import("logging");

const build = @import("build");
const Conf = @import("Conf.zig");
const Runtime = @import("Runtime.zig");
const inspect = @import("inspect.zig");
const server = @import("server.zig");

pub const tracy_impl = @import("tracy_impl");
pub const tracy = @import("tracy");
pub const tracy_options: tracy.Options = .{
    // starts the profiling only on connection initiation
    .on_demand = true,
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
        if (!build.release) {
            debugAlloc = .init;
            break :blk debugAlloc.?.allocator();
        } else {
            // TODO: hack a puzzle how we could eliminate runtime allocations
            // TODO: play with mallopt, such a mmap threshold
            break :blk std.heap.c_allocator;
        }
    };
    defer {
        if (debugAlloc) |*da| _ = da.deinit();
    }

    // TODO: replace IO API to evented/zio
    var ioImpl: std.Io.Threaded = .init(alloc, .{
        // TODO: change to a real number of cpus
        .concurrent_limit = .limited(16),
    });
    defer ioImpl.deinit();
    const io = ioImpl.io();

    var tracyAlloc = tracy.Allocator{
        .parent = alloc,
    };
    alloc = tracyAlloc.allocator();

    try server.startApp(io, alloc, .{
        .release = build.release,
        .version = build.version,
    });
}

test {
    var debugAlloc: std.heap.DebugAllocator(.{}) = .init;
    const alloc = debugAlloc.allocator();
    try Logger.setup(std.testing.io, alloc, .{
        .level = .None,
    });

    _ = @import("tidy.zig");
    _ = @import("test/server.zig");
    std.testing.refAllDecls(server);
}

// TODO: good to move the packages to its places:
// - move data.zig to data/Data.zig
// - extract components from BlockData
// - separate data and data/MemTable packages
// - separate index and index/Memtable packages
