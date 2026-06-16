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
    const level: Logger.Level = l: {
        if (builtin.is_test) break :l .None;
        if (builtin.mode == .Debug) break :l .Debug;
        break :l .Info;
    };
    try Logger.setup(io, alloc, .{
        .level = level,
        .pool_size = 1,
        .buffer_size = 4096,
        .large_buffer_count = 1,
        .large_buffer_size = 1 << 15, // 32 kb
        .output = .stdout,
        .encoding = .logfmt,
    });
    defer Logger.deinit();
    try inspect.inspect(build.release, io);

    var tracyAlloc = tracy.Allocator{
        .parent = alloc,
    };
    alloc = tracyAlloc.allocator();
    if (tracy.enabled) {
        Logger.log(.info, "Tracy profiler enabled", .{});
    }

    const conf = Conf.default(alloc);
    var cwdBuf: [std.fs.max_path_bytes]u8 = undefined;
    const n = try std.Io.Dir.cwd().realPathFile(io, conf.app.storePath, &cwdBuf);
    const runtime = try Runtime.init(io, alloc, cwdBuf[0..n], conf.app.maxCachePortion);
    errdefer runtime.deinit(alloc);

    // initialize a logging pool
    try Logger.setup(io, alloc, .{
        .level = level,
        .pool_size = 16 * runtime.cpus,
        .buffer_size = 4096,
        .large_buffer_count = @max(2, runtime.cpus),
        .large_buffer_size = 1 << 15, // 32 kb
        .output = .stdout,
        .encoding = .logfmt,
    });

    Logger.log(
        .info,
        "Ochi in mono mode starting",
        .{ .port = conf.server.port, .version = build.version },
    );

    try server.startServer(io, alloc, conf, runtime);
}

test {
    var debugAlloc: std.heap.DebugAllocator(.{}) = .init;
    const alloc = debugAlloc.allocator();
    try Logger.setup(std.testing.io, alloc, .{
        .level = .None,
        .pool_size = 16,
        .buffer_size = 4096,
        .large_buffer_count = 2,
        .large_buffer_size = 1 << 15, // 32 kb
        .output = .stdout,
        .encoding = .logfmt,
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
