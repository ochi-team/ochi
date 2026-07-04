const std = @import("std");
const builtin = @import("builtin");
const Io = std.Io;

const httpz = @import("httpz");

const inspect = @import("inspect.zig");
const Conf = @import("Conf.zig");
const Runtime = @import("Runtime.zig");
const Dispatcher = @import("dispatch.zig").Dispatcher;
const AppContext = @import("dispatch.zig").AppContext;
const Store = @import("Store.zig").Store;
const Layout = @import("Layout.zig");
const insert = @import("handlers/insert.zig");
const query = @import("handlers/query.zig");
const flush = @import("handlers/flush.zig");
const stream_ids = @import("handlers/stream_ids.zig");
const Logger = @import("logging");

var global_server: ?*httpz.Server(*Dispatcher) = null;

fn health(_: *AppContext, _: *httpz.Request, res: *httpz.Response) !void {
    res.status = 200;
}

fn metrics(ctx: *AppContext, _: *httpz.Request, res: *httpz.Response) !void {
    res.status = 200;
    res.header("content-type", "text/plain");

    const w = res.writer();
    try ctx.dispatchMeter.write(w);
    try ctx.storeMeter.write(w);
}

fn registerSigtermHandler() void {
    const empty_set = std.mem.zeroes(std.posix.sigset_t);
    const act = std.posix.Sigaction{
        .handler = .{ .handler = handleSigterm },
        .mask = empty_set,
        .flags = 0,
    };
    std.posix.sigaction(std.posix.SIG.TERM, &act, null);
    // skip in tests, otherwise it overrides the basic handler and it can't interrupt tests anymore
    if (!builtin.is_test) std.posix.sigaction(std.posix.SIG.INT, &act, null);
}

fn handleSigterm(_: std.posix.SIG) callconv(.c) void {
    if (global_server) |server| {
        server.stop();
    }
}

pub const StartOptions = struct {
    version: []const u8,
    release: bool,
    // TODO: we must get rid of a global logger, it's not ok,
    // but before we must reimplement the logger to manage buffers per thread / worker
    setupLogger: bool = true,
};

pub fn startApp(io: Io, alloc: std.mem.Allocator, options: StartOptions) !void {
    const conf = Conf.default(alloc);
    var cwdBuf: [std.fs.max_path_bytes]u8 = undefined;

    std.Io.Dir.cwd().createDir(io, conf.app.storePath, .default_dir) catch |err| switch (err) {
        error.PathAlreadyExists => {},
        else => return err,
    };
    const n = try std.Io.Dir.cwd().realPathFile(io, conf.app.storePath, &cwdBuf);
    const path = cwdBuf[0..n];

    var partitionsPathBuf: [std.fs.max_path_bytes]u8 = undefined;
    const layout = try Layout.make(io, path, &partitionsPathBuf);

    const runtime = try Runtime.init(io, alloc, path, conf.app.maxCachePortion);
    defer runtime.deinit(alloc);

    if (options.setupLogger) {
        // initialize a logging pool
        try Logger.setup(io, alloc, .{
            .level = Logger.levelFromBuildMode(),
            .pool_size = 2 * runtime.cpus,
            .buffer_size = 4096,
            .large_buffer_count = @max(2, runtime.cpus),
            .large_buffer_size = 1 << 15, // 32 kb
            .output = .stdout,
            .encoding = .logfmt,
        });
    }
    defer if (options.setupLogger) Logger.deinit();
    try inspect.inspect(options.release, io);

    Logger.log(
        .info,
        "Ochi in mono mode starting",
        .{ .port = conf.server.port, .version = options.version },
    );

    var store = try Store.init(io, alloc, &conf, runtime, layout);
    defer store.deinit(io, alloc);
    try store.start(io, alloc);

    try startServer(io, alloc, conf, &store);
}

pub fn startServer(io: Io, allocator: std.mem.Allocator, conf: Conf, store: *Store) !void {
    var dispatcher = try Dispatcher.init(io, allocator, &conf.app, store);
    defer dispatcher.deinit();
    var server = try httpz.Server(*Dispatcher).init(io, allocator, .{
        .address = .all(conf.server.port),
        .thread_pool = .{
            // TODO: set to amount of cpus
            .count = 8,
            .buffer_size = 32 * 1024,
        },
    }, &dispatcher);
    registerSigtermHandler();
    defer server.deinit();

    global_server = &server;
    defer global_server = null;

    var router = try server.router(.{});
    router.get("/health", health, .{});
    router.get("/metrics", metrics, .{});

    router.get("/ingest/loki/ready", insert.ingestLokiReady, .{});
    router.post("/ingest/loki/api/v1/push", insert.ingestLokiJsonHandler, .{});

    router.post("/query", query.queryHandler, .{});
    router.post("/stream_ids", stream_ids.streamIDsHandler, .{});
    router.post("/flush", flush.flushHandler, .{});

    // TODO: implement proper shutdown with cancel signal
    // - graceful draining of connections
    // - insert the rest incoming chunks
    // - graceful data cleaning
    // - stopping all the merge jobs
    // TODO: handle the above described shutdown in case of resource lack:
    // - memory resource (OOM)
    // - disk resource (full disk)
    // - file descriptors (too many open files)
    server.listen() catch |err| switch (err) {
        error.AddressInUse => {
            Logger.log(.err, "can't start server, port is in use", .{ .port = conf.server.port });
            return err;
        },
        else => {
            Logger.log(.err, "can't start server, unexpected error", .{ .err = err });
            return err;
        },
    };
}
