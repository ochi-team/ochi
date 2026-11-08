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
const ingest = @import("handlers/ingest.zig");
const query = @import("handlers/query.zig");
const pprof = @import("handlers/pprof.zig");
const flush = @import("handlers/flush.zig");
const stream_ids = @import("handlers/stream_ids.zig");
const PprofAllocator = @import("pprof/PprofAllocator.zig");
const Logger = @import("logging");

var shutdownRequested: std.atomic.Value(bool) = .init(false);
const ShutdownCoordinator = struct {
    io: Io,
    server: *httpz.Server(*Dispatcher),
    dispatcher: *Dispatcher,
    // TODO: make it a conditional variable or something? it's just spinning for that value
    finished: std.atomic.Value(bool) = .init(false),

    fn run(self: *ShutdownCoordinator) void {
        var shutdownStarted = false;
        while (!self.finished.load(.acquire)) {
            if (shutdownRequested.load(.acquire)) {
                if (!shutdownStarted) {
                    Logger.log(.debug, "graceful shutdown started", .{});
                    shutdownStarted = true;
                }
                self.dispatcher.drain();
                // listen holds _mut until every worker can safely be stopped.
                self.server._mut.lockUncancelable(self.io);
                const listening = self.server._listener != null;
                self.server._mut.unlock(self.io);
                if (listening) {
                    self.server.stop();
                    return;
                }
            }
            // Bound notification latency without running synchronization code in the signal handler.
            Io.sleep(self.io, .fromMilliseconds(10), .real) catch {};
        }
    }
};

fn health(_: *AppContext, _: *httpz.Request, res: *httpz.Response) !void {
    res.status = 200;
}

fn metrics(ctx: *AppContext, _: *httpz.Request, res: *httpz.Response) !void {
    res.status = 200;
    res.header("content-type", "text/plain");

    const w = res.writer();
    try ctx.dispatchMeter.write(w);
    try ctx.storeMeter.write(w);
    try ctx.store.streamCache.hitRate.write(w);
    try ctx.store.streamCache.missRate.write(w);
    try ctx.store.indexMemBlocksCache.hitRate.write(w);
    try ctx.store.indexMemBlocksCache.missRate.write(w);
}

fn registerSigtermHandler() void {
    // TODO: this flag must go away and test real restart of the process
    // this is testing convinience,
    // in order to test the in memory buffers flush we have to shutdown the app and start it again.
    shutdownRequested.store(false, .release);

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
    shutdownRequested.store(true, .release);
}

pub const StartOptions = struct {
    version: []const u8,
    release: bool,
    // TODO: we must get rid of a global logger, it's not ok,
    // but before we must reimplement the logger to manage buffers per thread / worker
    setupLogger: bool = true,
    pprof: bool = false,
};

pub fn startApp(io: Io, baseAllocator: std.mem.Allocator, options: StartOptions) !void {
    registerSigtermHandler();

    var pprofAlloc: PprofAllocator = .{
        .child = baseAllocator,
        .io = io,
    };
    defer pprofAlloc.deinit();
    var alloc = baseAllocator;
    if (options.pprof) {
        alloc = pprofAlloc.allocator();
    }

    const conf = Conf.default();
    var cwdBuf: [std.fs.max_path_bytes]u8 = undefined;

    std.Io.Dir.cwd().createDir(io, conf.app.storePath, .default_dir) catch |err| switch (err) {
        error.PathAlreadyExists => {},
        else => return err,
    };
    const n = try std.Io.Dir.cwd().realPathFile(io, conf.app.storePath, &cwdBuf);
    const path = cwdBuf[0..n];

    var partitionsPathBuf: [std.fs.max_path_bytes]u8 = undefined;
    const layout = try Layout.make(io, path, &partitionsPathBuf);
    defer layout.partitionsDir.close(io);

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
    try inspect.inspect(options.release);

    Logger.log(
        .info,
        "Ochi in mono mode starting",
        .{ .port = conf.server.port, .version = options.version, .cpus = runtime.cpus },
    );

    var store = try Store.init(io, alloc, &conf, runtime, layout);
    defer store.deinit(io, alloc);
    try store.start(io, alloc);

    startServer(io, alloc, conf, runtime, &store, &pprofAlloc) catch |err| {
        Logger.log(.err, "server stopped unexpectedly", .{ .err = err });
    };
    store.shutdown(io) catch |err| {
        Logger.log(.err, "server failed to shutdown", .{ .err = err });
    };
    Logger.log(.debug, "graceful shutdown completed", .{});
}

fn startServer(
    io: Io,
    allocator: std.mem.Allocator,
    conf: Conf,
    runtime: *Runtime,
    store: *Store,
    pprofAlloc: *PprofAllocator,
) !void {
    var dispatcher = try Dispatcher.init(io, allocator, &conf.app, runtime, store, pprofAlloc);
    defer {
        dispatcher.stopAndFlush(io);
        dispatcher.deinit();
    }
    var server = try httpz.Server(*Dispatcher).init(io, allocator, .{
        .address = .all(conf.server.port),
        .thread_pool = .{
            .count = runtime.cpus,
            .buffer_size = 32 * 1024,
        },
        .workers = .{
            .max_conn = @intCast(conf.app.maxConnections),
            .retain_allocated_bytes = 4 * 1024 * 1024,
        },
    }, &dispatcher);
    defer server.deinit();
    var router = try server.router(.{});
    router.get("/health", health, .{});
    router.get("/metrics", metrics, .{});
    router.get("/debug/pprof/allocs", pprof.allocsHandler, .{});

    router.get("/ingest/loki/ready", ingest.ingestLokiReady, .{});
    router.post("/ingest/loki/api/v1/push", ingest.ingestLokiJsonHandler, .{});

    router.post("/query", query.queryHandler, .{});
    router.post("/stream_ids", stream_ids.streamIDsHandler, .{});
    router.post("/flush", flush.flushHandler, .{});

    var coordinator: ShutdownCoordinator = .{ .io = io, .server = &server, .dispatcher = &dispatcher };
    // TODO: it feels wrong to run a thread to wait for a shutdown signal
    const coordinatorThread = try std.Thread.spawn(.{}, ShutdownCoordinator.run, .{&coordinator});
    defer {
        coordinator.finished.store(true, .release);
        coordinatorThread.join();
    }
    // TODO: fix http partial worker startup cleanup to join every driver thread
    // before worker deinit
    server.listen() catch |err| {
        Logger.log(.err, "HTTP listener failed", .{ .err = err, .port = conf.server.port });
    };
}
