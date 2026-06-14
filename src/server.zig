const std = @import("std");
const Io = std.Io;

const httpz = @import("httpz");

const Conf = @import("Conf.zig");
const Dispatcher = @import("dispatch.zig").Dispatcher;
const AppContext = @import("dispatch.zig").AppContext;
const Store = @import("Store.zig").Store;
const insert = @import("handlers/insert.zig");
const query = @import("handlers/query.zig");
const flush = @import("handlers/flush.zig");
const stream_ids = @import("handlers/stream_ids.zig");

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
}

fn handleSigterm(_: std.posix.SIG) callconv(.c) void {
    if (global_server) |server| {
        server.stop();
    }
}

// TODO: validate on startup ulimit -n is larger than 4k for a running process and log an error
// OR fail in release model with a recommendation 1 << 16 or unlimited
pub fn startServer(io: Io, allocator: std.mem.Allocator, conf: Conf) !void {
    std.Io.Dir.cwd().createDir(io, conf.app.storePath, .default_dir) catch |err| switch (err) {
        error.PathAlreadyExists => {},
        else => return err,
    };
    var storePathBuf: [std.fs.max_path_bytes]u8 = undefined;
    const n = try std.Io.Dir.cwd().realPathFile(io, conf.app.storePath, &storePathBuf);

    var store = try Store.init(io, allocator, storePathBuf[0..n], &conf);
    defer store.deinit(io, allocator);
    try store.start(io, allocator);

    var dispatcher = try Dispatcher.init(io, allocator, &conf.app, &store);
    defer dispatcher.deinit();
    var server = try httpz.Server(*Dispatcher).init(io, allocator, .{
        .address = .all(conf.server.port),
        .thread_pool = .{
            // TODO: set to amount of clients limit
            .count = 2,
            .buffer_size = 32 * 1024,
            .backlog = 64,
        },
    }, &dispatcher);
    registerSigtermHandler();
    defer server.deinit();

    global_server = &server;
    defer global_server = null;

    var router = try server.router(.{});
    router.get("/health", health, .{});
    router.get("/metrics", metrics, .{});

    router.get("/ingest/loki/ready", insert.insertLokiReady, .{});
    router.post("/ingest/loki/api/v1/push", insert.insertLokiJsonHandler, .{});

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
            std.debug.print("can't start server, port={d} is in use\n", .{conf.server.port});
            return err;
        },
        else => {
            std.debug.print("can't start server, unexpected error {any}\n", .{err});
            return err;
        },
    };
}

test "serverWithSIGTERM" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    const conf = Conf.default(allocator);
    // Start the server in a separate thread
    const ServerThread = struct {
        fn run(threadAllocator: std.mem.Allocator, threadConf: Conf) void {
            startServer(io, threadAllocator, threadConf) catch |err| {
                std.debug.print("Server error: {}\n", .{err});
            };
        }
    };

    var future = try io.concurrent(ServerThread.run, .{ allocator, conf });
    defer future.await(io);

    // Give the server time to start
    try io.sleep(.fromMilliseconds(100), .real);

    // Send SIGTERM to ourselves
    try std.posix.kill(std.c.getpid(), std.posix.SIG.TERM);
}
