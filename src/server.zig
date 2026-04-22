const std = @import("std");

const httpz = @import("httpz");

const Conf = @import("Conf.zig");
const Dispatcher = @import("dispatch.zig").Dispatcher;
const AppContext = @import("dispatch.zig").AppContext;
const Store = @import("Store.zig").Store;
const insert = @import("handlers/insert.zig");
const query = @import("handlers/query.zig");
const flush = @import("handlers/flush.zig");

var global_server: ?*httpz.Server(*Dispatcher) = null;

fn health(_: *AppContext, _: *httpz.Request, res: *httpz.Response) !void {
    res.status = 200;
}

fn handleSigterm(_: c_int) callconv(.c) void {
    if (global_server) |server| {
        server.stop();
    }
}

// TODO: make it configurable
const storePath = ".ochi";

pub fn startServer(allocator: std.mem.Allocator, conf: Conf) !void {
    std.fs.cwd().makeDir(storePath) catch |err| switch (err) {
        error.PathAlreadyExists => {},
        else => return err,
    };
    var storePathBuf: [std.fs.max_path_bytes]u8 = undefined;
    const path = try std.fs.cwd().realpath(storePath, &storePathBuf);

    var store = try Store.init(allocator, path);
    defer store.deinit(allocator);

    var dispatcher: Dispatcher = .{
        .conf = conf.app,
        .store = &store,
    };
    var server = try httpz.Server(*Dispatcher).init(allocator, .{ .port = conf.server.port }, &dispatcher);
    defer server.deinit();

    global_server = &server;
    defer global_server = null;

    // Set up SIGTERM handler
    const posix = std.posix;
    const empty_set = std.mem.zeroes(posix.sigset_t);
    const act = posix.Sigaction{
        .handler = .{ .handler = handleSigterm },
        .mask = empty_set,
        .flags = 0,
    };
    posix.sigaction(posix.SIG.TERM, &act, null);

    var router = try server.router(.{});
    router.get("/health", health, .{});

    router.get("/insert/loki/ready", insert.insertLokiReady, .{});
    router.post("/insert/loki/api/v1/push", insert.insertLokiJsonHandler, .{});
    router.post("/query", query.queryHandler, .{});
    router.post("/query", query.queryHandler, .{});
    router.post("/flush", flush.flushHandler, .{});

    server.listen() catch |err| switch (err) {
        std.posix.BindError.AddressInUse => {
            std.debug.print("can't start server, port={d} is in use\n", .{conf.server.port});
            return err;
        },
        else => {
            std.debug.print("can't start server, unexpected error {any}\n", .{err});
            return err;
        },
    };
}

test {
    std.testing.refAllDeclsRecursive(@This());
    _ = @import("server_test.zig");
}

test "serverWithSIGTERM" {
    const allocator = std.testing.allocator;

    const conf = Conf.default(allocator);
    // Start the server in a separate thread
    const ServerThread = struct {
        fn run(threadAllocator: std.mem.Allocator, threadConf: Conf) void {
            startServer(threadAllocator, threadConf) catch |err| {
                std.debug.print("Server error: {}\n", .{err});
            };
        }
    };

    const thread = try std.Thread.spawn(.{}, ServerThread.run, .{ allocator, conf });

    // Give the server time to start
    std.Thread.sleep(100 * std.time.ns_per_ms);

    // Send SIGTERM to ourselves
    const posix = std.posix;
    const pid = std.c.getpid();
    try posix.kill(pid, posix.SIG.TERM);

    // Wait for the server thread to finish
    thread.join();
}

test "tidy" {
    const alloc = std.testing.allocator;
    const lint = @import("tidy.zig");
    const noMergeCommits = try lint.gitHasNoMergeCommits(alloc);
    try std.testing.expect(noMergeCommits);

    const isFormatted = try lint.projectIsFormatted(alloc);
    try std.testing.expect(isFormatted);
}
