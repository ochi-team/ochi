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

var global_server: ?*httpz.Server(*Dispatcher) = null;

fn health(_: *AppContext, _: *httpz.Request, res: *httpz.Response) !void {
    res.status = 200;
}

// TODO not quite sure
fn handleSigterm(_: std.os.linux.SIG) callconv(.c) void {
    if (global_server) |server| {
        server.stop();
    }
}

// TODO: make it configurable
const storePath = ".ochi";

pub fn startServer(io: Io, allocator: std.mem.Allocator, conf: Conf) !void {
    std.Io.Dir.cwd().createDir(io, storePath, .default_dir) catch |err| switch (err) {
        error.PathAlreadyExists => {},
        else => return err,
    };
    var storePathBuf: [std.fs.max_path_bytes]u8 = undefined;
    const n = try std.Io.Dir.cwd().realPathFile(io, storePath, &storePathBuf);

    var store = try Store.init(io, allocator, storePathBuf[0..n]);
    defer store.deinit(io, allocator);

    var dispatcher: Dispatcher = .{
        .io = io,
        .allocator = allocator,
        .conf = conf.app,
        .store = &store,
    };
    var server = try httpz.Server(*Dispatcher).init(io, allocator, .{ .address = .localhost(conf.server.port) }, &dispatcher);
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

test {
    std.testing.refAllDecls(@This());
    _ = @import("server_test.zig");
}

test "serverWithSIGTERM" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    const conf = Conf.default(allocator);
    // Start the server in a separate thread
    var thread = Io.async(io, startServer, .{ io, allocator, conf });
    defer thread.cancel(io) catch |err| {
        std.debug.print("Server error: {}\n", .{err});
    };

    // Give the server time to start
    try Io.sleep(io, .fromMilliseconds(100), .real);

    // Send SIGTERM to ourselves
    const posix = std.posix;
    const pid = std.c.getpid();
    try posix.kill(pid, posix.SIG.TERM);

    // Wait for the server thread to finish

    try thread.await(io);
}

test "tidy" {
    const alloc = std.testing.allocator;
    const io = std.testing.io;
    const lint = @import("tidy.zig");
    const noMergeCommits = try lint.gitHasNoMergeCommits(io, alloc);
    try std.testing.expect(noMergeCommits);

    const isFormatted = try lint.projectIsFormatted(io, alloc);
    try std.testing.expect(isFormatted);
}
