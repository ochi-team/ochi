const std = @import("std");

const httpz = @import("httpz");

const Conf = @import("conf.zig");
const Dispatcher = @import("dispatch.zig").Dispatcher;
const AppContext = @import("dispatch.zig").AppContext;
const Processor = @import("process.zig").Processor;
const Store = @import("store.zig").Store;
const insert = @import("insert.zig");

var global_server: ?*httpz.Server(*Dispatcher) = null;

fn health(_: *AppContext, _: *httpz.Request, res: *httpz.Response) !void {
    res.status = 200;
}

fn handleSigterm(_: c_int) callconv(.c) void {
    if (global_server) |server| {
        server.stop();
    }
}

pub fn startServer(allocator: std.mem.Allocator, backgroundAllocator: std.mem.Allocator, conf: Conf) !void {
    const store = try Store.init(allocator, backgroundAllocator, ".seqor");
    defer store.deinit(allocator);
    const processor = try Processor.init(allocator, store);
    defer processor.deinit(allocator);

    const dispatcher = try allocator.create(Dispatcher);
    defer allocator.destroy(dispatcher);

    dispatcher.* = Dispatcher{
        .conf = conf.app,
        .processor = processor,
    };
    var server = try httpz.Server(*Dispatcher).init(allocator, .{ .port = conf.server.port }, dispatcher);
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
    router.post("/insert/loki/api/v1/push", insert.insertLokiJson, .{});

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
}

test "serverWithSIGTERM" {
    const allocator = std.testing.allocator;

    // Start the server in a separate thread
    const ServerThread = struct {
        fn run() void {
            startServer(allocator, allocator, Conf.default()) catch |err| {
                std.debug.print("Server error: {}\n", .{err});
            };
        }
    };

    const thread = try std.Thread.spawn(.{}, ServerThread.run, .{});

    // Give the server time to start
    std.Thread.sleep(100 * std.time.ns_per_ms);

    // Send SIGTERM to ourselves
    const posix = std.posix;
    const pid = std.c.getpid();
    try posix.kill(pid, posix.SIG.TERM);

    // Wait for the server thread to finish
    thread.join();
}
