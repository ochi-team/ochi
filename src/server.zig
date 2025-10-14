const std = @import("std");

const httpz = @import("httpz");

const Conf = @import("conf.zig").Conf;
const Dispatcher = @import("dispatch.zig").Dispatcher;
const AppContext = @import("dispatch.zig").AppContext;
const Processor = @import("process.zig").Processor;
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

pub fn startServer(allocator: std.mem.Allocator, conf: Conf) !void {
    const processor = try Processor.init(allocator);
    defer processor.deinit(allocator);

    const dispatcher = try allocator.create(Dispatcher);
    defer allocator.destroy(dispatcher);

    dispatcher.* = Dispatcher{
        .conf = conf.app,
        .processor = processor,
    };
    var server = try httpz.Server(*Dispatcher).init(allocator, .{ .port = conf.port }, dispatcher);
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

    try server.listen();
}

// TODO: this is not ok, I have to import every module I want to test???
test {
    _ = @import("server_test.zig");
}
