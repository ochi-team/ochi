const std = @import("std");
const Io = std.Io;
const httpz = @import("httpz");
const testing = std.testing;
const dispatch = @import("dispatch.zig");
const Store = @import("Store.zig");
const Conf = @import("Conf.zig");
const Runtime = @import("Runtime.zig");
const Layout = @import("Layout.zig");
const PprofAllocator = @import("pprof/PprofAllocator.zig");

const BlockedRequest = struct {
    io: Io,
    entered: Io.Event = .unset,
    release: Io.Event = .unset,
    body: []const u8,

    fn action(_: *dispatch.AppContext, req: *httpz.Request, res: *httpz.Response) !void {
        const self: *BlockedRequest = @ptrCast(@alignCast(@constCast(req.route_data.?)));
        self.entered.set(self.io);
        self.release.waitUncancelable(self.io);
        res.body = self.body;
    }

    fn request(io: Io, expectedStatus: std.http.Status, expectedBody: []const u8) anyerror!void {
        const alloc = testing.allocator;
        var client: std.http.Client = .{ .allocator = alloc, .io = io };
        defer client.deinit();
        var body: Io.Writer.Allocating = .init(alloc);
        defer body.deinit();
        const response = try client.fetch(.{
            .location = .{ .url = "http://127.0.0.1:9014/blocked" },
            .response_writer = &body.writer,
            .keep_alive = false,
        });
        try testing.expectEqual(expectedStatus, response.status);
        try testing.expectEqualStrings(expectedBody, body.written());
    }
};

test "shutdown HTTP admission rejects new requests and finishes admitted response writes" {
    const f = struct {
        fn run(bodySize: usize) !void {
            const alloc = testing.allocator;
            const io = testing.io;
            var tmp = testing.tmpDir(.{});
            defer tmp.cleanup();
            const root = try tmp.dir.realPathFileAlloc(io, ".", alloc);
            defer alloc.free(root);
            const conf = Conf.getConf();
            const runtime = try Runtime.init(io, alloc, root, conf.app.maxCachePortion);
            defer runtime.deinit(alloc);
            var pathBuf: [std.fs.max_path_bytes]u8 = undefined;
            const layout = try Layout.make(io, root, &pathBuf);
            defer layout.partitionsDir.close(io);
            var store = try Store.init(io, alloc, &conf, runtime, layout);
            defer store.deinit(io, alloc);
            var pprof: PprofAllocator = .{ .child = alloc, .io = io };
            defer pprof.deinit();
            var dispatcher = try dispatch.Dispatcher.init(io, alloc, &conf.app, runtime, &store, &pprof);
            defer dispatcher.deinit();

            const body = try alloc.alloc(u8, bodySize);
            defer alloc.free(body);
            @memset(body, 'x');

            var blocked: BlockedRequest = .{ .io = io, .body = body };
            var server = try httpz.Server(*dispatch.Dispatcher).init(io, alloc, .{
                .address = .localhost(9014),
                .thread_pool = .{ .count = 4 },
            }, &dispatcher);
            defer server.deinit();
            const router = try server.router(.{});

            router.get("/blocked", BlockedRequest.action, .{ .data = &blocked });
            const thread = try server.listenInNewThread();
            defer {
                blocked.release.set(io);
                dispatcher.drain();
                server.stop();
                thread.join();
            }

            // signal its handler has started
            // it hangs here, wait for .release.set in the end
            // and asserts it responds .ok status even when the admission is closed
            // and shutdown is in progress
            var response = try io.concurrent(BlockedRequest.request, .{ io, .ok, body });
            defer {
                blocked.release.set(io);
                response.await(io) catch {};
            }
            blocked.entered.waitUncancelable(io);
            var draining = try io.concurrent(dispatch.Dispatcher.drain, .{&dispatcher});
            defer {
                blocked.release.set(io);
                draining.await(io);
            }
            // Scheduling drain does not guarantee admission has closed yet.
            dispatcher.requests.closed.waitUncancelable(io);
            try testing.expect(dispatcher.requests.isClosed(io));
            try BlockedRequest.request(io, .service_unavailable, "server is shutting down");
            try BlockedRequest.request(io, .service_unavailable, "server is shutting down");
            blocked.release.set(io);
            try response.await(io);
            draining.await(io);
        }
    }.run;

    try f(1);
    // Exercise writes larger than httpz's request/response buffers.
    try f(512 * 1024);
}
