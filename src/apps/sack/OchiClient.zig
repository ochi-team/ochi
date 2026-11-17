const std = @import("std");
const Allocator = std.mem.Allocator;
const Self = @This();

/// The exact tenant header sent
pub const Tenant = union(enum) {
    absent,
    empty,
    id: u64,
};

pub const Request = struct {
    method: std.http.Method,
    path: []const u8,
    body: []const u8 = "",

    // headers
    contentType: ?[]const u8 = "application/json",
    tenant: Tenant = .{ .id = 0 },
    contentEncoding: ?[]const u8 = null,
};

pub const HttpResponse = struct {
    status: std.http.Status,
    body: std.Io.Writer.Allocating,

    pub fn deinit(self: *HttpResponse) void {
        self.body.deinit();
    }
};

host: []const u8,
client: std.http.Client,

pub fn deinit(self: *Self) void {
    self.client.deinit();
}

pub fn request(self: *Self, alloc: Allocator, options: Request) !HttpResponse {
    var urlBuf: [1024]u8 = undefined;
    const url = try std.fmt.bufPrint(&urlBuf, "{s}{s}", .{ std.mem.trimEnd(u8, self.host, "/"), options.path });

    var output: std.Io.Writer.Allocating = .init(alloc);

    var headers: [3]std.http.Header = undefined;
    var headersArr: std.ArrayList(std.http.Header) = .initBuffer(&headers);
    if (options.contentType) |contentType| {
        headersArr.appendAssumeCapacity(.{ .name = "Content-Type", .value = contentType });
    }
    var tenantBuffer: [32]u8 = undefined;
    const tenant: ?[]const u8 = switch (options.tenant) {
        .absent => null,
        .empty => "",
        .id => |id| try std.fmt.bufPrint(&tenantBuffer, "{d}", .{id}),
    };
    if (tenant) |value| {
        headersArr.appendAssumeCapacity(.{ .name = "X-Scope-OrgID", .value = value });
    }
    if (options.contentEncoding) |encoding| {
        headersArr.appendAssumeCapacity(.{ .name = "Content-Encoding", .value = encoding });
    }

    const result = try self.client.fetch(.{
        .location = .{ .url = url },
        .method = options.method,
        .payload = if (options.method.requestHasBody()) options.body else null,
        .extra_headers = headersArr.items,
        .response_writer = &output.writer,
    });
    return .{ .status = result.status, .body = output };
}

test "OchiClient tenant headers" {
    const testing = std.testing;
    const Io = std.Io;

    const responseText = "not ready";

    const f = struct {
        fn serve(listener: *Io.net.Server, expectedTenant: ?[]const u8) !void {
            const io = testing.io;
            const stream = try listener.accept(io);

            defer stream.close(io);
            var readBuffer: [4096]u8 = undefined;
            var writeBuffer: [4096]u8 = undefined;
            var reader = stream.reader(io, &readBuffer);
            var writer = stream.writer(io, &writeBuffer);
            var server = std.http.Server.init(&reader.interface, &writer.interface);

            var req = try server.receiveHead();
            try testing.expectEqual(.GET, req.head.method);
            try testing.expectEqualStrings("/ingest/loki/ready", req.head.target);
            var headers = req.iterateHeaders();
            var tenant: ?[]const u8 = null;
            while (headers.next()) |header| {
                if (std.ascii.eqlIgnoreCase(header.name, "X-Scope-OrgID")) tenant = header.value;
                try testing.expect(!std.ascii.eqlIgnoreCase(header.name, "Content-Type"));
            }
            try testing.expectEqualDeep(expectedTenant, tenant);
            try req.respond(responseText, .{ .status = .service_unavailable, .keep_alive = false });
        }

        fn check(tenant: Tenant, expectedTenant: ?[]const u8) !void {
            const alloc = testing.allocator;
            const io = testing.io;
            const address = try Io.net.IpAddress.parse("127.0.0.1", 0);
            var listener = try address.listen(io, .{});
            defer listener.deinit(io);

            var hostBuf: [128]u8 = undefined;
            const host = try std.fmt.bufPrint(&hostBuf, "http://127.0.0.1:{d}/", .{listener.socket.address.getPort()});

            var future = try Io.concurrent(io, serve, .{ &listener, expectedTenant });
            defer future.cancel(io) catch {};
            var client: Self = .{ .host = host, .client = .{ .allocator = alloc, .io = io } };
            defer client.deinit();
            var response = try client.request(alloc, .{
                .method = .GET,
                .path = "/ingest/loki/ready",
                .tenant = tenant,
                .contentType = null,
            });
            defer response.deinit();
            try future.await(io);

            try testing.expectEqual(.service_unavailable, response.status);
            try testing.expectEqualStrings(responseText, response.body.written());
        }
    }.check;
    try f(.absent, null);
    try f(.empty, "");
    try f(.{ .id = 0 }, "0");
    try f(.{ .id = std.math.maxInt(u64) }, "18446744073709551615");
}
