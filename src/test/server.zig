const std = @import("std");
const Io = std.Io;
const snappy = @import("snappy").raw;

const Conf = @import("../Conf.zig");
const server = @import("../server.zig");

const QueryField = struct {
    key: []const u8,
    value: []const u8,
};

const QueryLine = struct {
    timestampNs: u64,
    fields: []const QueryField,
};

const HttpResponse = struct {
    statusCode: u16,
    body: []u8,

    fn deinit(self: *HttpResponse, alloc: std.mem.Allocator) void {
        alloc.free(self.body);
    }
};

pub const OchiClient = struct {
    host: []const u8,
    client: std.http.Client,

    fn request(
        client: *OchiClient,
        alloc: std.mem.Allocator,
        method: std.http.Method,
        path: []const u8,
        body: []const u8,
        contentType: ?[]const u8,
        contentEncoding: ?[]const u8,
    ) !HttpResponse {
        const url = try std.fmt.allocPrint(alloc, "{s}{s}", .{ client.host, path });
        defer alloc.free(url);

        var responseWriter = try std.Io.Writer.Allocating.initCapacity(alloc, 1024);
        defer responseWriter.deinit();

        var headersBuf: [2]std.http.Header = undefined;
        var headersLen: usize = 0;
        if (contentType) |ct| {
            headersBuf[headersLen] = .{ .name = "content-type", .value = ct };
            headersLen += 1;
        }
        if (contentEncoding) |ce| {
            headersBuf[headersLen] = .{ .name = "content-encoding", .value = ce };
            headersLen += 1;
        }

        const payload: ?[]const u8 = if (method.requestHasBody()) body else null;

        const fetchResult = try client.client.fetch(.{
            .location = .{ .url = url },
            .method = method,
            .payload = payload,
            .extra_headers = headersBuf[0..headersLen],
            .response_writer = &responseWriter.writer,
        });

        const responseBody = try responseWriter.toOwnedSlice();

        return .{
            .statusCode = @intCast(@intFromEnum(fetchResult.status)),
            .body = responseBody,
        };
    }

    fn waitUntilReady(client: *OchiClient, io: Io, alloc: std.mem.Allocator, timeout: std.Io.Duration) !void {
        const start = Io.Timestamp.now(io, .real).nanoseconds;

        while ((Io.Timestamp.now(io, .real).nanoseconds - start) < timeout.nanoseconds) {
            var resp = client.request(alloc, .GET, "/insert/loki/ready", "", null, null) catch |err| {
                std.debug.print("Server not ready yet, error: {}\n", .{err});
                try Io.sleep(io, .fromMilliseconds(50), .real);
                continue;
            };
            defer resp.deinit(alloc);

            if (resp.statusCode == 200) {
                return;
            }
            std.debug.print("Server not ready yet, status code: {d}\n", .{resp.statusCode});
            try Io.sleep(io, .fromMilliseconds(50), .real);
        }

        return error.Timeout;
    }
};

fn expectField(line: QueryLine, expectedKey: []const u8, expectedValue: []const u8) !void {
    for (line.fields) |field| {
        if (std.mem.eql(u8, field.key, expectedKey)) {
            try std.testing.expectEqualStrings(expectedValue, field.value);
            return;
        }
    }

    return error.FieldNotFound;
}

test "serverEndToEndViaHTTP" {
    const alloc = std.testing.allocator;
    const io = std.testing.io;

    const oldCwd = try std.process.currentPathAlloc(io, alloc);
    defer {
        std.Io.Threaded.chdir(oldCwd) catch |err| {
            std.debug.print("Cannot chdir error: {}\n", .{err});
        };
        alloc.free(oldCwd);
    }

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const tmpPath = try tmp.dir.realPathFileAlloc(io, ".", alloc);
    defer alloc.free(tmpPath);
    try std.Io.Threaded.chdir(tmpPath);

    const conf = Conf.default(alloc);
    const ServerThread = struct {
        fn run(threadAllocator: std.mem.Allocator, threadConf: Conf) void {
            server.startServer(io, threadAllocator, threadConf) catch |err| {
                std.debug.print("Server error: {}\n", .{err});
            };
        }
    };

    // TODO should use testing.allocator
    // Produces a lot of memory leaks, that's why it's not used
    var serverFuture = try Io.concurrent(io, ServerThread.run, .{ std.heap.page_allocator, conf });
    defer serverFuture.await(io);

    var ochiClient: OchiClient = .{
        .host = "http://localhost:9014",
        .client = .{
            .allocator = alloc,
            .io = io,
        },
    };
    defer ochiClient.client.deinit();

    try ochiClient.waitUntilReady(io, alloc, .fromSeconds(1));

    const tsNs: u64 = @intCast(Io.Timestamp.now(io, .real).nanoseconds);
    const insertJson = try std.fmt.allocPrint(
        alloc,
        "{{\"streams\":[{{\"stream\":{{\"tag1\":\"alpha\",\"tag2\":\"beta\"}},\"values\":[[\"{d}\",\"same message\",{{\"field1\":\"x\",\"field2\":\"x\"}}]]}}]}}",
        .{tsNs},
    );
    defer alloc.free(insertJson);

    const maxCompressedLen = snappy.maxCompressedLength(insertJson.len);
    const compressed = try alloc.alloc(u8, maxCompressedLen);
    defer alloc.free(compressed);
    const compressedLen = try snappy.compress(insertJson, compressed);

    {
        var resp = try ochiClient.request(
            alloc,
            .POST,
            "/insert/loki/api/v1/push",
            compressed[0..compressedLen],
            "application/json",
            "snappy",
        );
        defer resp.deinit(alloc);
        try std.testing.expectEqual(200, resp.statusCode);
    }

    {
        var resp = try ochiClient.request(alloc, .POST, "/flush", "", null, null);
        defer resp.deinit(alloc);
        try std.testing.expectEqual(200, resp.statusCode);
    }

    const queryJson = try std.fmt.allocPrint(
        alloc,
        "{{\"start\":{d},\"end\":{d},\"tags\":[{{\"key\":\"tag1\",\"value\":\"alpha\"}},{{\"key\":\"tag2\",\"value\":\"beta\"}}],\"fields\":[{{\"key\":\"field1\",\"value\":\"x\"}},{{\"key\":\"field2\",\"value\":\"x\"}}]}}",
        .{ tsNs - 1, tsNs + 1 },
    );
    defer alloc.free(queryJson);

    {
        const queryStart = Io.Timestamp.now(io, .real).nanoseconds;
        const queryTimeoutNs = 5 * std.time.ns_per_s;

        while (true) {
            var resp = try ochiClient.request(alloc, .POST, "/query", queryJson, "application/json", null);
            defer resp.deinit(alloc);
            try std.testing.expectEqual(@as(u16, 200), resp.statusCode);

            const parsed = try std.json.parseFromSlice([]QueryLine, alloc, resp.body, .{
                .ignore_unknown_fields = true,
            });
            defer parsed.deinit();

            if (parsed.value.len == 1) {
                const line = parsed.value[0];
                try std.testing.expectEqual(tsNs, line.timestampNs);

                try expectField(line, "tag1", "alpha");
                try expectField(line, "tag2", "beta");
                try expectField(line, "field1", "x");
                try expectField(line, "field2", "x");
                try expectField(line, "", "same message");

                try std.testing.expectEqual(@as(usize, 5), line.fields.len);
                break;
            }

            if (Io.Timestamp.now(io, .real).nanoseconds - queryStart > queryTimeoutNs) {
                return error.Timeout;
            }

            try Io.sleep(io, .fromMilliseconds(50), .real);
        }
    }

    try std.posix.kill(std.c.getpid(), std.posix.SIG.TERM);
}

// TODO: test querying fields with "." in a key/value
