const std = @import("std");
const Io = std.Io;
const snappy = @import("snappy").raw;

const Conf = @import("Conf.zig");
const server = @import("server.zig");

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

fn request(
    io: Io,
    alloc: std.mem.Allocator,
    method: []const u8,
    path: []const u8,
    body: []const u8,
    contentType: ?[]const u8,
    contentEncoding: ?[]const u8,
) !HttpResponse {
    var stream: std.Io.net.Stream = .{
        .socket = .{
            // TODO wtf is a handle
            .handle = 0,
            .address = try .parse("127.0.0.1", 9014),
        },
    };
    defer stream.close(io);

    var req: std.ArrayList(u8) = .empty;
    defer req.deinit(alloc);

    try req.print(
        alloc,
        "{s} {s} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\nContent-Length: {d}\r\n",
        .{ method, path, body.len },
    );
    if (contentType) |ct| {
        try req.print(alloc, "Content-Type: {s}\r\n", .{ct});
    }
    if (contentEncoding) |ce| {
        try req.print(alloc, "Content-Encoding: {s}\r\n", .{ce});
    }
    // TODO not sure
    try req.appendSlice(alloc, "\r\n");
    if (body.len > 0) {
        // TODO not sure
        try req.appendSlice(alloc, body);
    }

    var buf: [4096]u8 = undefined;

    var writer = stream.writer(io, &buf);
    try writer.interface.writeAll(req.items);

    req.clearRetainingCapacity();

    while (true) {
        var reader = stream.reader(io, &buf);
        const n = try reader.interface.readSliceShort(&buf);

        if (n == 0) break;
        try req.appendSlice(alloc, buf[0..n]);
    }

    const raw = try req.toOwnedSlice(alloc);

    const sep = std.mem.indexOf(u8, raw, "\r\n\r\n") orelse return error.InvalidResponse;
    const head = raw[0..sep];
    const bodyStart = sep + 4;

    const statusLineEnd = std.mem.indexOf(u8, head, "\r\n") orelse head.len;
    const statusLine = head[0..statusLineEnd];
    if (!std.mem.startsWith(u8, statusLine, "HTTP/1.1 ") or statusLine.len < 12) {
        return error.InvalidResponse;
    }
    const statusCode = try std.fmt.parseInt(u16, statusLine[9..12], 10);

    const responseBody = try alloc.dupe(u8, raw[bodyStart..]);
    alloc.free(raw);

    return .{ .statusCode = statusCode, .body = responseBody };
}

fn waitUntilReady(io: Io, alloc: std.mem.Allocator) !void {
    const start = Io.Timestamp.now(io, .real).nanoseconds;
    const timeoutNs = 10 * std.time.ns_per_s;

    while (Io.Timestamp.now(io, .real).nanoseconds - start < timeoutNs) {
        const resp = request(io, alloc, "GET", "/insert/loki/ready", "", null, null) catch {
            try Io.sleep(io, .fromMilliseconds(50), .real);
            continue;
        };
        defer {
            var toFree = resp;
            toFree.deinit(alloc);
        }

        if (resp.statusCode == 200) {
            return;
        }
        try Io.sleep(io, .fromMilliseconds(50), .real);
    }

    return error.Timeout;
}

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
        // TODO Not sure
        std.Io.Threaded.chdir(oldCwd) catch {};
        alloc.free(oldCwd);
    }

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const tmpPath = try tmp.dir.realPathFileAlloc(io, ".", alloc);
    defer alloc.free(tmpPath);
    // TODO Not sure
    try std.Io.Threaded.chdir(tmpPath);

    const conf = Conf.default(alloc);
    const ServerThread = struct {
        fn run(threadAllocator: std.mem.Allocator, threadConf: Conf) void {
            server.startServer(io, threadAllocator, threadConf) catch |err| {
                std.debug.print("Server error: {}\n", .{err});
            };
        }
    };

    var thread = Io.async(io, ServerThread.run, .{ alloc, conf });
    errdefer {
        thread.cancel(io);
        std.posix.kill(std.c.getpid(), std.posix.SIG.TERM) catch {};
    }

    try waitUntilReady(io, alloc);

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
        var resp = try request(
            io,
            alloc,
            "POST",
            "/insert/loki/api/v1/push",
            compressed[0..compressedLen],
            "application/json",
            "snappy",
        );
        defer resp.deinit(alloc);
        try std.testing.expectEqual(@as(u16, 200), resp.statusCode);
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
            var resp = try request(io, alloc, "POST", "/query", queryJson, "application/json", null);
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
    thread.await(io);
}
