const std = @import("std");
const Io = std.Io;
const snappy = @import("snappy");

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
    alloc: std.mem.Allocator,
    method: []const u8,
    path: []const u8,
    body: []const u8,
    contentType: ?[]const u8,
    contentEncoding: ?[]const u8,
) !HttpResponse {
    var stream = try std.net.tcpConnectToHost(alloc, "127.0.0.1", 9014);
    defer stream.close();

    var req = std.ArrayList(u8).empty;
    defer req.deinit(alloc);

    try req.writer(alloc).print(
        "{s} {s} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\nContent-Length: {d}\r\n",
        .{ method, path, body.len },
    );
    if (contentType) |ct| {
        try req.writer(alloc).print("Content-Type: {s}\r\n", .{ct});
    }
    if (contentEncoding) |ce| {
        try req.writer(alloc).print("Content-Encoding: {s}\r\n", .{ce});
    }
    try req.writer(alloc).writeAll("\r\n");
    if (body.len > 0) {
        try req.writer(alloc).writeAll(body);
    }

    try stream.writeAll(req.items);

    req.clearRetainingCapacity();
    var buf: [4096]u8 = undefined;
    while (true) {
        const n = try stream.read(&buf);
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

fn waitUntilReady(alloc: std.mem.Allocator) !void {
    const start = std.time.nanoTimestamp();
    const timeoutNs = 10 * std.time.ns_per_s;

    while (std.time.nanoTimestamp() - start < timeoutNs) {
        const resp = request(alloc, "GET", "/insert/loki/ready", "", null, null) catch {
            Io.sleep(50 * std.time.ns_per_ms);
            continue;
        };
        defer {
            var toFree = resp;
            toFree.deinit(alloc);
        }

        if (resp.statusCode == 200) {
            return;
        }
        Io.sleep(50 * std.time.ns_per_ms);
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

    const oldCwd = try std.process.getCwdAlloc(alloc);
    defer {
        std.posix.chdir(oldCwd) catch {};
        alloc.free(oldCwd);
    }

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const tmpPath = try tmp.dir.realPathFileAlloc(io, alloc, ".");
    defer alloc.free(tmpPath);
    try std.posix.chdir(tmpPath);

    const serverAlloc = std.heap.page_allocator;
    const conf = Conf.default(serverAlloc);
    const ServerThread = struct {
        fn run(threadAllocator: std.mem.Allocator, threadConf: Conf) void {
            server.startServer(threadAllocator, threadConf) catch |err| {
                std.debug.print("Server error: {}\n", .{err});
            };
        }
    };

    const thread = try Io.spawn(.{}, ServerThread.run, .{ serverAlloc, conf });
    var stopped = false;
    defer {
        if (!stopped) {
            std.posix.kill(std.c.getpid(), std.posix.SIG.TERM) catch {};
            thread.join();
        }
    }

    try waitUntilReady(alloc);

    const tsNs: u64 = @intCast(std.time.nanoTimestamp());
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
        const queryStart = std.time.nanoTimestamp();
        const queryTimeoutNs = 5 * std.time.ns_per_s;

        while (true) {
            var resp = try request(alloc, "POST", "/query", queryJson, "application/json", null);
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

            if (std.time.nanoTimestamp() - queryStart > queryTimeoutNs) {
                return error.Timeout;
            }

            Io.sleep(50 * std.time.ns_per_ms);
        }
    }

    try std.posix.kill(std.c.getpid(), std.posix.SIG.TERM);
    thread.join();
    stopped = true;
}
