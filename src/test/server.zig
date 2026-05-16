const std = @import("std");
const Io = std.Io;
const Allocator = std.mem.Allocator;
const Dir = Io.Dir;
const snappy = @import("snappy").raw;

const MemOrder = @import("../stds/sort.zig").MemOrder;

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
        tenant: []const u8,
        contentType: []const u8,
        contentEncoding: ?[]const u8,
    ) !HttpResponse {
        const url = try std.fmt.allocPrint(alloc, "{s}{s}", .{ client.host, path });
        defer alloc.free(url);

        var responseWriter = try std.Io.Writer.Allocating.initCapacity(alloc, 1024);
        defer responseWriter.deinit();

        var headersBuf: [3]std.http.Header = undefined;
        var headersLen: usize = 0;
        if (contentType.len != 0) {
            headersBuf[headersLen] = .{ .name = "content-type", .value = contentType };
            headersLen += 1;
        }
        headersBuf[headersLen] = .{ .name = "X-Scope-OrgID", .value = tenant };
        headersLen += 1;
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
            var resp = client.request(alloc, .GET, "/insert/loki/ready", "", "", "application/json", null) catch |err| {
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

const IngestionLog = struct {
    offsetMin: i64,
    message: []const u8,
    fields: std.json.Value,
};
const IngestCorpus = struct {
    tenant: []const u8,
    stream: std.json.Value,
    logs: []IngestionLog,
};

const QueryCorpus = struct {
    description: []const u8,
    tenant: []const u8,
    query: []const u8,
    match: []const []const u8,
};

pub const QueryTestCorpus = struct {
    name: []const u8,
    ingest: IngestCorpus,
    queries: []QueryCorpus,
};

pub fn queryTestLessThan(_: void, first: QueryTestCorpus, second: QueryTestCorpus) bool {
    return std.mem.lessThan(u8, first.name, second.name);
}

const CorporaReader = struct {
    dirPath: []const u8,

    ingestJson: std.ArrayList(std.json.Parsed(IngestCorpus)) = .empty,
    queriesJson: std.ArrayList(std.json.Parsed([]QueryCorpus)) = .empty,

    fn read(self: *CorporaReader, io: Io, alloc: Allocator) !std.ArrayList(QueryTestCorpus) {
        const corporaDirName = "src/test/corpora";
        const ingestFileName = "ingest.json";
        const queriesFileName = "queries.json";

        var fullPathBuf: [std.fs.max_path_bytes]u8 = undefined;
        var w = Io.Writer.fixed(&fullPathBuf);
        try std.fs.path.fmtJoin(&.{ self.dirPath, corporaDirName }).format(&w);

        const openDir = try Dir.openDirAbsolute(io, w.buffer[0..w.end], .{
            .iterate = true,
        });
        defer openDir.close(io);

        var tests: std.ArrayList(QueryTestCorpus) = .empty;
        errdefer tests.deinit(alloc);

        var iter = Dir.iterate(openDir);
        while (true) {
            const entry = try iter.next(io) orelse break;
            if (entry.kind != .directory) {
                continue;
            }

            w.end = 0;
            try std.fs.path.fmtJoin(&.{ self.dirPath, corporaDirName, entry.name, ingestFileName }).format(&w);
            const parsedIngest = try parseTestFile(IngestCorpus, io, alloc, w.buffer[0..w.end]);
            errdefer parsedIngest.deinit();

            w.end = 0;
            try std.fs.path.fmtJoin(&.{ self.dirPath, corporaDirName, entry.name, queriesFileName }).format(&w);
            const parsedQueries = try parseTestFile([]QueryCorpus, io, alloc, w.buffer[0..w.end]);
            errdefer parsedQueries.deinit();

            try tests.append(alloc, .{
                .name = entry.name,
                .ingest = parsedIngest.value,
                .queries = parsedQueries.value,
            });

            try self.ingestJson.append(alloc, parsedIngest);
            try self.queriesJson.append(alloc, parsedQueries);
        }

        std.sort.pdq(QueryTestCorpus, tests.items, {}, queryTestLessThan);
        return tests;
    }

    fn deinit(self: *CorporaReader, alloc: Allocator) void {
        for (self.ingestJson.items) |parsed| {
            parsed.deinit();
        }
        self.ingestJson.deinit(alloc);
        for (self.queriesJson.items) |parsed| {
            parsed.deinit();
        }
        self.queriesJson.deinit(alloc);
    }
};

fn parseTestFile(comptime T: type, io: Io, alloc: Allocator, filePath: []const u8) !std.json.Parsed(T) {
    const file = try std.Io.Dir.openFileAbsolute(io, filePath, .{});
    defer file.close(io);

    var readBuf: [4096]u8 = undefined;
    var fileBuf: [4096]u8 = undefined;
    var reader = file.reader(io, &readBuf);
    const n = try reader.interface.readSliceShort(&fileBuf);
    if (n == 0) {
        return error.NotEnoughBufferSize;
    }

    return std.json.parseFromSlice(T, alloc, fileBuf[0..n], .{
        .ignore_unknown_fields = true,
        .allocate = .alloc_always,
    });
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

    var corporaReader = CorporaReader{ .dirPath = oldCwd };
    defer corporaReader.deinit(alloc);
    var corpora = try corporaReader.read(io, alloc);
    defer corpora.deinit(alloc);

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

    var serverFuture = try Io.concurrent(io, ServerThread.run, .{ std.testing.allocator, conf });
    defer serverFuture.await(io);
    defer std.posix.kill(std.c.getpid(), std.posix.SIG.TERM) catch {};

    var ochiClient: OchiClient = .{
        .host = "http://localhost:9014",
        .client = .{
            .allocator = alloc,
            .io = io,
        },
    };
    defer ochiClient.client.deinit();

    try ochiClient.waitUntilReady(io, alloc, .fromSeconds(1));

    var corpusArena = std.heap.ArenaAllocator.init(alloc);
    defer corpusArena.deinit();
    const arenAlloc = corpusArena.allocator();
    for (corpora.items) |corpus| {
        const nowNs = Io.Timestamp.now(io, .real).nanoseconds;
        try runCorpus(arenAlloc, &ochiClient, corpus, @intCast(nowNs));
        _ = corpusArena.reset(.retain_capacity);
    }
}

fn runCorpus(alloc: Allocator, client: *OchiClient, corpus: QueryTestCorpus, nowNs: u64) !void {
    {
        const ingestBody = try buildIngestBody(alloc, corpus.ingest, nowNs);
        std.debug.print("Ingest body: {s}\n", .{ingestBody});
        defer alloc.free(ingestBody);
        const maxCompressedLen = snappy.maxCompressedLength(ingestBody.len);
        const compressed = try alloc.alloc(u8, maxCompressedLen);
        defer alloc.free(compressed);
        const compressedLen = try snappy.compress(ingestBody, compressed);

        var resp = try client.request(
            alloc,
            .POST,
            "/insert/loki/api/v1/push",
            compressed[0..compressedLen],
            corpus.ingest.tenant,
            "application/json",
            "snappy",
        );
        defer resp.deinit(alloc);
        std.testing.expectEqual(200, resp.statusCode) catch |err| {
            std.debug.print("Ingest request failed, reponse body: {s}\n", .{resp.body});
            return err;
        };
    }

    {
        var resp = try client.request(alloc, .POST, "/flush", "", corpus.ingest.tenant, "", null);
        defer resp.deinit(alloc);
        try std.testing.expectEqual(200, resp.statusCode);
    }

    for (corpus.queries) |query| {
        var resp = try client.request(alloc, .POST, "/query", query.query, query.tenant, "application/loql", null);
        defer resp.deinit(alloc);
        std.testing.expectEqual(200, resp.statusCode) catch |err| {
            std.debug.print("Query request failed, reponse body: {s}\n", .{resp.body});
            return err;
        };

        const parsed = try std.json.parseFromSlice([]QueryLine, alloc, resp.body, .{
            .ignore_unknown_fields = true,
        });
        defer parsed.deinit();

        var fetchedIDs = std.ArrayList([]const u8).empty;
        defer fetchedIDs.deinit(alloc);
        for (parsed.value) |line| {
            for (line.fields) |field| {
                if (std.mem.eql(u8, field.key, "id")) {
                    try fetchedIDs.append(alloc, field.value);
                    break;
                }
            }
        }

        try std.testing.expectEqual(fetchedIDs.items.len, query.match.len);
        for (query.match, 0..query.match.len) |expectedID, i| {
            try std.testing.expectEqualStrings(expectedID, fetchedIDs.items[i]);
        }
    }
}

fn buildIngestBody(alloc: Allocator, ingest: IngestCorpus, nowNs: u64) ![]const u8 {
    const streamJson = try std.json.Stringify.valueAlloc(alloc, ingest.stream, .{});
    defer alloc.free(streamJson);

    var buf = std.ArrayList(u8).empty;
    errdefer buf.deinit(alloc);

    try buf.appendSlice(alloc, "{\"streams\":[{\"stream\":");
    try buf.appendSlice(alloc, streamJson);
    try buf.appendSlice(alloc, ",\"values\":[");
    for (ingest.logs, 0..) |log, i| {
        if (i != 0) {
            try buf.append(alloc, ',');
        }

        const offsetNs = @as(i128, log.offsetMin) * std.time.ns_per_min;
        const tsNsSigned = @as(i128, nowNs) + offsetNs;
        if (tsNsSigned < 0 or tsNsSigned > std.math.maxInt(u64)) {
            return error.InvalidTimestamp;
        }
        const tsNs: u64 = @intCast(tsNsSigned);

        const fieldsJson = try std.json.Stringify.valueAlloc(alloc, log.fields, .{});
        defer alloc.free(fieldsJson);

        try buf.appendSlice(alloc, "[\"");
        const tsString = try std.fmt.allocPrint(alloc, "{d}", .{tsNs});
        defer alloc.free(tsString);
        try buf.appendSlice(alloc, tsString);
        try buf.appendSlice(alloc, "\",\"");
        try buf.appendSlice(alloc, log.message);
        try buf.appendSlice(alloc, "\",");
        try buf.appendSlice(alloc, fieldsJson);
        try buf.appendSlice(alloc, "]");
    }
    try buf.appendSlice(alloc, "]}]}");

    return buf.toOwnedSlice(alloc);
}

// TODO: test querying fields with "." in a key/value
