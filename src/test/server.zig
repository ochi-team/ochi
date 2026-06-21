const std = @import("std");
const Io = std.Io;
const Allocator = std.mem.Allocator;
const Dir = Io.Dir;
const snappy = @import("snappy").raw;

const encodeTags = @import("../store/lines.zig").encodeTags;
const makeStreamID = @import("../store/lines.zig").makeStreamID;
const Field = @import("../store/lines.zig").Field;
const SID = @import("../store/lines.zig").SID;
const fieldLessThan = @import("../store/lines.zig").fieldLessThan;
const MemOrder = @import("../stds/sort.zig").MemOrder;
const Runtime = @import("../Runtime.zig");
const Store = @import("../Store.zig").Store;
const Layout = @import("../Layout.zig");
const Logger = @import("logging");

fn makeSID(alloc: Allocator, tenantID: u64, tags: std.json.Value) !u128 {
    if (tags != .object) return error.TagsMustBeObject;

    const tagsCount = tags.object.count();
    var parsedTags = try alloc.alloc(Field, tagsCount);
    defer alloc.free(parsedTags);

    var idx: usize = 0;
    var it = tags.object.iterator();
    while (it.next()) |entry| {
        const value = switch (entry.value_ptr.*) {
            .string => |s| s,
            else => return error.TagValueMustBeString,
        };
        parsedTags[idx] = .{
            .key = entry.key_ptr.*,
            .value = value,
        };
        idx += 1;
    }

    std.sort.pdq(Field, parsedTags, {}, fieldLessThan);

    const encodedTags = try encodeTags(alloc, parsedTags);
    defer alloc.free(encodedTags);

    return makeStreamID(tenantID, encodedTags).id;
}

const Conf = @import("../Conf.zig");
const server = @import("../server.zig");

const QueryLine = struct {
    timestampNs: u64,
    fields: []const Field,
};

const HttpResponse = struct {
    statusCode: u16,
    body: []u8,

    fn deinit(self: *HttpResponse, alloc: std.mem.Allocator) void {
        alloc.free(self.body);
    }
};

const StreamIDsResponse = struct {
    streamIDs: []const u128,
};

pub const OchiClient = struct {
    host: []const u8,
    client: std.http.Client,
    logger: *Logger.Instance,

    fn request(
        client: *OchiClient,
        alloc: std.mem.Allocator,
        method: std.http.Method,
        path: []const u8,
        body: []const u8,
        tenant: u64,
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

        var tenantStrBuf: [8]u8 = undefined;
        const tenantStr = try std.fmt.bufPrint(&tenantStrBuf, "{d}", .{tenant});
        headersBuf[headersLen] = .{ .name = "X-Scope-OrgID", .value = tenantStr };
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

    pub fn waitUntilReady(client: *OchiClient, io: Io, alloc: std.mem.Allocator, timeout: std.Io.Duration) !void {
        const start = Io.Timestamp.now(io, .real).nanoseconds;

        while ((Io.Timestamp.now(io, .real).nanoseconds - start) < timeout.nanoseconds) {
            var resp = client.request(alloc, .GET, "/ingest/loki/ready", "", 0, "application/json", null) catch |err| {
                client.logger.log(.debug, "server not ready yet", .{ .err = err });
                try Io.sleep(io, .fromMilliseconds(50), .real);
                continue;
            };
            defer resp.deinit(alloc);

            if (resp.statusCode == 200) {
                return;
            }
            client.logger.log(.debug, "server not ready yet", .{ .status = resp.statusCode });
            try Io.sleep(io, .fromMilliseconds(50), .real);
        }

        return error.Timeout;
    }

    pub fn ingestLokiJson(
        client: *OchiClient,
        alloc: Allocator,
        tenant: u64,
        body: []const u8,
    ) !void {
        const maxCompressedLen = snappy.maxCompressedLength(body.len);
        const compressed = try alloc.alloc(u8, maxCompressedLen);
        defer alloc.free(compressed);
        const compressedLen = try snappy.compress(body, compressed);

        var resp = try client.request(
            alloc,
            .POST,
            "/ingest/loki/api/v1/push",
            compressed[0..compressedLen],
            tenant,
            "application/json",
            "snappy",
        );
        defer resp.deinit(alloc);
        std.testing.expectEqual(200, resp.statusCode) catch |err| {
            client.logger.log(.err, "ingest request failed", .{ .body = resp.body });
            return err;
        };
    }

    pub fn flush(
        client: *OchiClient,
        alloc: Allocator,
        tenant: u64,
    ) !void {
        var resp = try client.request(alloc, .POST, "/flush", "", tenant, "", null);
        defer resp.deinit(alloc);
        try std.testing.expectEqual(200, resp.statusCode);
    }

    pub fn expectQueryIDs(
        client: *OchiClient,
        alloc: Allocator,
        tenant: u64,
        query: []const u8,
        expectedIDs: []const []const u8,
    ) !void {
        var resp = try client.request(alloc, .POST, "/query", query, tenant, "application/loql", null);
        defer resp.deinit(alloc);
        std.testing.expectEqual(200, resp.statusCode) catch |err| {
            client.logger.log(.err, "query request failed", .{ .body = resp.body });
            return err;
        };

        const parsed = try std.json.parseFromSlice([]QueryLine, alloc, resp.body, .{
            // TODO: removed sid from the response and switch it false
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

        try std.testing.expectEqual(fetchedIDs.items.len, expectedIDs.len);
        for (expectedIDs, 0..expectedIDs.len) |expectedID, i| {
            try std.testing.expectEqualStrings(expectedID, fetchedIDs.items[i]);
        }
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
    tenant: u64,
    stream: std.json.Value,
    logs: []IngestionLog,

    fn getMinMaxTimestamps(ingest: *const IngestCorpus, nowNs: u64) !struct { min: u64, max: u64 } {
        var minTs: u64 = std.math.maxInt(u64);
        var maxTs: u64 = 0;

        for (ingest.logs) |log| {
            const offsetNs = @as(i128, log.offsetMin) * std.time.ns_per_min;
            const tsNsSigned = @as(i128, nowNs) + offsetNs;
            if (tsNsSigned < 0 or tsNsSigned > std.math.maxInt(u64)) {
                return error.InvalidTimestamp;
            }
            const tsNs: u64 = @intCast(tsNsSigned);
            if (tsNs < minTs) minTs = tsNs;
            if (tsNs > maxTs) maxTs = tsNs;
        }

        return .{ .min = minTs, .max = maxTs };
    }
};

const QueryCorpus = struct {
    description: []const u8,
    tenant: u64,
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
        .ignore_unknown_fields = false,
        .allocate = .alloc_always,
    });
}

test "serverEndToEndViaHTTP" {
    const alloc = std.testing.allocator;
    const io = std.testing.io;
    var logger = try Logger.Instance.init(io, alloc, .{
        .level = .Debug,
        .pool_size = 1,
        .buffer_size = 1 << 10,
        .large_buffer_count = 1,
        .large_buffer_size = 1 << 12,
        .output = .stderr,
        .encoding = .logfmt,
    });
    defer logger.deinit();

    const oldCwd = try std.process.currentPathAlloc(io, alloc);
    defer {
        std.Io.Threaded.chdir(oldCwd) catch |err| {
            logger.log(.err, "cannot chdir", .{ .err = err });
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

    const ServerThread = struct {
        fn run(threadAllocator: std.mem.Allocator) void {
            server.startApp(
                io,
                threadAllocator,
                .{ .release = false, .version = "", .setupLogger = false },
            ) catch |err| {
                Logger.log(.err, "server error", .{ .err = err });
            };
        }
    };
    var serverFuture = try Io.concurrent(io, ServerThread.run, .{std.testing.allocator});
    defer serverFuture.await(io);
    defer std.posix.kill(std.c.getpid(), std.posix.SIG.TERM) catch {};

    var ochiClient: OchiClient = .{
        .host = "http://localhost:9014",
        .client = .{
            .allocator = alloc,
            .io = io,
        },
        .logger = &logger,
    };
    defer ochiClient.client.deinit();

    try ochiClient.waitUntilReady(io, alloc, .fromSeconds(1));

    // TODO: writing deinits in the testing code is overwhelming,
    // we don't need to clean it eventually and use testing alloc only to pass to the server
    var expectedSIDsByTenant = std.AutoHashMap(u64, std.ArrayList(u128)).init(alloc);
    defer {
        var it = expectedSIDsByTenant.iterator();
        while (it.next()) |entry| {
            entry.value_ptr.deinit(alloc);
        }
        expectedSIDsByTenant.deinit();
    }

    var corpusArena = std.heap.ArenaAllocator.init(alloc);
    defer corpusArena.deinit();
    const arenAlloc = corpusArena.allocator();
    for (corpora.items) |corpus| {
        const nowNs: u64 = @intCast(Io.Timestamp.now(io, .real).nanoseconds);
        const expectedSID = try makeSID(arenAlloc, corpus.ingest.tenant, corpus.ingest.stream);

        try runCorpus(arenAlloc, &ochiClient, corpus, nowNs);
        try expectQueryBySIDs(arenAlloc, &ochiClient, corpus, expectedSID, nowNs);

        const gop = try expectedSIDsByTenant.getOrPut(corpus.ingest.tenant);
        if (!gop.found_existing) {
            gop.value_ptr.* = .empty;
        }
        try appendUniqueSID(alloc, gop.value_ptr, expectedSID);

        var it = expectedSIDsByTenant.iterator();
        while (it.next()) |entry| {
            try expectStreamIDs(
                arenAlloc,
                &ochiClient,
                entry.key_ptr.*,
                entry.value_ptr.items,
            );
            _ = corpusArena.reset(.retain_capacity);
        }
    }
}

fn expectStreamIDs(
    alloc: Allocator,
    client: *OchiClient,
    tenant: u64,
    expectedStreamIDs: []const u128,
) !void {
    const nowNs: u64 = @intCast(Io.Timestamp.now(std.testing.io, .real).nanoseconds);
    const Case = struct {
        body: []const u8,
        expectedStreamIDs: []const u128,
    };

    const fromNsToNsBody = try std.fmt.allocPrint(alloc, "{{\"fromNs\":0,\"toNs\":{d}}}", .{nowNs});
    defer alloc.free(fromNsToNsBody);

    const cases = [_]Case{
        .{
            .body = fromNsToNsBody,
            .expectedStreamIDs = expectedStreamIDs,
        },
        .{
            .body = "{\"from\":\"1970-01-01T00:00:00.000000001Z\",\"to\":\"2262-04-11T23:47:16Z\"}",
            .expectedStreamIDs = expectedStreamIDs,
        },
        .{
            .body = "{\"since\":\"1h\"}",
            .expectedStreamIDs = expectedStreamIDs,
        },
        .{
            .body = "{\"fromNs\":1,\"toNs\":1}",
            .expectedStreamIDs = &.{},
        },
    };

    for (cases) |case| {
        try expectStreamIDsByBody(alloc, client, tenant, case.body, case.expectedStreamIDs);
    }
}

fn expectStreamIDsByBody(
    alloc: Allocator,
    client: *OchiClient,
    tenant: u64,
    body: []const u8,
    expectedStreamIDs: []const u128,
) !void {
    var resp = try client.request(
        alloc,
        .POST,
        "/stream_ids",
        body,
        tenant,
        "application/json",
        null,
    );
    defer resp.deinit(alloc);

    std.testing.expectEqual(200, resp.statusCode) catch |err| {
        // TODO: implement a client and move all the error loging to it,
        // it must be comptime configurable
        client.logger.log(.err, "stream_ids request failed", .{ .body = resp.body });
        return err;
    };

    const parsed = try std.json.parseFromSlice(StreamIDsResponse, alloc, resp.body, .{
        .ignore_unknown_fields = false,
    });
    defer parsed.deinit();

    // TODO: avoid copying of test data and the pared value
    const expectedSorted = try alloc.dupe(u128, expectedStreamIDs);
    defer alloc.free(expectedSorted);
    std.sort.pdq(u128, expectedSorted, {}, std.sort.asc(u128));

    const actualSorted = try alloc.dupe(u128, parsed.value.streamIDs);
    defer alloc.free(actualSorted);
    std.sort.pdq(u128, actualSorted, {}, std.sort.asc(u128));

    try std.testing.expectEqualSlices(u128, expectedSorted, actualSorted);
    for (actualSorted) |streamID| {
        // validate it's not 00000, but a generated valid hash
        try std.testing.expect(streamID > 0);
    }
}

fn appendUniqueSID(alloc: Allocator, sids: *std.ArrayList(u128), sid: u128) !void {
    for (sids.items) |existing| {
        if (existing == sid) {
            return;
        }
    }
    try sids.append(alloc, sid);
}

fn expectQueryBySIDs(alloc: Allocator, client: *OchiClient, corpus: QueryTestCorpus, sid: u128, nowNs: u64) !void {
    const tsRange = try corpus.ingest.getMinMaxTimestamps(nowNs);
    const minTs = tsRange.min;
    const maxTs = tsRange.max;

    const body = try std.fmt.allocPrint(
        alloc,
        "{{\"start\":{d},\"end\":{d},\"streamIDs\":[{d}]}}",
        .{ minTs, maxTs, sid },
    );
    defer alloc.free(body);

    var resp = try client.request(
        alloc,
        .POST,
        "/query",
        body,
        corpus.ingest.tenant,
        "application/json",
        null,
    );
    defer resp.deinit(alloc);

    std.testing.expectEqual(200, resp.statusCode) catch |err| {
        client.logger.log(.err, "query-by-sids request failed", .{ .body = resp.body });
        return err;
    };

    const parsed = try std.json.parseFromSlice([]QueryLine, alloc, resp.body, .{
        .ignore_unknown_fields = true,
    });
    defer parsed.deinit();

    // TODO: since the query API is broken and returns sid we can't compare the log lines,
    // fix it and the compare the lines completely
    var actualIDs = std.ArrayList([]const u8).empty;
    defer actualIDs.deinit(alloc);
    for (parsed.value) |line| {
        for (line.fields) |field| {
            if (std.mem.eql(u8, field.key, "id")) {
                try actualIDs.append(alloc, field.value);
                break;
            }
        }
    }

    var expectedIDs = std.ArrayList([]const u8).empty;
    defer expectedIDs.deinit(alloc);
    for (corpus.ingest.logs) |log| {
        if (log.fields != .object) return error.ExpectedFieldsObject;
        const idVal = log.fields.object.get("id") orelse return error.MissingIDField;
        if (idVal != .string) return error.IDFieldMustBeString;
        try expectedIDs.append(alloc, idVal.string);
    }

    std.sort.pdq([]const u8, actualIDs.items, {}, MemOrder(u8).lessThanConst);
    std.sort.pdq([]const u8, expectedIDs.items, {}, MemOrder(u8).lessThanConst);

    try std.testing.expectEqual(expectedIDs.items.len, actualIDs.items.len);
    for (expectedIDs.items, 0..) |expectedID, i| {
        try std.testing.expectEqualStrings(expectedID, actualIDs.items[i]);
    }
}

fn runCorpus(alloc: Allocator, client: *OchiClient, corpus: QueryTestCorpus, nowNs: u64) !void {
    // ingest
    {
        const ingestBody = try buildIngestBody(alloc, corpus.ingest, nowNs);
        client.logger.log(.debug, "ingest body", .{ .body = ingestBody, .corpus = corpus.name });
        client.ingestLokiJson(alloc, corpus.ingest.tenant, ingestBody) catch |err| {
            client.logger.log(.err, "failed to ingest", .{
                .err = err,
                .name = corpus.name,
            });
            return err;
        };
    }

    // flush
    {
        client.flush(alloc, corpus.ingest.tenant) catch |err| {
            client.logger.log(.err, "failed to flush", .{
                .err = err,
                .name = corpus.name,
            });
            return err;
        };
    }

    // query all the test cases
    for (corpus.queries) |query| {
        client.expectQueryIDs(alloc, query.tenant, query.query, query.match) catch |err| {
            client.logger.log(.err, "failed to match query ids", .{
                .err = err,
                .query = query.query,
                .description = query.description,
                .name = corpus.name,
            });
            return err;
        };
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

// TODO: test querying fields with ".", "0", "1", "_", "-", "@", "#", "\", "/" in a key/value
