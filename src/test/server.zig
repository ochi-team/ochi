const std = @import("std");
const Io = std.Io;
const Allocator = std.mem.Allocator;
const Dir = Io.Dir;
const snappy = @import("snappy").raw;
const zeit = @import("zeit");

const encodeTags = @import("../store/lines.zig").encodeTags;
const makeStreamID = @import("../store/lines.zig").makeStreamID;
const Field = @import("../store/lines.zig").Field;
const SID = @import("../store/lines.zig").SID;
const Line = @import("../store/lines.zig").Line;
const writeLines = @import("../store/lines.zig").writeLines;
const timestampKey = @import("../store/lines.zig").timestampKey;
const msgKey = @import("../store/lines.zig").msgKey;
const lineLatestFirst = @import("../store/lines.zig").lineLatestFirst;
const fieldLessThan = @import("../store/lines.zig").fieldLessThan;
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

const QueryResponse = struct {
    lines: []std.json.Value,
};

const QuerySyntaxErrorResponse = struct {
    code: []const u8,
    meta: struct {
        locs: []const Loc,
    },

    const Loc = struct {
        line: u16,
        col: u16,
        msg: []const u8,
    };
};

fn responseObjectString(value: std.json.Value, key: []const u8) ?[]const u8 {
    if (value != .object) {
        return null;
    }

    const field = value.object.get(key) orelse return null;
    return switch (field) {
        .string => |s| s,
        else => null,
    };
}

fn stringifyJsonValueList(writer: *std.Io.Writer, values: []const std.json.Value) !void {
    try std.json.Stringify.value(values, .{}, writer);
}

fn lineID(line: Line) ?[]const u8 {
    for (line.fields) |field| {
        if (std.mem.eql(u8, field.key, "id")) {
            return field.value;
        }
    }
    return null;
}

fn filterLinesByIDs(alloc: Allocator, lines: []const Line, ids: []const []const u8) ![]Line {
    var filtered = std.ArrayList(Line).empty;
    errdefer filtered.deinit(alloc);

    // iterate over expected ids to save their order for assertion
    for (ids) |expectedID| {
        for (lines) |line| {
            const id = lineID(line);
            if (id == null) std.debug.panic("every corpus line must contain id field", .{});
            if (std.mem.eql(u8, expectedID, id.?)) {
                try filtered.append(alloc, line);
                break;
            }
        }
    }

    return filtered.toOwnedSlice(alloc);
}

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
        testing.expectEqual(200, resp.statusCode) catch |err| {
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
        try testing.expectEqual(200, resp.statusCode);
    }

    pub fn expectQueryIDs(
        client: *OchiClient,
        alloc: Allocator,
        tenant: u64,
        query: []const u8,
        expectedIDs: []const []const u8,
        expectedLines: []const Line,
    ) !void {
        var resp = try client.request(alloc, .POST, "/query", query, tenant, "application/loql", null);
        defer resp.deinit(alloc);
        testing.expectEqual(200, resp.statusCode) catch |err| {
            client.logger.log(.err, "query request failed", .{ .body = resp.body });
            return err;
        };

        const parsed = try std.json.parseFromSlice(QueryResponse, alloc, resp.body, .{
            .ignore_unknown_fields = false,
        });
        defer parsed.deinit();

        var fetchedIDs = std.ArrayList([]const u8).empty;
        defer fetchedIDs.deinit(alloc);
        for (parsed.value.lines) |line| {
            if (responseObjectString(line, "id")) |id| {
                try fetchedIDs.append(alloc, id);
            }
        }

        // assert ids
        try testing.expectEqual(expectedIDs.len, fetchedIDs.items.len);
        for (expectedIDs, fetchedIDs.items) |expectedID, fetchedID| {
            try testing.expectEqualStrings(expectedID, fetchedID);
        }

        // get the matching line,
        // we must not compare to all the ingested lines,
        // because the query takes only filtered data
        const expectedMatchedLines = try filterLinesByIDs(alloc, expectedLines, expectedIDs);
        defer alloc.free(expectedMatchedLines);
        try client.expectResponseLines(expectedMatchedLines, parsed.value.lines);
    }

    pub fn expectQuerySyntaxError(
        client: *OchiClient,
        alloc: Allocator,
        tenant: u64,
        query: []const u8,
        expectedLocs: []const QuerySyntaxErrorResponse.Loc,
    ) !void {
        var resp = try client.request(alloc, .POST, "/query", query, tenant, "application/loql", null);
        defer resp.deinit(alloc);
        testing.expectEqual(400, resp.statusCode) catch |err| {
            client.logger.log(.err, "query syntax request returned unexpected status", .{
                .body = resp.body,
                .status = resp.statusCode,
            });
            return err;
        };

        const parsed = try std.json.parseFromSlice(QuerySyntaxErrorResponse, alloc, resp.body, .{
            .ignore_unknown_fields = false,
        });
        defer parsed.deinit();

        try testing.expectEqualStrings("QUERY_SYNTAX", parsed.value.code);
        try testing.expectEqualDeep(expectedLocs, parsed.value.meta.locs);
    }

    fn expectResponseLines(client: *const OchiClient, expected: []const Line, actual: []const std.json.Value) !void {
        try testing.expectEqual(expected.len, actual.len);
        for (expected, actual) |expectedLineValue, actualLineValue| {
            expectResponseLine(expectedLineValue, actualLineValue) catch |err| {
                var expectedBuffer: [4096]u8 = undefined;
                var fetchedBuffer: [4096]u8 = undefined;
                var expectedWriter = std.Io.Writer.fixed(&expectedBuffer);
                var fetchedWriter = std.Io.Writer.fixed(&fetchedBuffer);

                try writeLines(&expectedWriter, expected);
                try stringifyJsonValueList(&fetchedWriter, actual);
                client.logger.log(.err, "expected response lines len don't match", .{
                    .expected = expectedWriter.buffer[0..expectedWriter.end],
                    .fetched = fetchedWriter.buffer[0..fetchedWriter.end],
                });
                return err;
            };
        }
    }
};

fn logTimestampNs(nowNs: u64, offsetMinutes: i64) !u64 {
    const offsetNs = offsetMinutes * std.time.ns_per_min;
    const tsNsSigned = @as(i128, nowNs) + offsetNs;
    return @intCast(tsNsSigned);
}

fn expectResponseLine(expected: Line, actual: std.json.Value) !void {
    if (actual != .object) {
        return error.ExpectedLineObject;
    }

    const timestamp = responseObjectString(actual, timestampKey) orelse return error.MissingTimestamp;
    const expectedTime = try zeit.instant(testing.io, .{ .source = .{ .unix_nano = @as(i64, @intCast(expected.timestampNs)) } });
    var timeBuf: [32]u8 = undefined;
    try testing.expectEqualStrings(try expectedTime.time().bufPrint(&timeBuf, .rfc3339), timestamp);

    for (expected.fields) |field| {
        const key = if (field.key.len == 0) msgKey else field.key;
        const actualValue = responseObjectString(actual, key) orelse return error.MissingField;
        try testing.expectEqualStrings(field.value, actualValue);
    }
}

fn expectLoqlSyntaxErrors(alloc: Allocator, client: *OchiClient) !void {
    const Case = struct {
        query: []const u8,
        expectedLocs: []const QuerySyntaxErrorResponse.Loc,
    };

    const cases = [_]Case{
        .{
            .query = "[,] {env=prod}",
            .expectedLocs = &.{
                .{ .line = 1, .col = 5, .msg = "At least one of the time range values must be specified." },
            },
        },
        .{
            .query = "[-5m,now] {env~prod}",
            .expectedLocs = &.{
                .{ .line = 1, .col = 15, .msg = "Expect '}' after tags." },
            },
        },
        .{
            .query = "[-5m,now] {env=prod} message=",
            .expectedLocs = &.{
                .{ .line = 1, .col = 29, .msg = "Expect expression." },
            },
        },
        .{
            .query = "[-5m, now] {v}",
            .expectedLocs = &.{
                .{ .line = 1, .col = 13, .msg = "Expect expression." },
            },
        },
        .{
            .query = "[-5m, now] 42",
            .expectedLocs = &.{
                .{ .line = 1, .col = 12, .msg = "Expect expression." },
            },
        },
        .{
            .query = "[-5m,now] {env=prod} (message=timeout",
            .expectedLocs = &.{
                .{ .line = 1, .col = 31, .msg = "Expect ')' after expression." },
            },
        },
        .{
            .query = "[560,now] {env=prod} (message=timeout",
            .expectedLocs = &.{
                .{ .line = 1, .col = 31, .msg = "Expect ')' after expression." },
            },
        },
        .{
            .query = "[-50000d,now] {env=prod}",
            .expectedLocs = &.{
                .{ .line = 1, .col = 1, .msg = "Relative time range starts before the Unix epoch." },
            },
        },
        .{
            .query = "[-18446744242s,now] job=local",
            .expectedLocs = &.{
                .{ .line = 1, .col = 1, .msg = "Invalid relative time range duration." },
            },
        },
    };

    for (cases) |case| {
        try client.expectQuerySyntaxError(alloc, 0, case.query, case.expectedLocs);
    }
}

fn expectedLinesCorpus(alloc: Allocator, corpus: QueryTestCorpus, nowNs: u64) ![]Line {
    var lineCount: usize = 0;
    for (corpus.ingest.streams) |stream| {
        lineCount += stream.logs.len;
    }

    const expected = try alloc.alloc(Line, lineCount);
    var logI: usize = 0;
    for (corpus.ingest.streams) |stream| {
        if (stream.stream != .object) return error.ExpectedStreamObject;

        const streamFieldsCount = stream.stream.object.count();
        for (stream.logs) |log| {
            if (log.fields != .object) return error.ExpectedFieldsObject;

            const fields = try alloc.alloc(Field, streamFieldsCount + log.fields.object.count() + 1);
            var fieldI: usize = 0;

            var streamIt = stream.stream.object.iterator();
            while (streamIt.next()) |entry| {
                const value = switch (entry.value_ptr.*) {
                    .string => |s| s,
                    else => return error.StreamValueMustBeString,
                };
                fields[fieldI] = .{ .key = entry.key_ptr.*, .value = value };
                fieldI += 1;
            }

            var fieldsIt = log.fields.object.iterator();
            while (fieldsIt.next()) |entry| {
                const value = switch (entry.value_ptr.*) {
                    .string => |s| s,
                    else => return error.FieldValueMustBeString,
                };
                fields[fieldI] = .{ .key = entry.key_ptr.*, .value = value };
                fieldI += 1;
            }

            fields[fieldI] = .{ .key = "", .value = log.message };
            std.sort.pdq(Field, fields, {}, fieldLessThan);

            expected[logI] = .{
                .timestampNs = try logTimestampNs(nowNs, log.offsetMin),
                .fields = fields,
            };
            logI += 1;
        }
    }

    std.sort.pdq(Line, expected, {}, lineLatestFirst);
    return expected;
}

const IngestionLog = struct {
    offsetMin: i64,
    message: []const u8,
    fields: std.json.Value,
};
const IngestStream = struct {
    stream: std.json.Value,
    logs: []IngestionLog,
};
const IngestCorpus = struct {
    tenant: u64,
    streams: []IngestStream,

    fn getMinMaxTimestamps(ingest: *const IngestCorpus, nowNs: u64) !struct { min: u64, max: u64 } {
        var minTs: u64 = std.math.maxInt(u64);
        var maxTs: u64 = 0;

        for (ingest.streams) |stream| {
            for (stream.logs) |log| {
                const tsNs = try logTimestampNs(nowNs, log.offsetMin);
                if (tsNs < minTs) minTs = tsNs;
                if (tsNs > maxTs) maxTs = tsNs;
            }
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
                .name = try alloc.dupe(u8, entry.name),
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

const testing = std.testing;

test "serverEndToEndViaHTTP" {
    const alloc = testing.allocator;
    const io = testing.io;
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
    defer {
        for (corpora.items) |corpus| {
            alloc.free(corpus.name);
        }
        corpora.deinit(alloc);
    }

    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    const tmpPath = try tmp.dir.realPathFileAlloc(io, ".", alloc);
    defer alloc.free(tmpPath);
    try std.Io.Threaded.chdir(tmpPath);

    const ServerThread = struct {
        fn run(threadAllocator: std.mem.Allocator) void {
            // TODO: shitdown the server, start again
            // and validate all the latest corpora queries pass
            server.startApp(
                io,
                threadAllocator,
                .{ .release = false, .version = "", .setupLogger = false },
            ) catch |err| {
                Logger.log(.err, "server error", .{ .err = err });
                std.debug.panic("server error: {s}\n", .{@errorName(err)});
            };
        }
    };
    var serverFuture = try Io.concurrent(io, ServerThread.run, .{testing.allocator});
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

    try expectLoqlSyntaxErrors(alloc, &ochiClient);

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
        const expectedSIDs = try arenAlloc.alloc(u128, corpus.ingest.streams.len);
        for (corpus.ingest.streams, 0..) |stream, i| {
            expectedSIDs[i] = try makeSID(arenAlloc, corpus.ingest.tenant, stream.stream);
        }

        try runCorpus(arenAlloc, &ochiClient, corpus, nowNs);
        try expectQueryBySIDs(arenAlloc, &ochiClient, corpus, expectedSIDs, nowNs);

        const gop = try expectedSIDsByTenant.getOrPut(corpus.ingest.tenant);
        if (!gop.found_existing) {
            gop.value_ptr.* = .empty;
        }
        for (expectedSIDs) |expectedSID| {
            try appendUniqueSID(alloc, gop.value_ptr, expectedSID);
        }

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
    const nowNs: u64 = @intCast(Io.Timestamp.now(testing.io, .real).nanoseconds);
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

    testing.expectEqual(200, resp.statusCode) catch |err| {
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

    try testing.expectEqualSlices(u128, expectedSorted, actualSorted);
    for (actualSorted) |streamID| {
        // validate it's not 00000, but a generated valid hash
        try testing.expect(streamID > 0);
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

fn expectQueryBySIDs(alloc: Allocator, client: *OchiClient, corpus: QueryTestCorpus, sids: []const u128, nowNs: u64) !void {
    const tsRange = try corpus.ingest.getMinMaxTimestamps(nowNs);
    const minTs = tsRange.min;
    const maxTs = tsRange.max;

    var body = std.ArrayList(u8).empty;
    defer body.deinit(alloc);

    const prefix = try std.fmt.allocPrint(alloc, "{{\"start\":{d},\"end\":{d},\"streamIDs\":[", .{ minTs, maxTs });
    defer alloc.free(prefix);
    try body.appendSlice(alloc, prefix);

    for (sids, 0..) |sid, i| {
        if (i != 0) {
            try body.append(alloc, ',');
        }
        const sidStr = try std.fmt.allocPrint(alloc, "{d}", .{sid});
        defer alloc.free(sidStr);
        try body.appendSlice(alloc, sidStr);
    }
    try body.appendSlice(alloc, "]}");

    var resp = try client.request(
        alloc,
        .POST,
        "/query",
        body.items,
        corpus.ingest.tenant,
        "application/json",
        null,
    );
    defer resp.deinit(alloc);

    testing.expectEqual(200, resp.statusCode) catch |err| {
        client.logger.log(.err, "query-by-sids request failed", .{ .body = resp.body });
        return err;
    };

    const parsed = try std.json.parseFromSlice(QueryResponse, alloc, resp.body, .{
        .ignore_unknown_fields = false,
    });
    defer parsed.deinit();

    const expected = try expectedLinesCorpus(alloc, corpus, nowNs);
    // TODO: on a first failure it's worth implementing better,
    // iterating over every line, compare fields one by one and log an error,
    // it must take into account the keys might be missing in either
    try client.expectResponseLines(expected, parsed.value.lines);
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
    const expectedLines = try expectedLinesCorpus(alloc, corpus, nowNs);
    for (corpus.queries) |query| {
        client.expectQueryIDs(alloc, query.tenant, query.query, query.match, expectedLines) catch |err| {
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
    var buf = std.ArrayList(u8).empty;
    errdefer buf.deinit(alloc);

    try buf.appendSlice(alloc, "{\"streams\":[");
    for (ingest.streams, 0..) |stream, streamI| {
        if (streamI != 0) {
            try buf.append(alloc, ',');
        }

        const streamJson = try std.json.Stringify.valueAlloc(alloc, stream.stream, .{});
        defer alloc.free(streamJson);

        try buf.appendSlice(alloc, "{\"stream\":");
        try buf.appendSlice(alloc, streamJson);
        try buf.appendSlice(alloc, ",\"values\":[");
        for (stream.logs, 0..) |log, i| {
            if (i != 0) {
                try buf.append(alloc, ',');
            }

            const tsNs = try logTimestampNs(nowNs, log.offsetMin);

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
        try buf.appendSlice(alloc, "]}");
    }
    try buf.appendSlice(alloc, "]}");

    return buf.toOwnedSlice(alloc);
}

// TODO: test querying fields with ".", "0", "1", "_", "-", "@", "#", "\", "/" in a key/value
