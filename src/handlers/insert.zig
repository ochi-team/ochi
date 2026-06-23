/// insert module provides write path for Ochi
const std = @import("std");
const Io = std.Io;

const httpz = @import("httpz");
const AppContext = @import("../dispatch.zig").AppContext;
const Store = @import("../Store.zig").Store;
const Processor = @import("../process.zig").Processor;
const Field = @import("../store/lines.zig").Field;
const Params = @import("../process.zig").Params;
const ApiError = @import("../server/error.zig").ApiError;
const Compression = @import("../server/compression.zig").Compression;

// TODO: document API in a typed spec
// and find a way to test it extensively
// to reproduce all the errors

/// ingestLokiJsonHandler defines a loki json insertion operation
pub fn ingestLokiJsonHandler(ctx: *AppContext, r: *httpz.Request, res: *httpz.Response) ApiError!void {
    const contentType = r.headers.get("content-type");

    if (contentType != null and !std.mem.eql(u8, "application/json", contentType.?)) {
        ctx.diagnostic.set(.{ .key = "req.contentType", .value = contentType.? });
        // TODO: implement protobuf marhsalling
        return ApiError.ContentTypeNotSupported;
    }
    // TODO: consider using concurrent reader of the body,
    // currently the entire body is pre-read by the start of the API handler
    const body = r.body() orelse return ApiError.EmptyBody;

    if (body.len > ctx.conf.maxRequestSize) {
        return ApiError.MaxBodySize;
    }

    // TODO: validate a disk has enough space
    const encoding = r.headers.get("content-encoding") orelse "";
    const compress = Compression.fromEncoding(encoding) catch
        return ApiError.ContentEncodingNotSupported;

    const uncompressed = compress.uncompress(res.arena, body) catch
        return ApiError.DecompressFailed;
    defer res.arena.free(uncompressed);

    const params = Params{ .tenantID = ctx.tenantID };

    var parseArena = std.heap.ArenaAllocator.init(ctx.allocator);
    defer parseArena.deinit();

    process(ctx.io, parseArena.allocator(), ctx.allocator, ctx, uncompressed, params) catch
        return ApiError.FailedToProccess;

    res.status = 200;
}

/// ingestLokiReady defines a loki handler to signal its readiness
pub fn ingestLokiReady(_: *AppContext, _: *httpz.Request, res: *httpz.Response) !void {
    res.status = 200;
    res.body = "ready";
}

/// docs for more info: https://grafana.com/docs/loki/latest/reference/loki-http-api/#ingest-logs
fn process(
    io: Io,
    parseAlloc: std.mem.Allocator,
    ingestAlloc: std.mem.Allocator,
    ctx: *AppContext,
    data: []const u8,
    params: Params,
) !void {
    var parser = LokiJsonParser.init(ingestAlloc, parseAlloc, data);
    defer parser.deinit();

    var tags: std.ArrayList(Field) = .empty;
    defer tags.deinit(parseAlloc);

    var processor = Processor.empty(ctx.store);
    defer processor.deinit(ingestAlloc);

    try parser.parse(io, ingestAlloc, parseAlloc, &processor, &tags, params);
}

const LokiJsonParser = struct {
    scanner: std.json.Scanner,
    stringAlloc: std.mem.Allocator,
    maxValueLen: usize,

    fn init(stackAlloc: std.mem.Allocator, stringAlloc: std.mem.Allocator, data: []const u8) LokiJsonParser {
        return .{
            .scanner = std.json.Scanner.initCompleteInput(stackAlloc, data),
            .stringAlloc = stringAlloc,
            .maxValueLen = data.len,
        };
    }

    fn deinit(self: *LokiJsonParser) void {
        self.scanner.deinit();
    }

    fn parse(
        self: *LokiJsonParser,
        io: Io,
        ingestAlloc: std.mem.Allocator,
        parseAlloc: std.mem.Allocator,
        processor: *Processor,
        tags: *std.ArrayList(Field),
        params: Params,
    ) !void {
        try self.expect(.object_begin, error.RootNotObject);

        var seenStreams = false;
        while (true) {
            const token = try self.next();
            switch (token) {
                .object_end => break,
                .string, .allocated_string => |key| {
                    if (std.mem.eql(u8, key, "streams")) {
                        seenStreams = true;
                        try self.parseStreams(io, ingestAlloc, parseAlloc, processor, tags, params);
                    } else {
                        try self.scanner.skipValue();
                    }
                },
                else => return error.SyntaxError,
            }
        }

        if (!seenStreams) return error.MissingStreams;
        try self.expect(.end_of_document, error.SyntaxError);
    }

    fn parseStreams(
        self: *LokiJsonParser,
        io: Io,
        ingestAlloc: std.mem.Allocator,
        parseAlloc: std.mem.Allocator,
        processor: *Processor,
        tags: *std.ArrayList(Field),
        params: Params,
    ) !void {
        try self.expect(.array_begin, error.StreamsNotArray);

        while (true) {
            const token = try self.next();
            switch (token) {
                .array_end => break,
                .object_begin => try self.parseStream(io, ingestAlloc, parseAlloc, processor, tags, params),
                else => return error.StreamNotObject,
            }
        }
    }

    fn parseStream(
        self: *LokiJsonParser,
        io: Io,
        ingestAlloc: std.mem.Allocator,
        parseAlloc: std.mem.Allocator,
        processor: *Processor,
        tags: *std.ArrayList(Field),
        params: Params,
    ) !void {
        var seenStream = false;
        var seenValues = false;

        while (true) {
            const token = try self.next();
            switch (token) {
                .object_end => break,
                .string, .allocated_string => |key| {
                    if (std.mem.eql(u8, key, "stream")) {
                        try self.parseLabels(parseAlloc, tags);
                        seenStream = true;
                    } else if (std.mem.eql(u8, key, "values")) {
                        if (!seenStream) return error.MissingStream;
                        seenValues = true;
                        try self.parseValues(io, ingestAlloc, parseAlloc, processor, tags, params);
                    } else {
                        try self.scanner.skipValue();
                    }
                },
                else => return error.SyntaxError,
            }
        }

        if (!seenValues) return error.MissingValues;
        tags.clearRetainingCapacity();
    }

    fn parseLabels(
        self: *LokiJsonParser,
        parseAlloc: std.mem.Allocator,
        tags: *std.ArrayList(Field),
    ) !void {
        try self.expect(.object_begin, error.StreamFieldNotObject);

        while (true) {
            const token = try self.next();
            switch (token) {
                .object_end => break,
                .string, .allocated_string => |key| {
                    const valueStr = try self.expectString(error.LabelValueNotString);
                    try tags.append(parseAlloc, .{ .key = key, .value = valueStr });
                },
                else => return error.SyntaxError,
            }
        }
    }

    fn parseValues(
        self: *LokiJsonParser,
        io: Io,
        ingestAlloc: std.mem.Allocator,
        parseAlloc: std.mem.Allocator,
        processor: *Processor,
        tags: *std.ArrayList(Field),
        params: Params,
    ) !void {
        try self.expect(.array_begin, error.ValuesNotArray);

        const tagsLen = tags.items.len;
        const streamTags = tags.items[0..tagsLen];
        try processor.reinit(ingestAlloc, streamTags, params.tenantID);

        while (true) {
            const token = try self.next();
            switch (token) {
                .array_end => break,
                .array_begin => try self.parseLine(io, ingestAlloc, parseAlloc, processor, tags, tagsLen),
                else => return error.LineNotArray,
            }
        }

        processor.tags = tags.items[0..tagsLen];
        try processor.flush(io, ingestAlloc);
    }

    fn parseLine(
        self: *LokiJsonParser,
        io: Io,
        ingestAlloc: std.mem.Allocator,
        parseAlloc: std.mem.Allocator,
        processor: *Processor,
        tags: *std.ArrayList(Field),
        tagsLen: usize,
    ) !void {
        const timestampStr = try self.expectString(error.TimestampNotString);
        const tsNs = try std.fmt.parseInt(u64, timestampStr, 10);
        const msg = try self.expectString(error.MessageNotString);

        const token = try self.next();
        switch (token) {
            .array_end => {},
            .object_begin => {
                try self.parseMetadata(parseAlloc, tags);
                try self.expect(.array_end, error.InvalidLineArrayLength);
            },
            else => return error.InvalidLineArrayLength,
        }

        // TODO: support a flag to parse msg as json
        // it requires 2 more options: parseJsonMsg and msgField,
        // first defines whether the parins is required,
        // second is optional and defines what field in the given json is read as a _msg field
        try tags.append(parseAlloc, .{ .key = "", .value = msg });

        // TODO: we push every line with all the labels including the tags,
        // as a result we duplicated a lot of data,
        // we have to think how to hold the tags separately in the block
        // or even store them only in a stream index
        processor.tags = tags.items[0..tagsLen];
        try processor.pushLine(io, ingestAlloc, tsNs, tags.items);

        tags.items.len = tagsLen;
    }

    fn parseMetadata(
        self: *LokiJsonParser,
        parseAlloc: std.mem.Allocator,
        tags: *std.ArrayList(Field),
    ) !void {
        while (true) {
            const token = try self.next();
            switch (token) {
                .object_end => break,
                .string, .allocated_string => |key| {
                    const valueStr = try self.expectString(error.MetadataValueNotString);
                    try tags.append(parseAlloc, .{ .key = key, .value = valueStr });
                },
                else => return error.SyntaxError,
            }
        }
    }

    fn next(self: *LokiJsonParser) !std.json.Scanner.Token {
        return self.scanner.nextAllocMax(self.stringAlloc, .alloc_if_needed, self.maxValueLen);
    }

    fn expectString(self: *LokiJsonParser, err: anyerror) ![]const u8 {
        const token = try self.next();
        return switch (token) {
            .string, .allocated_string => |s| s,
            else => err,
        };
    }

    fn expect(self: *LokiJsonParser, comptime expected: std.meta.Tag(std.json.Scanner.Token), err: anyerror) !void {
        const token = try self.next();
        if (std.meta.activeTag(token) != expected) return err;
    }
};

const testing = std.testing;

// TODO: move this test to corpora
test "process does not panic when values has three lines" {
    var store: Store = undefined;
    var ctx = AppContext{
        .io = testing.io,
        .allocator = testing.allocator,
        .conf = undefined,
        .tenantID = 0,
        .store = &store,
        .dispatchMeter = undefined,
        .storeMeter = undefined,
    };
    // process uses leaky parsing, so we rely on arena
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();

    const body =
        \\{"streams":[{"stream":{"app":"api"},"values":[
        \\["bad-ts","line-1"],
        \\["1778922991218871001","line-2"],
        \\["1778922991218871002","line-3"]]}]}
    ;

    try testing.expectError(error.InvalidCharacter, process(testing.io, arena.allocator(), testing.allocator, &ctx, body, .{ .tenantID = ctx.tenantID }));
}
