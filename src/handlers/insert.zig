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
const Logger = @import("logging");

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

    // TODO: it's too early to pass page allocator,
    // we might be able to use arena a bit more
    process(ctx.io, res.arena, ctx.allocator, ctx, uncompressed, params) catch
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
    // TODO: implement a zero allocation parsing

    const root = try std.json.parseFromSliceLeaky(std.json.Value, parseAlloc, data, .{
        .allocate = .alloc_if_needed,
    });

    // Get "streams" array
    const streams = root.object.get("streams") orelse return error.MissingStreams;
    if (streams != .array) return error.StreamsNotArray;

    // pre allocate labels list
    var tags: std.ArrayList(Field) = .empty;
    defer tags.deinit(parseAlloc);

    if (streams.array.items.len > 0 and streams.array.items[0] == .object) {
        const stream = streams.array.items[0].object;
        // Parse "stream" object (labels) - preallocate for typical label count
        var labelSize: u16 = 1; // 1 is for msgKey
        if (stream.get("stream")) |streamObj| {
            if (streamObj == .object) {
                labelSize += @intCast(streamObj.object.count());
            }
        }
        if (stream.get("values")) |valuesObj| {
            if (valuesObj == .array and valuesObj.array.items.len > 0) {
                const firstLine = valuesObj.array.items[0];
                if (firstLine == .array and firstLine.array.items.len == 3 and firstLine.array.items[2] == .object) {
                    labelSize += @intCast(firstLine.array.items[2].object.count());
                }
            }
        }
        tags = try std.ArrayList(Field).initCapacity(parseAlloc, labelSize);
    }

    var processor = try Processor.init(ctx.allocator, ctx.store);
    defer processor.deinit(ingestAlloc);

    // Iterate through each stream
    for (streams.array.items) |stream| {
        if (stream != .object) return error.StreamNotObject;

        if (stream.object.get("stream")) |streamObj| {
            if (streamObj != .object) return error.StreamFieldNotObject;

            var it = streamObj.object.iterator();
            while (it.next()) |entry| {
                const valueStr = switch (entry.value_ptr.*) {
                    .string => |s| s,
                    else => return error.LabelValueNotString,
                };
                try tags.append(parseAlloc, .{ .key = entry.key_ptr.*, .value = valueStr });
            }
        }

        const tagsLen = tags.items.len;
        const streamTags = tags.items[0..tagsLen];

        try processor.reinit(ingestAlloc, streamTags, params.tenantID);

        // Parse "values" array
        const values = stream.object.get("values") orelse return error.MissingValues;
        if (values != .array) return error.ValuesNotArray;

        for (values.array.items) |line| {
            if (line != .array) return error.LineNotArray;

            const lineArray = line.array.items;
            if (lineArray.len < 2 or lineArray.len > 3) {
                return error.InvalidLineArrayLength;
            }

            // Parse timestamp
            const timestampStr = switch (lineArray[0]) {
                .string => |s| s,
                else => return error.TimestampNotString,
            };
            const tsNs = try std.fmt.parseInt(u64, timestampStr, 10);

            // Parse structured metadata (if present)
            if (lineArray.len > 2) {
                if (lineArray[2] != .object) return error.StructuredMetadataNotObject;

                var metadata_it = lineArray[2].object.iterator();
                while (metadata_it.next()) |entry| {
                    const value_str = switch (entry.value_ptr.*) {
                        .string => |s| s,
                        else => return error.MetadataValueNotString,
                    };
                    try tags.append(parseAlloc, .{ .key = entry.key_ptr.*, .value = value_str });
                }
            }

            // Parse log message
            const msg = switch (lineArray[1]) {
                .string => |s| s,
                else => return error.MessageNotString,
            };
            // TODO: support a flag to parse msg as json
            // it requires 2 more options: parseJsonMsg and msgField,
            // first defines whether the parins is required,
            // second is optional and defines what field in the given json is read as a `msgKey` field
            try tags.append(parseAlloc, .{ .key = "", .value = msg });

            // TODO: we push every line with all the labels including the tags,
            // as a result we duplicated a lot of data,
            // we have to think how to hold the tags separately in the block
            // or even store them only in a stream index
            try processor.tryAppendLine(io, ingestAlloc, tsNs, tags.items);

            // clean value labels, but retain stream labels
            tags.items.len = tagsLen;
        }

        try processor.flush(io, ingestAlloc);

        // clean len of the labels len, but retain allocated memory
        tags.clearRetainingCapacity();
    }
}

const testing = std.testing;

// TODO: move this test to corpora
test "process does not panic when values has three lines" {
    var store: Store = undefined;
    var diagnostic: Logger.Diagnostic = .{};
    var ctx = AppContext{
        .io = testing.io,
        .allocator = testing.allocator,
        .conf = undefined,
        .tenantID = 0,
        .store = &store,
        .diagnostic = &diagnostic,
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
