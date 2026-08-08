/// insert module provides write path for Ochi
const std = @import("std");
const Io = std.Io;

const tracy = @import("tracy");
const httpz = @import("httpz");

const AppContext = @import("../dispatch.zig").AppContext;
const Store = @import("../Store.zig").Store;
const Accumulator = @import("../Accumulator.zig");
const AccumulatorPool = @import("../AccumulatorPool.zig");
const TimerLoop = @import("../stds/xev/TimerLoop.zig");
const Params = Accumulator.Params;
const Field = @import("../store/lines.zig").Field;
const defaultMaxFieldsPerLine = @import("../store/lines.zig").defaultMaxFieldsPerLine;
const ApiError = @import("../server/error.zig").ApiError;
const Compression = @import("../server/compression.zig").Compression;
const Logger = @import("logging");

// TODO: document API in a typed spec
// and find a way to test it extensively
// to reproduce all the errors

/// ingestLokiJsonHandler defines a loki json insertion operation
pub fn ingestLokiJsonHandler(ctx: *AppContext, r: *httpz.Request, res: *httpz.Response) !void {
    const z = tracy.Zone.begin(.{
        .src = @src(),
        .name = "ingestLokiJsonHandler",
    });
    defer z.end();

    const contentType = r.headers.get("content-type");

    if (contentType != null and !std.mem.eql(u8, "application/json", contentType.?)) {
        ctx.request.diagnostic.set(.{ .key = "req.contentType", .value = contentType.? });
        // TODO: implement protobuf marhsalling
        return ApiError.ContentTypeNotSupported;
    }
    // TODO: consider using streaming reader of the body,
    // currently the entire body is pre-read by the start of the API handler
    const body = r.body() orelse return ApiError.EmptyBody;

    // TODO: validate a disk has enough space
    const encoding = r.headers.get("content-encoding") orelse "";
    const compress = Compression.fromEncoding(encoding) catch
        return ApiError.ContentEncodingNotSupported;

    const uncompressed = compress.uncompress(res.arena, body) catch
        return ApiError.DecompressFailed;
    defer res.arena.free(uncompressed);

    const params = Params{ .tenantID = ctx.request.tenantID };

    try process(ctx.io, res.arena, ctx, uncompressed, params);
    // process(ctx.io, res.arena, ctx, uncompressed, params) catch |err| switch (err) {
    //     ApiError.InvalidTimestamp => return err,
    //     else => return ApiError.FailedToProccess,
    // };

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
    requestArena: std.mem.Allocator,
    ctx: *AppContext,
    data: []const u8,
    params: Params,
) !void {
    const slot = ctx.accumulatorPool.acquire(ctx.io, data.len) catch
        return ApiError.FailedToProccess;
    defer ctx.accumulatorPool.release(ctx.io, slot);

    const root = try std.json.parseFromSliceLeaky(std.json.Value, requestArena, data, .{
        .allocate = .alloc_if_needed,
    });

    // Get "streams" array
    const streams = root.object.get("streams") orelse return error.MissingStreams;
    if (streams != .array) return error.StreamsNotArray;

    // pre allocate labels list
    var tags: std.ArrayList(Field) = .empty;
    defer tags.deinit(requestArena);

    const accumulator = &slot.accumulator;

    // Iterate through each stream
    for (streams.array.items) |stream| {
        if (stream != .object) return error.StreamNotObject;

        {
            var labelSize: u16 = 1; // 1 is for msgKey
            if (stream.object.get("stream")) |streamObj| {
                if (streamObj != .object) return error.StreamFieldNotObject;
                labelSize += @intCast(streamObj.object.count());
            }
            if (stream.object.get("values")) |valuesObj| {
                if (valuesObj == .array and valuesObj.array.items.len > 0) {
                    const firstLine = valuesObj.array.items[0];
                    if (firstLine == .array and firstLine.array.items.len == 3 and
                        firstLine.array.items[2] == .object)
                    {
                        labelSize += @intCast(firstLine.array.items[2].object.count());
                    }
                }
            }
            if (labelSize > defaultMaxFieldsPerLine) return error.MaxFieldsPerLineExceeded;
            try tags.ensureTotalCapacity(requestArena, labelSize);
        }

        if (stream.object.get("stream")) |streamObj| {
            var it = streamObj.object.iterator();
            while (it.next()) |entry| {
                const valueStr = switch (entry.value_ptr.*) {
                    .string => |s| s,
                    else => return error.LabelValueNotString,
                };
                try tags.append(requestArena, .{ .key = entry.key_ptr.*, .value = valueStr });
            }
        }

        const tagsLen = tags.items.len;
        const streamTags = tags.items[0..tagsLen];

        try accumulator.reinit(ctx.allocator, streamTags, params.tenantID);

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
            const tsNs = std.fmt.parseInt(u64, timestampStr, 10) catch |err| switch (err) {
                error.InvalidCharacter => {
                    ctx.request.diagnostic.set(.{ .key = "timestamp", .value = timestampStr });
                    return ApiError.InvalidTimestamp;
                },
                else => return err,
            };

            // Parse structured metadata (if present)
            if (lineArray.len > 2) {
                if (lineArray[2] != .object) return error.StructuredMetadataNotObject;

                var metadata_it = lineArray[2].object.iterator();
                while (metadata_it.next()) |entry| {
                    const value_str = switch (entry.value_ptr.*) {
                        .string => |s| s,
                        else => return error.MetadataValueNotString,
                    };
                    try tags.append(requestArena, .{ .key = entry.key_ptr.*, .value = value_str });
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
            try tags.append(requestArena, .{ .key = "", .value = msg });

            try accumulator.tryAppendLine(io, ctx.allocator, tsNs, tags.items);

            // clean value labels, but retain stream labels
            tags.items.len = tagsLen;
        }

        try ctx.accumulatorPool.afterAppend(io, slot);
        // clean len of the labels len, but retain allocated memory
        tags.clearRetainingCapacity();
    }
}

const testing = std.testing;

const encodeTags = @import("../store/lines.zig").encodeTags;
const makeStreamID = @import("../store/lines.zig").makeStreamID;
const Query = @import("../query/Query.zig");
const Layout = @import("../Layout.zig");
const Runtime = @import("../Runtime.zig");
const Conf = @import("../Conf.zig");
const Consts = @import("../Consts.zig");

test "large body is processed and appears in the query response" {
    const alloc = testing.allocator;
    const io = testing.io;

    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    const rootPath = try tmp.dir.realPathFileAlloc(io, ".", alloc);
    defer alloc.free(rootPath);

    var partitionsPathBuf: [std.fs.max_path_bytes]u8 = undefined;
    const layout = try Layout.make(io, rootPath, &partitionsPathBuf);

    const conf = Conf.getConf();
    const runtime = try Runtime.init(io, alloc, rootPath, conf.app.maxCachePortion);
    defer runtime.deinit(alloc);

    var store = try Store.init(io, alloc, &conf, runtime, layout);
    defer store.deinit(io, alloc);

    var diagnostic: Logger.Diagnostic = .{};
    const timerLoop = try TimerLoop.init(alloc);
    defer timerLoop.deinit();
    const accumulatorPool = try AccumulatorPool.init(io, alloc, &store, timerLoop, 1);
    defer accumulatorPool.deinit(alloc);

    var ctx = AppContext{
        .io = io,
        .allocator = alloc,
        .conf = undefined,
        .store = &store,
        .dispatchMeter = undefined,
        .storeMeter = undefined,
        .accumulatorPool = accumulatorPool,
        .request = &.{
            .tenantID = 0,
            .diagnostic = &diagnostic,
        },
    };

    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const a = arena.allocator();

    // enough lines * message size to exceed Consts.maxBlockSize (the accumulator's inner buffer),
    // forcing at least one internal flush to the store mid-request
    const lineCount = 200;
    const msgSize = 15_000; // stays under defaultMaxFieldValueSize (16KiB)
    try testing.expect(lineCount * msgSize > Consts.maxBlockSize);

    const message = try a.alloc(u8, msgSize);
    @memset(message, 'x');

    const nowNs: u64 = @intCast(Io.Timestamp.now(io, .real).nanoseconds);

    var body: std.ArrayList(u8) = try .initCapacity(a, 4 * 1024 * 1024);
    body.appendSliceAssumeCapacity("{\"streams\":[{\"stream\":{\"app\":\"large\"},\"values\":[");
    for (0..lineCount) |i| {
        if (i != 0) body.appendAssumeCapacity(',');
        const entry = try std.fmt.allocPrint(a, "[\"{d}\",\"{s}\"]", .{ nowNs + i, message });
        defer a.free(entry);
        body.appendSliceAssumeCapacity(entry);
    }
    body.appendSliceAssumeCapacity("]}]}");

    try process(io, a, &ctx, body.items, .{ .tenantID = ctx.request.tenantID });

    var tags = [_]Field{.{ .key = "app", .value = "large" }};
    const encodedTags = try encodeTags(a, tags[0..]);
    const sid = makeStreamID(ctx.request.tenantID, encodedTags).id;

    const query = Query{
        .streamIDs = &.{sid},
        .start = 0,
        .end = nowNs + std.time.ns_per_hour,
    };

    // assert accumulator has a left over the flushed body,
    // but only part of it since the body doesn't fit its buffer
    const pendingLines = accumulatorPool.slots[0].accumulator.lines.items.len;
    try testing.expect(pendingLines > 0);
    try testing.expect(pendingLines < lineCount);

    // assert the rest of the lines available after flush
    try accumulatorPool.flushAll(io);
    try store.flush(io, alloc);

    var lines = try ctx.store.queryLines(io, a, alloc, ctx.request.tenantID, query);
    defer lines.deinit(a);

    try testing.expectEqual(lineCount, lines.items.len);
    for (lines.items) |line| {
        var found = false;
        for (line.fields) |field| {
            // _msg found
            if (field.key.len == 0) {
                try testing.expectEqualStrings(message, field.value);
                found = true;
            }
        }
        try testing.expect(found);
    }
}
