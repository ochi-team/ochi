/// insert module provides write path for Ochi
const std = @import("std");

const httpz = @import("httpz");
const AppContext = @import("../dispatch.zig").AppContext;
const Processor = @import("../process.zig").Processor;
const Field = @import("../store/lines.zig").Field;
const Params = @import("../process.zig").Params;
const ApiError = @import("../server/error.zig").ApiError;
const Compression = @import("../server/compression.zig").Compression;

/// insertLokiJson defines a loki json insertion operation
pub fn insertLokiJsonHandler(ctx: *AppContext, r: *httpz.Request, res: *httpz.Response) ApiError!void {
    const contentType = r.headers.get("content-type");

    if (contentType != null and !std.mem.eql(u8, "application/json", contentType.?)) {
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
    const encoding = r.headers.get("content-encoding") orelse "snappy";
    const compress = Compression.fromEncoding(encoding) catch
        return ApiError.ContentEncodingNotSupported;

    const uncompressed = compress.uncompress(res.arena, body) catch
        return ApiError.DecompressFailed;
    defer res.arena.free(uncompressed);

    const params = Params{ .tenantID = ctx.tenantID };

    process(res.arena, std.heap.page_allocator, ctx, uncompressed, params) catch
        return ApiError.FailedToProccess;

    res.status = 200;
}

/// insertLokiReady defines a loki handler to signal its readiness
pub fn insertLokiReady(_: *AppContext, _: *httpz.Request, res: *httpz.Response) !void {
    res.status = 200;
    res.body = "ready";
}

/// docs for more info: https://grafana.com/docs/loki/latest/reference/loki-http-api/#ingest-logs
fn process(
    parseAlloc: std.mem.Allocator,
    ingestAlloc: std.mem.Allocator,
    ctx: *AppContext,
    data: []const u8,
    params: Params,
) !void {
    // TODO: consider implementing a zero allocation parsing

    // TODO: if we use arena for parsing we can do Leaky
    const parsed = try std.json.parseFromSlice(std.json.Value, parseAlloc, data, .{});
    defer parsed.deinit();

    const root = parsed.value;

    // Get "streams" array
    const streams = root.object.get("streams") orelse return error.MissingStreams;
    if (streams != .array) return error.StreamsNotArray;

    // pre allocate labels list
    // TODO: reuse for next requests, put back to the pool
    var labels: std.ArrayList(Field) = .empty;
    defer labels.deinit(parseAlloc);

    if (streams.array.items.len > 0 and streams.array.items[0] == .object) {
        const stream = streams.array.items[0].object;
        // Parse "stream" object (labels) - preallocate for typical label count
        var labelSize: u16 = 1; // 1 is for _msg
        if (stream.get("stream")) |streamObj| {
            if (streamObj == .object) {
                labelSize += @intCast(streamObj.object.count());
            }
        }
        if (stream.get("values")) |valuesObj| {
            if (valuesObj == .array and valuesObj.array.items.len == 3) {
                labelSize += @intCast(valuesObj.array.items[2].object.count());
            }
        }
        labels = try std.ArrayList(Field).initCapacity(parseAlloc, labelSize);
    }

    var processor = Processor.empty(ctx.store);
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
                try labels.append(parseAlloc, .{ .key = entry.key_ptr.*, .value = valueStr });
            }
        }

        const tagsLen = labels.items.len;
        const streamTags = labels.items[0..tagsLen];

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
                    try labels.append(parseAlloc, .{ .key = entry.key_ptr.*, .value = value_str });
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
            // second is optional and defines what field in the given json is read as a _msg field
            try labels.append(parseAlloc, .{ .key = "", .value = msg });

            try processor.pushLine(ingestAlloc, tsNs, labels.items);

            // clean value labels, but retain stream labels
            labels.items.len = tagsLen;
        }

        try processor.flush(ingestAlloc);

        // clean len of the labels len, but retain allocated memory
        labels.clearRetainingCapacity();
    }
}
