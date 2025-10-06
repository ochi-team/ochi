/// insert module provides write path for Seqor
const std = @import("std");

const httpz = @import("httpz");
const snappy = @import("snappy");

const AppContext = @import("dispatch.zig").AppContext;
const Processor = @import("process.zig").Processor;
const Field = @import("process.zig").Field;
const Params = @import("process.zig").Params;

/// insertLokiJson defines a loki json insertion operation
pub fn insertLokiJson(ctx: *AppContext, r: *httpz.Request, res: *httpz.Response) !void {
    const contentType = r.headers.get("Content-Type");
    if (contentType != null and !std.mem.eql(u8, "application/json", contentType.?)) {
        // implement protobuf marhsalling
        res.status = 400;
        res.body = "protobuf content-type is not supported";
        return;
    }
    const params = collectParams(r) catch {
        res.body = "failed to parse params";
        res.status = 500;
        return;
    };
    // TODO: consider using concurrent reader of the body,
    // currently the entire body is pre-read by the start of the API handler
    const body = r.body();
    if (body == null) {
        res.body = "given empty body";
        res.status = 400;
        return;
    }
    if (body.?.len > ctx.conf.maxRequestSize) {
        res.body = "max body size is exceeded";
        res.status = 400;
        return;
    }
    // TODO: validate a disk has enough space
    const encoding = r.headers.get("Content-Encoding") orelse "";
    const uncompressed = uncompress(res.arena, body.?, encoding) catch {
        res.body = "failed to decompress body";
        res.status = 500;
        return;
    };
    // TODO: consider if arena requires defer res.arena.free(uncompressed);

    process(res.arena, params, uncompressed, ctx.processor) catch {
        res.body = "failed to process logs";
        res.status = 500;
        return;
    };

    res.status = 200;
}

/// insertLokiReady defines a loki handler to signal its readiness
pub fn insertLokiReady(_: *AppContext, _: *httpz.Request, res: *httpz.Response) !void {
    res.status = 200;
    res.body = "ready";
}
const LokiParams = struct {
    params: Params,
    // parseLoki defines whether the given loki message must be parsed as a json
    // and broken down instead of storing is a plain text
    // TODO: implement its handling
    parseLoki: bool,
};

fn collectParams(r: *httpz.Request) !Params {
    _ = r;
    // TODO: implemented me
    return .{
        .tenant = .{ .accountID = 0, .projectID = 0 },
        .streamFields = null,
        .extraStreamFields = &.{},
    };
}

fn uncompress(allocator: std.mem.Allocator, body: []const u8, encoding: []const u8) ![]const u8 {
    if (std.mem.eql(u8, encoding, "snappy")) {
        const uncompressed = try allocator.alloc(u8, try snappy.uncompressedLength(body[0..body.len]));
        // TODO: consider if arena requires errdefer allocator.free(uncompressed);
        _ = try snappy.uncompress(body[0..body.len], uncompressed);
        return uncompressed;
    }

    // TODO: support gzip to cover loki fully

    // TODO: suport the other compressions types here like zstd, datadog, etc.
    return "";
}

fn process(allocator: std.mem.Allocator, params: Params, data: []const u8, processor: *Processor) !void {
    try parseJson(allocator, data, params, processor);
    try processor.flush();
}

/// docs for more info: https://grafana.com/docs/loki/latest/reference/loki-http-api/#ingest-logs
fn parseJson(allocator: std.mem.Allocator, data: []const u8, params: Params, processor: *Processor) !void {
    // TODO: consider implementing a zero allocation json parsing
    const parsed = try std.json.parseFromSlice(std.json.Value, allocator, data, .{});
    defer parsed.deinit();

    const root = parsed.value;

    // Get "streams" array
    const streams = root.object.get("streams") orelse return error.MissingStreams;
    if (streams != .array) return error.StreamsNotArray;

    const currentTimestamp = std.time.nanoTimestamp();

    // Iterate through each stream
    for (streams.array.items) |stream| {
        if (stream != .object) return error.StreamNotObject;

        // Parse "stream" object (labels) - preallocate for typical label count
        var labels = try std.ArrayList(Field).initCapacity(allocator, 16);
        defer labels.deinit(allocator);

        if (stream.object.get("stream")) |streamObj| {
            if (streamObj != .object) return error.StreamFieldNotObject;

            var it = streamObj.object.iterator();
            while (it.next()) |entry| {
                const valueStr = switch (entry.value_ptr.*) {
                    .string => |s| s,
                    else => return error.LabelValueNotString,
                };
                try labels.append(allocator, .{ .name = entry.key_ptr.*, .value = valueStr });
            }
        }

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
            var timestamp = currentTimestamp;
            if (timestampStr.len != 0) {
                const ts = try std.fmt.parseInt(i64, timestampStr, 10);
                if (ts != 0) {
                    timestamp = ts;
                }
            }

            // Parse structured metadata (if present)
            if (lineArray.len > 2) {
                if (lineArray[2] != .object) return error.StructuredMetadataNotObject;

                var metadata_it = lineArray[2].object.iterator();
                while (metadata_it.next()) |entry| {
                    const value_str = switch (entry.value_ptr.*) {
                        .string => |s| s,
                        else => return error.MetadataValueNotString,
                    };
                    try labels.append(allocator, .{ .name = entry.key_ptr.*, .value = value_str });
                }
            }

            // TODO: rename fields if allowed

            // Parse log message
            const msg = switch (lineArray[1]) {
                .string => |s| s,
                else => return error.MessageNotString,
            };
            // TODO: support a flag to parse msg as json
            try labels.append(allocator, .{ .name = "_msg", .value = msg });

            try processor.pushLine(allocator, timestamp, labels.items, params);
        }
    }
}
