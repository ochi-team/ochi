/// insert module provides write path for Seqor
const std = @import("std");

const httpz = @import("httpz");
const snappy = @import("snappy");

const AppContext = @import("dispatch.zig").AppContext;
const Processor = @import("process.zig").Processor;
const Field = @import("process.zig").Field;
const Params = @import("process.zig").Params;
const Tenant = @import("process.zig").Tenant;

/// insertLokiJson defines a loki json insertion operation
pub fn insertLokiJson(ctx: *AppContext, r: *httpz.Request, res: *httpz.Response) !void {
    const contentType = r.headers.get("Content-Type");
    if (contentType != null and !std.mem.eql(u8, "application/json", contentType.?)) {
        // TODO: implement protobuf marhsalling
        res.status = 400;
        res.body = "protobuf content-type is not supported";
        return;
    }
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

    const tenantStr = r.headers.get("X-Scope-OrgID") orelse "default";
    const tenant = try parseTenantID(tenantStr);
    const params = Params{ .tenant = tenant };

    process(res.arena, uncompressed, params, ctx.processor) catch {
        res.body = "failed to process logs";
        res.status = 500;
        return;
    };

    res.status = 200;
}

fn parseTenantID(tenantStr: []const u8) !Tenant {
    if (tenantStr.len > 16) {
        return error.InvalidTenantID;
    }

    return Tenant{ .id = tenantStr };
}

/// insertLokiReady defines a loki handler to signal its readiness
pub fn insertLokiReady(_: *AppContext, _: *httpz.Request, res: *httpz.Response) !void {
    res.status = 200;
    res.body = "ready";
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

fn process(allocator: std.mem.Allocator, data: []const u8, params: Params, processor: *Processor) !void {
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

    // pre allocate labels list
    var labels: std.ArrayList(Field) = undefined;
    defer labels.deinit(allocator);

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
        labels = try std.ArrayList(Field).initCapacity(allocator, labelSize);
    }

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
                try labels.append(allocator, .{ .key = entry.key_ptr.*, .value = valueStr });
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
            const ts = try std.fmt.parseInt(i64, timestampStr, 10);

            // Parse structured metadata (if present)
            if (lineArray.len > 2) {
                if (lineArray[2] != .object) return error.StructuredMetadataNotObject;

                var metadata_it = lineArray[2].object.iterator();
                while (metadata_it.next()) |entry| {
                    const value_str = switch (entry.value_ptr.*) {
                        .string => |s| s,
                        else => return error.MetadataValueNotString,
                    };
                    try labels.append(allocator, .{ .key = entry.key_ptr.*, .value = value_str });
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
            try labels.append(allocator, .{ .key = "_msg", .value = msg });

            try processor.pushLine(allocator, ts, labels.items, params);
        }

        // clean len of the labels len, but retain allocated memory
        labels.clearRetainingCapacity();
    }
}
