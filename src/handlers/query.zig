const std = @import("std");
const Allocator = std.mem.Allocator;

const httpz = @import("httpz");

const Field = @import("../store/lines.zig").Field;
const Line = @import("../store/lines.zig").Line;
const Query = @import("../store/query.zig").Query;

const AppContext = @import("../dispatch.zig").AppContext;
const ApiError = @import("../server/error.zig").ApiError;

/// queryHandler does a query fetch according to a passed Query in the body
pub fn queryHandler(ctx: *AppContext, r: *httpz.Request, res: *httpz.Response) ApiError!void {
    const contentType = r.headers.get("content-type");

    if (contentType != null and !std.mem.eql(u8, "application/json", contentType.?)) {
        return ApiError.ContentTypeNotSupported;
    }

    const body = r.body() orelse return ApiError.EmptyBody;

    if (body.len > ctx.conf.maxRequestSize) {
        return ApiError.MaxBodySize;
    }

    const value = parseQuery(res.arena, body) catch return ApiError.FailedToParse;

    var lines = ctx.store.queryLines(res.arena, ctx.tenantID, value) catch {
        return ApiError.FailedToProccess;
    };
    defer lines.deinit(res.arena);

    writeResponse(res, lines.items) catch return ApiError.FailedToWriteResponse;

    res.status = 200;
}

/// JSON response shape for a single log line.
const LineResponse = struct {
    ts: u64,
    fields: []const Field,
};

fn writeResponse(res: *httpz.Response, lines: []const Line) !void {
    var responseLines = try std.ArrayList(LineResponse).initCapacity(res.arena, lines.len);
    for (lines) |line| {
        responseLines.appendAssumeCapacity(.{
            .ts = line.timestampNs,
            .fields = line.fields,
        });
    }

    const buf = try std.json.Stringify.valueAlloc(res.arena, lines, .{});

    res.body = buf;
    res.content_type = .JSON;
}

/// parseQuery unmarshals a JSON body into a Query.
///
/// Expected format:
/// ```json
/// {
///   "start": "1234567890000000000",
///   "end":   "1234567890000000000",
///   "tags":   {"app": "myapp"},
///   "fields": {"level": "error"}
/// }
/// ```
/// `start` and `end` are nanosecond Unix timestamps encoded as strings.
/// `tags` and `fields` are optional label-filter maps.
fn parseQuery(alloc: Allocator, data: []const u8) !Query {
    return std.json.parseFromSliceLeaky(Query, alloc, data, .{ .allocate = .alloc_if_needed });
}
