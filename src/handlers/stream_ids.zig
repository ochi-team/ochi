const std = @import("std");
const Allocator = std.mem.Allocator;
const Io = std.Io;

const zeit = @import("zeit");
const httpz = @import("httpz");

const parseDurationNs = @import("../stds/time.zig").parseDurationNs;
const parseTimestamp = @import("../stds/time.zig").parseTimestamp;

const AppContext = @import("../dispatch.zig").AppContext;
const ApiError = @import("../server/error.zig").ApiError;

const StreamIDsRequest = struct {
    // since is a duration format, e.g. 2h, 60s, 5m, etc,
    // read more parseDurationNs.
    // if since is presented it's used instead of [from, to] as [since, now()]
    since: ?[]const u8 = null,
    fromNs: ?u64 = null,
    toNs: ?u64 = null,
    from: ?[]const u8 = null,
    to: ?[]const u8 = null,
};

pub fn streamIDsHandler(ctx: *AppContext, r: *httpz.Request, res: *httpz.Response) ApiError!void {
    const contentType = r.headers.get("content-type");
    if (contentType != null and !std.mem.eql(u8, "application/json", contentType.?)) {
        return ApiError.ContentTypeNotSupported;
    }

    const body = r.body() orelse return ApiError.EmptyBody;
    if (body.len > ctx.conf.maxRequestSize) {
        return ApiError.MaxBodySize;
    }

    const request = parseRequest(res.arena, body) catch return ApiError.FailedToParse;
    const timeRange = resolveTimeRange(ctx.io, request) catch return ApiError.InvalidBody;

    var streamIDs = ctx.store.queryStreamIDs(ctx.io, res.arena, ctx.tenantID, timeRange.from, timeRange.to) catch {
        return ApiError.FailedToProccess;
    };
    defer streamIDs.deinit(res.arena);

    // TODO: test values beyond u64, stream ids are u128
    const payload = .{ .streamIDs = streamIDs.keys() };
    const buf = std.json.Stringify.valueAlloc(res.arena, payload, .{}) catch return ApiError.FailedToWriteResponse;

    res.body = buf;
    res.content_type = .JSON;
    res.status = 200;
}

fn parseRequest(alloc: Allocator, data: []const u8) !StreamIDsRequest {
    return std.json.parseFromSliceLeaky(StreamIDsRequest, alloc, data, .{ .allocate = .alloc_if_needed });
}

const TimeRange = struct {
    from: u64,
    to: u64,
};

fn resolveTimeRange(io: Io, request: StreamIDsRequest) !TimeRange {
    if (request.since) |since| {
        const nowNs: u64 = @intCast(Io.Timestamp.now(io, .real).nanoseconds);
        const duration = try parseDurationNs(since);

        return .{
            .from = if (duration >= nowNs) 0 else nowNs - duration,
            .to = nowNs,
        };
    }

    const timerange: TimeRange = timerange: {
        if (request.fromNs != null and request.toNs != null) {
            const fromNs = request.fromNs.?;
            const toNs = request.toNs.?;

            break :timerange .{
                .from = fromNs,
                .to = toNs,
            };
        }

        if (request.from != null and request.to != null) {
            const from = request.from.?;
            const to = request.to.?;

            const fromNs = try parseTimestamp(from);
            const toNs = try parseTimestamp(to);

            break :timerange .{
                .from = fromNs,
                .to = toNs,
            };
        }

        return error.MissingFromToTimestamp;
    };

    // TODO: all the APIs need a proper errors documentation and handling
    if (timerange.from > timerange.to) {
        return error.InvalidTimeRange;
    }

    return timerange;
}
