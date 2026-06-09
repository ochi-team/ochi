const std = @import("std");
const Allocator = std.mem.Allocator;
const Io = std.Io;

const httpz = @import("httpz");

const Field = @import("../store/lines.zig").Field;
const Line = @import("../store/lines.zig").Line;
const Query = @import("../query/Query.zig");

const Loql = @import("../query/Loql.zig");
const ErrorReporter = @import("../query/ErrorReporter.zig");

const AppContext = @import("../dispatch.zig").AppContext;
const ApiError = @import("../server/error.zig").ApiError;

/// queryHandler does a query fetch according to a passed Query in the body
pub fn queryHandler(ctx: *AppContext, r: *httpz.Request, res: *httpz.Response) ApiError!void {
    const contentType = r.headers.get("content-type");

    const body = r.body() orelse return ApiError.EmptyBody;

    if (body.len > ctx.conf.maxRequestSize) {
        return ApiError.MaxBodySize;
    }

    const query = q: {
        if (contentType == null or std.mem.eql(u8, "application/loql", contentType.?)) {
            var loql: Loql = .{};
            defer loql.deinit(res.arena);

            const now = Io.Timestamp.now(ctx.io, .real);
            var errs: ErrorReporter = .{};

            const translatedQuery = loql.translateQuery(res.arena, &errs, body, @intCast(now.nanoseconds)) catch return ApiError.FailedToParse;
            break :q translatedQuery;
        } else if (std.mem.eql(u8, "application/json", contentType.?)) {
            const value = parseQuery(res.arena, body) catch return ApiError.FailedToParse;
            break :q value;
        } else {
            return ApiError.ContentTypeNotSupported;
        }
    };

    var lines = ctx.store.queryLines(ctx.io, res.arena, ctx.allocator, ctx.tenantID, query) catch {
        return ApiError.FailedToProccess;
    };
    defer lines.deinit(res.arena);

    writeResponse(res, lines.items) catch return ApiError.FailedToWriteResponse;

    res.status = 200;
}

// TODO: remove it when Line has no sid
/// JSON response shape for a single log line.
const LineResponse = struct {
    ts: u64,
    fields: []const Field,
};

// TODO: this is broken, it writes SID with array list fields
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
/// TODO: implement our zero allloc json parser to eliminate double parsing,
/// default json parser panics on attempt to parse float64 into u128
fn parseQuery(alloc: Allocator, data: []const u8) !Query {
    const value = try std.json.parseFromSliceLeaky(std.json.Value, alloc, data, .{
        .allocate = .alloc_if_needed,
        .parse_numbers = false,
    });
    try validateStreamIDs(value);

    return std.json.parseFromValueLeaky(Query, alloc, value, .{});
}

const testing = std.testing;

fn validateStreamIDs(value: std.json.Value) !void {
    const object = switch (value) {
        .object => |object| object,
        else => return error.UnexpectedToken,
    };

    const streamIDs = object.get("streamIDs") orelse return;
    const items = switch (streamIDs) {
        .null => return,
        .array => |array| array.items,
        else => return error.UnexpectedToken,
    };

    for (items) |item| {
        const sid = switch (item) {
            .number_string, .string => |sid| sid,
            else => return error.UnexpectedToken,
        };
        if (!std.json.isNumberFormattedLikeAnInteger(sid)) {
            std.debug.print("invalid streamID format: {s}\n", .{sid});
            return error.InvalidNumber;
        }
        _ = try std.fmt.parseInt(u128, sid, 10);
    }
}

test "parseQuery rejects exponent formatted streamIDs outside i128 range" {
    const Case = struct {
        content: []const u8,
        expected: ?Query = null,
        expectedErr: ?anyerror = null,
    };

    const validStreamIDs = [_]u128{170141183460469231731687303715884105727};
    const cases = [_]Case{
        .{
            .content =
            \\{
            \\  "streamIDs": [170141183460469231731687303715884105727],
            \\  "start": 0,
            \\  "end": 1
            \\}
            ,
            .expected = .{
                .streamIDs = &validStreamIDs,
                .start = 0,
                .end = 1,
            },
        },
        .{
            .content =
            \\{
            \\  "streamIDs": [2e38],
            \\  "start": 0,
            \\  "end": 1
            \\}
            ,
            .expectedErr = error.InvalidNumber,
        },
        .{
            .content =
            \\{
            \\  "streamIDs": ["2e38"],
            \\  "start": 0,
            \\  "end": 1
            \\}
            ,
            .expectedErr = error.InvalidNumber,
        },
    };

    for (cases) |case| {
        var arena: std.heap.ArenaAllocator = .init(testing.allocator);
        defer arena.deinit();

        if (case.expectedErr) |expectedErr| {
            try testing.expectError(expectedErr, parseQuery(arena.allocator(), case.content));
        } else {
            const query = try parseQuery(arena.allocator(), case.content);
            try testing.expectEqualDeep(case.expected.?, query);
        }
    }
}
