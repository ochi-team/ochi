const std = @import("std");
const zeit = @import("zeit");
const Io = std.Io;
const Allocator = std.mem.Allocator;

const cli = @import("cli");
const OchiClient = @import("OchiClient.zig");

const Options = struct {
    url: []const u8,
    run_dir: []const u8,
};

const Record = struct {
    _time: []const u8,
    _msg: []const u8,
    app: []const u8,
    id: []const u8,
};
const Response = struct { lines: []const Record };

pub fn main(init: std.process.Init) !void {
    const alloc = init.arena.allocator();

    const args = try init.minimal.args.toSlice(alloc);
    const options = cli.parse(args[1..], Options) catch |err| {
        std.debug.print("Usage: sack --url http://127.0.0.1:9014 --run_dir NEW_DIRECTORY\n", .{});
        return err;
    };
    run(init.io, init.gpa, options, @intCast(Io.Timestamp.now(init.io, .real).nanoseconds), .fromSeconds(6)) catch |err| {
        std.debug.print("sack failed: {s}; artifacts: {s}\n", .{ @errorName(err), options.run_dir });
        return err;
    };
    std.debug.print("sack: exact single-record response verified; artifacts: {s}\n", .{options.run_dir});
}

fn run(io: Io, alloc: Allocator, options: Options, timestamp_ns: u64, settle_delay: Io.Duration) !void {
    // never reuse an existing directory data directory, create new
    try Io.Dir.cwd().createDir(io, options.run_dir, .default_dir);
    var dir = try Io.Dir.cwd().openDir(io, options.run_dir, .{});
    defer dir.close(io);

    const ingest = try std.fmt.allocPrint(
        alloc,
        "{{\"streams\":[{{\"stream\":{{\"app\":\"sack\"}},\"values\":[[\"{d}\",\"sack smoke\",{{\"id\":\"sack-0\"}}]]}}]}}",
        .{timestamp_ns},
    );
    defer alloc.free(ingest);

    const query = try std.fmt.allocPrint(alloc, "{{\"start\":0,\"end\":{d}}}", .{try std.math.add(u64, timestamp_ns, 1)});
    defer alloc.free(query);
    var time_buf: [64]u8 = undefined;
    const instant: zeit.Instant = .{ .timestamp = timestamp_ns, .timezone = &zeit.utc };
    const expected: Record = .{
        ._time = try instant.time().bufPrint(&time_buf, .rfc3339),
        ._msg = "sack smoke",
        .app = "sack",
        .id = "sack-0",
    };
    const expected_json = try std.json.Stringify.valueAlloc(alloc, .{ .lines = &.{expected} }, .{});
    defer alloc.free(expected_json);
    try save(dir, io, "ingest.request.json", ingest);
    try save(dir, io, "query.request.json", query);
    try save(dir, io, "expected.json", expected_json);

    var client: OchiClient = .{ .host = options.url, .client = .{ .allocator = alloc, .io = io } };
    defer client.deinit();

    var ingest_response = try post(&client, alloc, dir, "/ingest/loki/api/v1/push", "ingest", ingest);
    defer ingest_response.deinit();
    if (ingest_response.status != .ok) return error.IngestRejected;

    // allow the flush delay
    try Io.sleep(io, settle_delay, .awake);

    var query_response = try post(&client, alloc, dir, "/query", "query", query);
    defer query_response.deinit();
    if (query_response.status != .ok) return error.QueryRejected;

    const parsed = try std.json.parseFromSlice(Response, alloc, query_response.body.written(), .{});
    defer parsed.deinit();

    if (parsed.value.lines.len != 1) return error.UnexpectedQueryResult;
    const actual = parsed.value.lines[0];
    inline for (std.meta.fields(Record)) |field| {
        if (!std.mem.eql(u8, @field(expected, field.name), @field(actual, field.name)))
            return error.UnexpectedQueryResult;
    }
}

fn post(client: *OchiClient, alloc: Allocator, dir: Io.Dir, path: []const u8, name: []const u8, payload: []const u8) !OchiClient.HttpResponse {
    var result = client.request(alloc, .{ .method = .POST, .path = path, .body = payload }) catch |err| {
        std.log.debug("failed to post request, name={s}", .{name});
        return err;
    };
    errdefer result.deinit();

    var response_name_buf: [std.fs.max_path_bytes]u8 = undefined;
    const response_name = try std.fmt.bufPrint(&response_name_buf, "{s}.response.json", .{name});

    try save(dir, client.client.io, response_name, result.body.written());

    var status_name_buf: [std.fs.max_path_bytes]u8 = undefined;
    const status_name = try std.fmt.bufPrint(&status_name_buf, "{s}.status.txt", .{name});
    var buf: [16]u8 = undefined;
    try save(dir, client.client.io, status_name, try std.fmt.bufPrint(&buf, "{d}\n", .{@intFromEnum(result.status)}));
    return result;
}

fn save(dir: Io.Dir, io: Io, name: []const u8, bytes: []const u8) !void {
    try dir.writeFile(io, .{ .sub_path = name, .data = bytes });
}
