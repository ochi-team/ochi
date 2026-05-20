const std = @import("std");
const Allocator = std.mem.Allocator;
const Io = std.Io;

const m = @import("metrics");

const DispatchMeter = @This();
throughput: Throughput,
requests: Requests,
latencyMs: Latency,

pub const Labels = struct { status: u16, path: []const u8 };
const Throughput = m.CounterVec(u64, Labels);
const Requests = m.CounterVec(u64, Labels);
const Latency = m.HistogramVec(u64, Labels, &.{ 20, 50, 100, 200, 500, 1000, 5000, 10000, 30000 });

pub fn init(allocator: Allocator, io: Io) !DispatchMeter {
    var throughput = try Throughput.init(allocator, io, "dispatch_request_body_bytes_total", .{
        .help = "Total request body bytes received grouped by HTTP status",
    }, .{});
    errdefer throughput.deinit();

    var requests = try Requests.init(allocator, io, "dispatch_requests_total", .{
        .help = "Total requests grouped by HTTP status",
    }, .{});
    errdefer requests.deinit();

    const latency = try Latency.init(allocator, io, "dispatch_request_latency_milliseconds", .{
        .help = "Request latency in milliseconds grouped by HTTP status and path",
    }, .{});
    errdefer latency.deinit();

    return .{
        .throughput = throughput,
        .requests = requests,
        .latencyMs = latency,
    };
}

pub fn deinit(self: *DispatchMeter) void {
    self.throughput.deinit();
    self.requests.deinit();
    self.latencyMs.deinit();
}

pub fn write(self: *DispatchMeter, writer: *std.Io.Writer) !void {
    try self.throughput.write(writer);
    try self.requests.write(writer);
    try self.latencyMs.write(writer);
}
