const std = @import("std");
const Allocator = std.mem.Allocator;
const Io = std.Io;

const m = @import("metrics");

const DispatchMeter = @This();
throughput: Throughput,
requests: Requests,
latency: Latency,

pub const Labels = struct { status: u16, path: []const u8 };
const Throughput = m.CounterVec(u64, Labels);
const Requests = m.CounterVec(u64, Labels);
const Latency = m.HistogramVec(u64, Labels, &.{ 1, 5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10000 });

pub fn init(allocator: Allocator, io: Io) !DispatchMeter {
    return .{
        .throughput = try Throughput.init(allocator, io, "dispatch_request_body_bytes_total", .{
            .help = "Total request body bytes received grouped by HTTP status",
        }, .{}),
        .requests = try Requests.init(allocator, io, "dispatch_requests_total", .{
            .help = "Total requests grouped by HTTP status",
        }, .{}),
        .latency = try Latency.init(allocator, io, "dispatch_request_latency_milliseconds", .{
            .help = "Request latency in milliseconds grouped by HTTP status and path",
        }, .{}),
    };
}

pub fn deinit(self: *DispatchMeter) void {
    self.throughput.deinit();
    self.requests.deinit();
    self.latency.deinit();
}

pub fn write(self: *DispatchMeter, writer: *std.Io.Writer) !void {
    try m.write(self, writer);
}
