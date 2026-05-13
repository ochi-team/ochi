const std = @import("std");
const Allocator = std.mem.Allocator;
const Io = std.Io;

const m = @import("metrics");

const DispatchMeter = @This();
throughput: Throughput,
requests: Requests,

const Throughput = m.CounterVec(u64, struct { status: u16, path: []const u8 });
const Requests = m.CounterVec(u64, struct { status: u16, path: []const u8 });

pub fn init(allocator: Allocator, io: Io) !DispatchMeter {
    return .{
        .throughput = try Throughput.init(allocator, io, "dispatch_request_body_bytes_total", .{
            .help = "Total request body bytes received grouped by HTTP status",
        }, .{}),
        .requests = try Requests.init(allocator, io, "dispatch_requests_total", .{
            .help = "Total requests grouped by HTTP status",
        }, .{}),
    };
}

pub fn deinit(self: *DispatchMeter) void {
    self.throughput.deinit();
    self.requests.deinit();
}


