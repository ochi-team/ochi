const std = @import("std");

const m = @import("metrics");

const DispatchMeter = @This();
diskUsage: DiskUsage,

const DiskUsage = m.Gauge(u64);

pub fn init() DispatchMeter {
    return .{
        .diskUsage = DiskUsage.init("store_disk_usage_bytes", .{
            .help = "Total bytes used by the store",
        }, .{}),
    };
}

pub fn write(self: *DispatchMeter, writer: *std.Io.Writer) !void {
    try m.write(self, writer);
}
