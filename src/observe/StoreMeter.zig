const std = @import("std");

const m = @import("metrics");

const StoreMeter = @This();
diskUsage: DiskUsage,

const DiskUsage = m.Gauge(u64);

pub fn init() StoreMeter {
    return .{
        .diskUsage = DiskUsage.init("store_disk_usage_bytes", .{
            .help = "Total bytes used by the store",
        }, .{}),
    };
}

pub fn write(self: *StoreMeter, writer: *std.Io.Writer) !void {
    try self.diskUsage.write(writer);
}
