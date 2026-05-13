const std = @import("std");
const Allocator = std.mem.Allocator;
const Io = std.Io;

const m = @import("metrics");

const DispatchMeter = @This();
diskUsage: DiskUsage,

const DiskUsage = m.GaugeVec(u64, struct {});

pub fn init(alloc: Allocator, io: Io) !DispatchMeter {
    return .{
        .diskUsage = try DiskUsage.init(alloc, io, "store_disk_usage_bytes", .{
            .help = "Total bytes used by the store",
        }, .{}),
    };
}

pub fn deinit(self: *DispatchMeter) void {
    self.diskUsage.deinit();
}
