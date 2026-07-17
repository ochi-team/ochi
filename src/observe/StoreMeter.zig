const std = @import("std");
const Allocator = std.mem.Allocator;
const Io = std.Io;

const m = @import("metrics");

const StoreMeter = @This();
diskUsage: DiskUsage,
openTables: OpenTables,

const DiskUsage = m.Gauge(u64);
pub const OpenTablesLabels = struct { kind: []const u8, partition: []const u8, residence: []const u8 };
const OpenTables = m.GaugeVec(u32, OpenTablesLabels);

pub fn init(io: Io, alloc: Allocator) !StoreMeter {
    return .{
        .diskUsage = DiskUsage.init("store_disk_usage_bytes", .{
            .help = "Total bytes used by the store",
        }, .{}),
        .openTables = try OpenTables.init(alloc, io, "store_open_tables", .{
            .help = "Amount of currently open tables in all the partitions",
        }, .{}),
    };
}

pub fn deinit(self: *StoreMeter) void {
    self.openTables.deinit();
}

pub fn write(self: *StoreMeter, writer: *std.Io.Writer) !void {
    try self.diskUsage.write(writer);
    try self.openTables.write(writer);
}
