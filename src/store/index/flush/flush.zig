const std = @import("std");
const Allocator = std.mem.Allocator;

const Conf = @import("../../../Conf.zig");
const MemTable = @import("../MemTable.zig");

pub fn getFlushToDiskDeadline(memTables: []*MemTable) i64 {
    const interval = Conf.getConf().app.flushIntervalUs;
    var min: i64 = interval + std.time.microTimestamp();
    for (memTables) |table| {
        if (table.flushAtUs) |flushAtUs| {
            min = @min(flushAtUs, min);
        }
    }

    return min;
}
