const std = @import("std");

const Conf = @import("../../../Conf.zig");
const Table = @import("../Table.zig");

pub fn getFlushToDiskDeadline(memTables: []*Table) i64 {
    const interval = Conf.getConf().app.flushIntervalUs;
    var min: i64 = interval + std.time.microTimestamp();
    for (memTables) |table| {
        if (table.mem) |memTable| {
            min = @min(memTable.flushAtUs, min);
        }
    }

    return min;
}
