const std = @import("std");
const Io = std.Io;

const Conf = @import("../../Conf.zig");

pub fn getFlushTablesToDiskDeadline(
    io: Io,
    comptime T: type,
    memTables: []T,
) i64 {
    const interval = Conf.getConf().app.flushIntervalUs;
    var min: i64 = interval + Io.Timestamp.now(io, .real).toMicroseconds();
    for (memTables) |table| {
        if (table.inner == .mem) {
            min = @min(table.inner.mem.flushAtUs, min);
        }
    }

    return min;
}
