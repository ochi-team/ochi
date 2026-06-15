const std = @import("std");
const Io = std.Io;

const Consts = @import("../../Consts.zig");

pub fn getFlushTablesToDiskDeadline(
    io: Io,
    comptime T: type,
    memTables: []T,
) i64 {
    var min: i64 = Consts.indexFlushIntervalUs + Io.Timestamp.now(io, .real).toMicroseconds();
    for (memTables) |table| {
        if (table.inner == .mem) {
            min = @min(table.inner.mem.flushAtUs, min);
        }
    }

    return min;
}
