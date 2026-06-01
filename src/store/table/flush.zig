const std = @import("std");
const Io = std.Io;

const Conf = @import("../../Conf.zig");

const TableContract = @import("contract.zig").TableContract;

pub fn getFlushTablesToDiskDeadline(
    io: Io,
    comptime T: type,
    comptime M: type,
    memTables: []T,
) i64 {
    comptime {
        const contract = TableContract(T, M);
        contract.satisfies(T, true) catch |err| {
            @compileError("TableContract is not satisfied by " ++ @typeName(T) ++ ": " ++ @errorName(err));
        };
    }

    const interval = Conf.getConf().app.flushIntervalUs;
    var min: i64 = interval + Io.Timestamp.now(io, .real).toMicroseconds();
    for (memTables) |table| {
        if (table.mem) |memTable| {
            min = @min(memTable.flushAtUs, min);
        }
    }

    return min;
}
