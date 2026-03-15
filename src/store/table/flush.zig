const std = @import("std");

const Conf = @import("../../Conf.zig");

const TableContract = @import("contract.zig").TableContract;
const FlushableTableContract = @import("contract.zig").FlushableTableContract;

pub fn getFlushTablesToDiskDeadline(
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
    var min: i64 = interval + std.time.microTimestamp();
    for (memTables) |table| {
        if (table.mem) |memTable| {
            min = @min(memTable.flushAtUs, min);
        }
    }

    return min;
}

pub fn getFlushMemTableToDiskDeadline(
    comptime M: type,
    memTables: []M,
) i64 {
    comptime {
        const contract = FlushableTableContract;
        contract.satisfies(M, true) catch |err| {
            @compileError("FlushableTableContract is not satisfied by " ++ @typeName(M) ++ ": " ++ @errorName(err));
        };
    }

    const interval = Conf.getConf().app.flushIntervalUs;
    var min: i64 = interval + std.time.microTimestamp();
    for (memTables) |table| {
        min = @min(table.flushAtUs, min);
    }

    return min;
}
