const std = @import("std");

const Contract = @import("../../stds/Contract.zig");

pub const FlushableTableContract = Contract{
    .fields = &.{
        .{ .name = "flushAtUs", .type = i64 },
    },
    .funcs = &.{},
};

// TODO: this package requires a solution, it doesn't seem either useful or reliable
pub fn TableContract(comptime T: type, comptime M: type) Contract {
    return Contract{
        .fields = &.{
            .{ .name = "size", .type = u64 },
            .{ .name = "inMerge", .type = bool },
            .{ .name = "mem", .type = ?M, .contract = FlushableTableContract },
        },
        .funcs = &.{
            .{ .name = "lessThan", .type = fn (void, T, T) bool },
        },
    };
}

pub fn TableRefCountContract(comptime T: type) Contract {
    return Contract{
        .fields = &.{
            .{ .name = "toRemove", .type = std.atomic.Value(bool) },
        },
        .funcs = &.{
            .{ .name = "release", .type = fn (*T) void },
            .{ .name = "writeNames", .type = fn (std.mem.Allocator, []const u8, []*T) anyerror!void },
        },
    };
}
