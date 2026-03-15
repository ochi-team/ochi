const Contract = @import("../../stds/Contract.zig");

pub const FlushableTableContract = Contract{
    .fields = &.{
        .{ .name = "flushAtUs", .type = i64 },
    },
    .funcs = &.{},
};

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
