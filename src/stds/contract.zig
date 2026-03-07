const std = @import("std");
const Allocator = std.mem.Allocator;

const Sample = struct {
    size: u32,
    has: bool,

    pub fn lessThan(_: void, a: Sample, b: Sample) bool {
        return a.size < b.size;
    }
};

test "contract" {
    const Case = struct {
        contract: Contract,
        err: ?anyerror,
    };

    const cases = [_]Case{
        .{
            .contract = Contract{ .fields = &.{
                .{
                    .name = "size",
                    .type = u32,
                },
                .{
                    .name = "has",
                    .type = bool,
                },
            }, .funcs = &.{
                .{
                    .name = "lessThan",
                    .type = fn (void, Sample, Sample) bool,
                },
            } },
        },
    };

    for (cases) |case| {
        const s = Sample{ .size = 0, .has = false };

        if (case.err) |err| {
            try std.testing.expectError(case.err, case.contract.satisfies(s));
        } else {
            case.contract.satisfies(s);
        }
    }
}
