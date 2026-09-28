const std = @import("std");

const IndexBlockHeader = @import("IndexBlockHeader.zig");
const encodeExpectedSize = IndexBlockHeader.encodeExpectedSize;

// Maximum size for an index block (8MB)
pub const maxIndexBlockSize: u64 = 8 * 1024 * 1024;

const testing = std.testing;

test "IndexBlockHeaderEncode" {
    const Case = struct {
        header: IndexBlockHeader,
        expectedLen: usize,
    };

    const cases = &[_]Case{
        .{
            .header = .{
                .sid = .{
                    .tenantID = 42,
                    .id = 42,
                },
                .minTs = 100,
                .maxTs = 200,
                .offset = 1,
                .size = 1234,
            },
            .expectedLen = encodeExpectedSize,
        },
        .{
            .header = std.mem.zeroInit(IndexBlockHeader, .{}),
            .expectedLen = encodeExpectedSize,
        },
        .{
            .header = .{
                .sid = .{
                    .tenantID = 42,
                    .id = std.math.maxInt(u128),
                },
                .minTs = std.math.maxInt(u64),
                .maxTs = std.math.maxInt(u64),
                .offset = std.math.maxInt(u64),
                .size = std.math.maxInt(u64),
            },
            .expectedLen = encodeExpectedSize,
        },
    };

    for (cases) |case| {
        var encodeBuf: [encodeExpectedSize]u8 = undefined;
        const offset = case.header.encode(&encodeBuf);
        try testing.expectEqual(case.expectedLen, offset);

        const h = IndexBlockHeader.decode(encodeBuf[0..offset]);
        try testing.expectEqualDeep(case.header, h);
    }
}
