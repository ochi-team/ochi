const std = @import("std");

const Packer = @import("Packer.zig");

const UnpackError = @import("Unpacker.zig").UnpackError;
const unpackU64s = @import("Unpacker.zig").unpackU64s;

const testing = std.testing;

test "Unpacker.unpackU64s" {
    const Case = struct {
        data: []const u8,
        count: usize,
        expected: []const u64 = &.{},
        expectedErr: ?anyerror = null,
    };

    const cases = [_]Case{
        .{
            // uintBlockTypeInvariant64: single big-endian u64 repeated for every element
            .data = &[_]u8{ Packer.uintBlockTypeInvariant64, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08 },
            .count = 3,
            .expected = &[_]u64{ 0x0102030405060708, 0x0102030405060708, 0x0102030405060708 },
        },
        .{
            // uintBlockTypeInvariant64: wrong payload length
            .data = &[_]u8{ Packer.uintBlockTypeInvariant64, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07 },
            .count = 3,
            .expectedErr = UnpackError.InsufficientDataLen,
        },
        .{
            // uintBlockType64: one big-endian u64 per element
            .data = &[_]u8{
                Packer.uintBlockType64,
                0x00,
                0x00,
                0x00,
                0x00,
                0x00,
                0x00,
                0x00,
                0x01,
                0xFF,
                0xFF,
                0xFF,
                0xFF,
                0xFF,
                0xFF,
                0xFF,
                0xFF,
            },
            .count = 2,
            .expected = &[_]u64{ 1, 0xFFFFFFFFFFFFFFFF },
        },
        .{
            // uintBlockType64: wrong payload length for count
            .data = &[_]u8{
                Packer.uintBlockType64,
                0x00,
                0x00,
                0x00,
                0x00,
                0x00,
                0x00,
                0x00,
                0x01,
            },
            .count = 2,
            .expectedErr = UnpackError.InsufficientDataLen,
        },
    };

    for (cases) |case| {
        if (case.expectedErr) |expectedErr| {
            try testing.expectError(expectedErr, unpackU64s(testing.allocator, case.data, case.count));
            continue;
        }
        const got = try unpackU64s(testing.allocator, case.data, case.count);
        defer testing.allocator.free(got);
        try testing.expectEqualSlices(u64, case.expected, got);
    }
}
