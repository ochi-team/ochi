const std = @import("std");

const TimestampsEncoder = @import("TimestampsEncoder.zig");
const EncodingType = TimestampsEncoder.EncodingType;

const testing = std.testing;

test "TimestampsEncoder" {
    const alloc = testing.allocator;
    const Case = struct {
        input: []const u64,
    };
    const cases = &[_]Case{
        .{ .input = &[_]u64{ 1, 2, 3, 4 } },
        .{ .input = &[_]u64{} },
        .{ .input = &[_]u64{std.math.maxInt(u64)} },
        .{ .input = &[_]u64{ std.math.maxInt(u64), 0 } },
        .{ .input = &[_]u64{ 0, std.math.maxInt(u64) } },
    };

    for (cases) |case| {
        const enc = try TimestampsEncoder.init(alloc);
        defer enc.deinit(alloc);

        var buf: [64]u8 = undefined;
        const res = try enc.encode(&buf, case.input);
        try testing.expectEqual(EncodingType.ZDeltapack, res.encodingType);

        var decoded: [8]u64 = undefined;
        try enc.decode(decoded[0..case.input.len], buf[0..res.offset]);
        try std.testing.expectEqualSlices(u64, case.input, decoded[0..case.input.len]);
    }
}
