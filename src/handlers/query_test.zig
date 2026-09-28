const std = @import("std");

const Query = @import("../query/Query.zig");
const parseQuery = @import("query.zig").parseQuery;

const testing = std.testing;

test "parseQuery rejects exponent formatted streamIDs outside i128 range" {
    const Case = struct {
        content: []const u8,
        expected: ?Query = null,
        expectedErr: ?anyerror = null,
    };

    const validStreamIDs = [_]u128{170141183460469231731687303715884105727};
    const cases = [_]Case{
        .{
            .content =
            \\{
            \\  "streamIDs": [170141183460469231731687303715884105727],
            \\  "start": 0,
            \\  "end": 1
            \\}
            ,
            .expected = .{
                .streamIDs = &validStreamIDs,
                .start = 0,
                .end = 1,
            },
        },
        .{
            .content =
            \\{
            \\  "streamIDs": [2e38],
            \\  "start": 0,
            \\  "end": 1
            \\}
            ,
            .expectedErr = error.InvalidNumber,
        },
        .{
            .content =
            \\{
            \\  "streamIDs": ["2e38"],
            \\  "start": 0,
            \\  "end": 1
            \\}
            ,
            .expectedErr = error.InvalidNumber,
        },
    };

    for (cases) |case| {
        var arena: std.heap.ArenaAllocator = .init(testing.allocator);
        defer arena.deinit();

        if (case.expectedErr) |expectedErr| {
            try testing.expectError(expectedErr, parseQuery(arena.allocator(), case.content));
        } else {
            const query = try parseQuery(arena.allocator(), case.content);
            try testing.expectEqualDeep(case.expected.?, query);
        }
    }
}
