const std = @import("std");

const lines = @import("lines.zig");
const Field = lines.Field;
const Line = lines.Line;
pub const timestampKey = lines.timestampKey;
pub const msgKey = lines.msgKey;

const testing = std.testing;

test "Field.encodeIndexTag" {
    const alloc = testing.allocator;
    const Case = struct {
        key: []const u8,
        value: []const u8,
        expected: []const u8,
    };

    const cases = [_]Case{
        .{
            .key = "key",
            .value = "value",
            .expected = "key\x01value\x01",
        },
        .{
            .key = "ke\x00y",
            .value = "value",
            .expected = "ke\x000y\x01value\x01",
        },
        .{
            .key = "key",
            .value = "val\x01ue",
            .expected = "key\x01val\x001ue\x01",
        },
        .{
            .key = "k\x00e\x01y",
            .value = "v\x01al\x00ue",
            .expected = "k\x000e\x001y\x01v\x001al\x000ue\x01",
        },
    };
    for (cases) |case| {
        const f = Field{ .key = case.key, .value = case.value };
        const bound = f.encodeIndexTagBound();
        const buf = try alloc.alloc(u8, bound);
        defer alloc.free(buf);

        const encodedLen = f.encodeIndexTag(buf);
        try testing.expectEqualSlices(u8, case.expected, buf[0..encodedLen]);

        // Test round-trip: decode and verify we get back the original key/value
        const decodeBuf = try alloc.alloc(u8, bound);
        defer alloc.free(decodeBuf);
        @memcpy(decodeBuf[0..encodedLen], buf[0..encodedLen]);

        var decoded = Field{ .key = "", .value = "" };
        const decodeOffset = decoded.decodeIndexTag(decodeBuf[0..encodedLen]);

        try testing.expectEqualSlices(u8, case.key, decoded.key);
        try testing.expectEqualSlices(u8, case.value, decoded.value);
        try testing.expectEqual(decodeBuf.len, decodeOffset);

        try testing.expect(f.eql(.{ .key = decoded.key, .value = decoded.value }));
    }
}

test "Line.stringifyJSON emits query response line object" {
    const Case = struct {
        timestampNs: u64,
        fields: []Field,
        expected: []const u8,
    };

    var normalFields = [_]Field{
        .{ .key = "x", .value = "y" },
        .{ .key = "", .value = "hello" },
        .{ .key = "skip", .value = "" },
    };
    var escapedFields = [_]Field{
        .{ .key = "quote\"key", .value = "line\nvalue" },
        .{ .key = "slash\\key", .value = "tab\tvalue" },
    };

    const cases = [_]Case{
        .{
            .timestampNs = 0,
            .fields = normalFields[0..],
            .expected = "{\"" ++ timestampKey ++ "\":\"1970-01-01T00:00:00.000Z\",\"x\":\"y\",\"" ++ msgKey ++ "\":\"hello\"}",
        },
        .{
            .timestampNs = 1_234_567_890,
            .fields = escapedFields[0..],
            .expected = "{\"" ++ timestampKey ++ "\":\"1970-01-01T00:00:01.234Z\",\"quote\\\"key\":\"line\\nvalue\",\"slash\\\\key\":\"tab\\tvalue\"}",
        },
    };

    for (cases) |case| {
        var writer = try std.Io.Writer.Allocating.initCapacity(testing.allocator, 128);
        defer writer.deinit();

        var jw: std.json.Stringify = .{ .writer = &writer.writer };
        const line = Line{ .timestampNs = case.timestampNs, .fields = case.fields };
        try line.stringifyJSON(&jw);

        try testing.expectEqualStrings(case.expected, writer.written());
    }
}
