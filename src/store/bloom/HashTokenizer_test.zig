const std = @import("std");

const bloom = @import("bloom.zig");
const bucketsSize = bloom.bucketsSize;
const Bucket = bloom.Bucket;

pub const HashTokenizer = @import("HashTokenizer.zig");

const testing = std.testing;

test "tokenizeValues" {
    const Case = struct {
        input: []const []const u8,
        expected: []const u64,
    };
    const cases = [_]Case{
        .{ .input = &[_][]const u8{}, .expected = &[_]u64{} },
        .{ .input = &[_][]const u8{""}, .expected = &[_]u64{} },
        .{ .input = &[_][]const u8{"foo"}, .expected = &[_]u64{0x33BF00A859C4BA3F} },
        .{ .input = &[_][]const u8{ "foo -- foo", "~~'(foo) ==^%" }, .expected = &[_]u64{0x33BF00A859C4BA3F} },
        .{
            .input = &[_][]const u8{"foo bar -- .##([baz]## %^&* Groovy"},
            .expected = &[_]u64{ 0x33BF00A859C4BA3F, 0x48A37C90AD27A659, 0x42598CF26A247404, 15498472218330607137 },
        },
        .{
            .input = &[_][]const u8{"foo bar -- .##([baz]## %^&* Groovy [[foo]] <<bar>> --- baz!!"},
            .expected = &[_]u64{ 0x33BF00A859C4BA3F, 0x48A37C90AD27A659, 0x42598CF26A247404, 15498472218330607137 },
        },
        // TODO: enable after unicode implementation
        // .{
        //     .input = &[_][]const u8{ "Юникод 999 var12.34", "34 var12 qwer" },
        //     .expected = &[_]u64{ 0xFE846FA145CEABD1, 0xD8316E61D84F6BA4, 0x6D67BA71C4E03D10, 0x5E8D522CA93563ED, 0xED80AED10E029FC8 },
        // },
    };

    for (cases) |c| {
        const allocator = testing.allocator;
        var buckets: [bucketsSize]Bucket = undefined;
        var tokenizer = try HashTokenizer.init(allocator, &buckets);
        defer tokenizer.deinit(allocator);

        var tokens = try tokenizer.tokenizeValues(allocator, c.input);
        defer tokens.deinit(allocator);

        try testing.expectEqualSlices(u64, c.expected, tokens.items);
    }
}
