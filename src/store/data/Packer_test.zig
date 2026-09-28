const std = @import("std");
const Unpacker = @import("Unpacker.zig").Unpacker;
const CompressionPool = @import("../compression/CompressionPool.zig");
const DecompressionPool = @import("../compression/DecompressionPool.zig");

const Width = struct {
    max: u64,
    size: usize,
    block: u8,
    blockInvariant: u8,
};
pub const uintBlockType8: u8 = 0;
pub const uintBlockType16: u8 = 1;
pub const uintBlockType32: u8 = 2;
pub const uintBlockType64: u8 = 3;
pub const uintBlockTypeInvariant8: u8 = 4;
pub const uintBlockTypeInvariant16: u8 = 5;
pub const uintBlockTypeInvariant32: u8 = 6;
pub const uintBlockTypeInvariant64: u8 = 7;

const widths = [_]Width{
    .{ .max = (1 << 8), .block = uintBlockType8, .blockInvariant = uintBlockTypeInvariant8, .size = @sizeOf(u8) },
    .{ .max = (1 << 16), .block = uintBlockType16, .blockInvariant = uintBlockTypeInvariant16, .size = @sizeOf(u16) },
    .{ .max = (1 << 32), .block = uintBlockType32, .blockInvariant = uintBlockTypeInvariant32, .size = @sizeOf(u32) },
    .{ .max = ~@as(u64, 0), .block = uintBlockType64, .blockInvariant = uintBlockTypeInvariant64, .size = @sizeOf(u64) },
};
fn pickWidth(maxLen: u64) Width {
    for (widths) |w| {
        if (maxLen < w.max) return w;
    }
    std.debug.panic("unexpected int width, given len={}", .{maxLen});
}

pub const compressionKindPlain: u8 = 0;
pub const compressionKindZstd: u8 = 1;

const Packer = @import("Packer.zig");
const packValues = Packer.packValues;

const testing = std.testing;

// TODO: there must be more properties besides rount-trippness,
// e.g. size of the output is less
test "Packer.packValuesRoundtrip" {
    const alloc = testing.allocator;

    const Case = struct {
        strings: []const []const u8,
    };

    var veryLongString: [2 << 15]u8 = undefined;
    @memset(&veryLongString, 'x');
    var manyStrings: [512][]const u8 = undefined;
    for (0..manyStrings.len) |i| {
        manyStrings[i] = try std.fmt.allocPrint(alloc, "{d}", .{1000 + i});
    }
    defer {
        for (manyStrings) |str| {
            alloc.free(str);
        }
    }

    // u16-width lengths 256..65535, non-invariant block
    var mediumA: [300]u8 = undefined;
    @memset(&mediumA, 'a');
    var mediumB: [500]u8 = undefined;
    @memset(&mediumB, 'b');

    // u16-width lengths 256..65535, invariant block
    var mediumC: [400]u8 = undefined;
    @memset(&mediumC, 'c');
    var mediumD: [400]u8 = undefined;
    @memset(&mediumD, 'd');

    // u32-width lengths 65536+, non-invariant block
    var bigA: [70000]u8 = undefined;
    @memset(&bigA, 'p');
    var bigB: [70001]u8 = undefined;
    @memset(&bigB, 'q');

    // u32-width lengths 65536+, invariant block
    var bigC: [70000]u8 = undefined;
    @memset(&bigC, 'e');
    var bigD: [70000]u8 = undefined;
    @memset(&bigD, 'f');

    // NOTE: uintBlockType64/uintBlockTypeInvariant64 require a length >= 1<<32
    // (a 4GiB+ string) to trigger, which isn't practical to allocate in a test.
    // The u8/u16/u32 cases above exercise the same code path structurally.

    const cases = [_]Case{
        .{
            .strings = &[_][]const u8{
                "192.168.0.1 - - [10/May/2025:13:00:00 +0000]" ++
                    " \"GET /index.html HTTP/1.1\" 200 1024 \"-\" \"Mozilla/5.0\"",
                "192.168.0.1 - - [10/May/2025:13:00:01 +0000]" ++
                    " \"GET /index.html HTTP/1.1\" 200 1024 \"-\" \"Mozilla/5.0\"",
                "192.168.0.1 - - [10/May/2025:13:00:02 +0000]" ++
                    " \"GET /index.html HTTP/1.1\" 200 1024 \"-\" \"Mozilla/5.0\"",
            },
        },
        .{
            .strings = &[_][]const u8{
                "foo",
                "bar",
            },
        },
        .{
            .strings = &[_][]const u8{
                "foo",
                "foo",
                "foo",
            },
        },
        .{
            .strings = &[_][]const u8{
                &veryLongString,
            },
        },
        .{
            .strings = manyStrings[0..],
        },
        .{
            // non-invariant, u8-width lengths
            .strings = &[_][]const u8{ "a", "bb", "ccc" },
        },
        .{
            // empty input
            .strings = &[_][]const u8{},
        },
        .{
            // zero-length strings mixed with non-zero
            .strings = &[_][]const u8{ "", "abc", "" },
        },
        .{
            .strings = &[_][]const u8{ &mediumA, &mediumB },
        },
        .{
            .strings = &[_][]const u8{ &mediumC, &mediumD },
        },
        .{
            .strings = &[_][]const u8{ &bigA, &bigB },
        },
        .{
            .strings = &[_][]const u8{ &bigC, &bigD },
        },
    };

    for (cases) |case| {
        var encoder = try Packer.init(alloc);
        defer encoder.deinit();

        var bound = try encoder.packValuesInterBound(case.strings);
        defer bound.deinit(alloc);
        const packedValues = try alloc.alloc(u8, bound.lensBound + bound.valuesBound);
        defer alloc.free(packedValues);
        const compressionPool = try CompressionPool.init(alloc, 1);
        defer compressionPool.deinit(alloc);
        const decompressionPool = try DecompressionPool.init(alloc, 1);
        defer decompressionPool.deinit(alloc);
        const n = try packValues(compressionPool, testing.io, packedValues, bound);

        var unpacker: Unpacker(false) = .init(decompressionPool);
        defer unpacker.deinit(alloc);
        const unpacked = try unpacker.unpackValues(testing.io, alloc, packedValues[0..n], case.strings.len);
        defer alloc.free(unpacked);

        try testing.expectEqual(case.strings.len, unpacked.len);
        for (case.strings, unpacked) |original, decoded| {
            try testing.expectEqualStrings(original, decoded);
        }
    }
}
