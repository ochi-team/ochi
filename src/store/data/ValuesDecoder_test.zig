const std = @import("std");

/// ValuesDecoder decodes values encoded by ValuesEncoder back to string representations.
const ValuesDecoder = @import("ValuesDecoder.zig");

const testing = std.testing;

test "ValuesDecoder.decodeUint8String" {
    const allocator = testing.allocator;
    var decoder: ValuesDecoder = .{};
    defer decoder.deinit(allocator);

    // Ensure capacity for writing
    try decoder.buf.ensureUnusedCapacity(allocator, 16);

    decoder.buf.clearRetainingCapacity();
    decoder.decodeUint8String(0);
    try testing.expectEqualStrings("0", decoder.buf.items);

    decoder.buf.clearRetainingCapacity();
    decoder.decodeUint8String(9);
    try testing.expectEqualStrings("9", decoder.buf.items);

    decoder.buf.clearRetainingCapacity();
    decoder.decodeUint8String(42);
    try testing.expectEqualStrings("42", decoder.buf.items);

    decoder.buf.clearRetainingCapacity();
    decoder.decodeUint8String(99);
    try testing.expectEqualStrings("99", decoder.buf.items);

    decoder.buf.clearRetainingCapacity();
    decoder.decodeUint8String(100);
    try testing.expectEqualStrings("100", decoder.buf.items);

    decoder.buf.clearRetainingCapacity();
    decoder.decodeUint8String(199);
    try testing.expectEqualStrings("199", decoder.buf.items);

    decoder.buf.clearRetainingCapacity();
    decoder.decodeUint8String(200);
    try testing.expectEqualStrings("200", decoder.buf.items);

    decoder.buf.clearRetainingCapacity();
    decoder.decodeUint8String(255);
    try testing.expectEqualStrings("255", decoder.buf.items);
}

test "ValuesDecoder.decodeIPv4String" {
    const allocator = testing.allocator;
    var decoder: ValuesDecoder = .{};
    defer decoder.deinit(allocator);

    try decoder.buf.ensureUnusedCapacity(allocator, 16);

    // 1.2.3.4 = (1 << 24) | (2 << 16) | (3 << 8) | 4
    const ip: u32 = (1 << 24) | (2 << 16) | (3 << 8) | 4;
    decoder.buf.clearRetainingCapacity();
    decoder.decodeIPv4String(ip);
    try testing.expectEqualStrings("1.2.3.4", decoder.buf.items);

    const ip2: u32 = (192 << 24) | (168 << 16) | (1 << 8) | 1;
    decoder.buf.clearRetainingCapacity();
    decoder.decodeIPv4String(ip2);
    try testing.expectEqualStrings("192.168.1.1", decoder.buf.items);
}

test "ValuesDecoder.decode uint8 grows output buffer" {
    const allocator = testing.allocator;
    const io = testing.io;

    var decoder: ValuesDecoder = .{};
    defer decoder.deinit(allocator);

    const encoded = [_][1]u8{
        .{0},
        .{9},
        .{42},
        .{255},
    };
    var values = [_][]const u8{
        encoded[0][0..],
        encoded[1][0..],
        encoded[2][0..],
        encoded[3][0..],
    };

    try testing.expectEqual(0, decoder.buf.capacity);
    try decoder.decode(io, allocator, values[0..], .uint8, &.{});

    try testing.expectEqualDeep(&[_][]const u8{
        "0",
        "9",
        "42",
        "255",
    }, values[0..]);
}

test "ValuesDecoder.decode ipv4 grows output buffer" {
    const allocator = testing.allocator;
    const io = testing.io;

    var decoder: ValuesDecoder = .{};
    defer decoder.deinit(allocator);

    const encoded = [_][4]u8{
        .{ 1, 2, 3, 4 },
        .{ 192, 168, 1, 1 },
        .{ 255, 255, 255, 255 },
    };
    var values = [_][]const u8{
        encoded[0][0..],
        encoded[1][0..],
        encoded[2][0..],
    };

    try testing.expectEqual(0, decoder.buf.capacity);
    try decoder.decode(io, allocator, values[0..], .ipv4, &.{});

    try testing.expectEqualDeep(&[_][]const u8{
        "1.2.3.4",
        "192.168.1.1",
        "255.255.255.255",
    }, values[0..]);
}

test "ValuesDecoder.decode dict replaces previous dictionary" {
    const allocator = testing.allocator;
    const io = testing.io;

    var decoder: ValuesDecoder = .{};
    defer decoder.deinit(allocator);

    const firstEncoded = [_][1]u8{
        .{0},
        .{1},
    };
    var firstValues = [_][]const u8{
        firstEncoded[0][0..],
        firstEncoded[1][0..],
    };
    const firstDict = [_][]const u8{ "alpha", "beta" };

    try decoder.decode(io, allocator, firstValues[0..], .dict, &firstDict);
    try testing.expectEqualDeep(&[_][]const u8{ "alpha", "beta" }, firstValues[0..]);

    const secondEncoded = [_][1]u8{
        .{1},
        .{0},
        .{1},
    };
    var secondValues = [_][]const u8{
        secondEncoded[0][0..],
        secondEncoded[1][0..],
        secondEncoded[2][0..],
    };
    const secondDict = [_][]const u8{ "warn", "error" };

    try decoder.decode(io, allocator, secondValues[0..], .dict, &secondDict);
    try testing.expectEqualDeep(&[_][]const u8{ "error", "warn", "error" }, secondValues[0..]);
}
