const std = @import("std");
const encoding = @import("encoding");
const Encoder = encoding.Encoder;
const Unpacker = @import("Unpacker.zig");

const Width = struct {
    max: u64,
    size: usize,
    block: u8,
    blockCell: u8,
};
pub const uintBlockType8: u8 = 0;
pub const uintBlockType16: u8 = 1;
pub const uintBlockType32: u8 = 2;
pub const uintBlockType64: u8 = 3;
pub const uintBlockTypeCell8: u8 = 4;
pub const uintBlockTypeCell16: u8 = 5;
pub const uintBlockTypeCell32: u8 = 6;
pub const uintBlockTypeCell64: u8 = 7;

const widths = [_]Width{
    .{ .max = (1 << 8), .block = uintBlockType8, .blockCell = uintBlockTypeCell8, .size = @sizeOf(u8) },
    .{ .max = (1 << 16), .block = uintBlockType16, .blockCell = uintBlockTypeCell16, .size = @sizeOf(u16) },
    .{ .max = (1 << 32), .block = uintBlockType32, .blockCell = uintBlockTypeCell32, .size = @sizeOf(u32) },
    .{ .max = ~@as(u64, 0), .block = uintBlockType64, .blockCell = uintBlockTypeCell64, .size = @sizeOf(u64) },
};
fn pickWidth(maxLen: u64) Width {
    for (widths) |w| {
        if (maxLen < w.max) return w;
    }
    std.debug.panic("unexpected int width, given len={}", .{maxLen});
}

pub const compressionKindPlain: u8 = 0;
pub const compressionKindZstd: u8 = 1;

const Self = @This();

allocator: std.mem.Allocator,
lengths: std.ArrayList(u64),

pub fn init(allocator: std.mem.Allocator) !*Self {
    const e = try allocator.create(Self);
    e.* = .{
        .allocator = allocator,
        // TODO: reuse a buffer from values encoder,
        // parsed holds same amount of data in case of u64 parsing
        .lengths = std.ArrayList(u64).empty,
    };
    return e;
}

pub fn deinit(self: *Self) void {
    self.lengths.deinit(self.allocator);
    self.allocator.destroy(self);
}

pub fn packValues(self: *Self, values: [][]const u8) ![]u8 {
    defer self.lengths.clearRetainingCapacity();
    try self.lengths.ensureUnusedCapacity(self.allocator, values.len);
    var lenSum: usize = 0;
    for (values) |v| {
        self.lengths.appendAssumeCapacity(@intCast(v.len));
        lenSum += v.len;
    }

    var maxLen: u64 = 0;
    for (self.lengths.items) |n| {
        if (n > maxLen) maxLen = n;
    }

    var stackFba = std.heap.stackFallback(2048, self.allocator);
    const fba = stackFba.get();
    var interBuf: []u8 = &[_]u8{};
    defer {
        if (interBuf.len > 0) fba.free(interBuf);
    }
    const areCells = (self.lengths.items.len >= 2) and areNumbersSame(self.lengths.items[0..]);
    const w = pickWidth(maxLen);
    if (areCells) {
        interBuf = try fba.alloc(u8, 1 + w.size);
        var enc = Encoder.init(interBuf);
        enc.writeInt(u8, w.blockCell);
        enc.writeIntBytes(w.size, self.lengths.items[0]);
    } else {
        interBuf = try fba.alloc(u8, 1 + w.size * self.lengths.items.len);
        var enc = Encoder.init(interBuf);
        _ = enc.writeInt(u8, w.block);
        for (self.lengths.items) |n| _ = enc.writeIntBytes(w.size, n);
    }

    // Optimize: if all values are the same, only pack the first one
    const valuesAreSame = (values.len >= 2) and areValuesSame(values);
    const valuesToPack = if (valuesAreSame) values[0..1] else values;
    const packSum = if (valuesAreSame) values[0].len else lenSum;

    // TODO: perhaps fixed buffer is useful here
    const valuesBuf = try self.allocator.alloc(u8, packSum);
    defer self.allocator.free(valuesBuf);
    var bufOffset: usize = 0;
    for (valuesToPack) |value| {
        @memcpy(valuesBuf[bufOffset .. bufOffset + value.len], value);
        bufOffset += value.len;
    }

    // Calculate bounds for both encoded parts
    const lensBound = try packBytesBound(interBuf.len);
    const valuesBound = try packBytesBound(valuesBuf.len);

    // Allocate once for both encoded lengths and values
    const totalBound = lensBound + valuesBound;
    const result = try self.allocator.alloc(u8, totalBound);
    errdefer self.allocator.free(result);

    // Pack lengths and values into different slices of the same buffer
    const encodedLensSize = try packBytes(fba, result[0..lensBound], interBuf);
    const encodedValuesSize = try packBytes(fba, result[encodedLensSize..], valuesBuf);

    // Return the exact slice we used (not the whole bound)
    // TODO: benchmark whether the realloc worth it or better to return the entire slice,
    // perhaps worth adding a metric on relation of actualSize to result.len
    const actualSize = encodedLensSize + encodedValuesSize;
    return self.allocator.realloc(result, actualSize);
}

fn packBytesBound(src_len: usize) !usize {
    if (src_len < 128) {
        // 1 compression kind, 1 len, len of the buf
        return 2 + src_len;
    }
    // 1 compression kind, 10 compressed len via leb128, len of the compressed (worst case)
    const compressSize = try encoding.compressBound(src_len);
    return 11 + compressSize;
}

fn packBytes(fba: std.mem.Allocator, dest: []u8, src: []u8) !usize {
    if (src.len < 128) {
        // skip compression, up to 127 can be in a single byte to be compatible with leb128
        // 1 compression kind, 1 len, len of the buf
        var enc = Encoder.init(dest);
        enc.writeInt(u8, compressionKindPlain);
        enc.writeInt(u8, @intCast(src.len));
        enc.writeBytes(src);
        return enc.offset;
    }

    const compressSize = try encoding.compressBound(src.len);
    const compressed = try fba.alloc(u8, compressSize);
    defer fba.free(compressed);
    const compressedSize = try encoding.compressAuto(compressed, src);

    // 1 compression kind, 10 compressed len via leb128, len of the compressed
    var enc = Encoder.init(dest);
    enc.writeInt(u8, compressionKindZstd);
    enc.writeVarInt(compressedSize);
    enc.writeBytes(compressed[0..compressedSize]);
    return enc.offset;
}

pub fn areNumbersSame(a: []const u64) bool {
    if (a.len == 0) return false;
    const v = a[0];
    for (a[1..]) |x| if (x != v) return false;
    return true;
}

fn areValuesSame(values: []const []const u8) bool {
    if (values.len == 0) return false;
    const first = values[0];
    for (values[1..]) |v| {
        if (!std.mem.eql(u8, v, first)) return false;
    }
    return true;
}

test "Packer.packValuesRoundtrip" {
    const allocator = std.testing.allocator;

    const Case = struct {
        strings: []const []const u8,
    };

    const veryLongString = try allocator.alloc(u8, 2 << 15);
    defer allocator.free(veryLongString);
    @memset(veryLongString, 'x');
    var manyStrings: [1000][]const u8 = undefined;
    for (0..manyStrings.len) |i| {
        manyStrings[i] = try std.fmt.allocPrint(allocator, "log {d}", .{1000 + i});
    }
    defer {
        for (manyStrings) |str| {
            allocator.free(str);
        }
    }
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
                veryLongString,
            },
        },
        .{
            .strings = manyStrings[0..],
        },
    };

    for (cases) |case| {
        const encoder = try Self.init(allocator);
        defer encoder.deinit();

        const packedValues = try encoder.packValues(@constCast(case.strings));
        defer allocator.free(packedValues);

        const unpacker = try Unpacker.init(allocator);
        defer unpacker.deinit(allocator);
        const unpacked = try unpacker.unpackValues(allocator, packedValues, case.strings.len);
        defer allocator.free(unpacked);

        try std.testing.expectEqual(case.strings.len, unpacked.len);
        for (case.strings, unpacked) |original, decoded| {
            try std.testing.expectEqualStrings(original, decoded);
        }
    }
}
