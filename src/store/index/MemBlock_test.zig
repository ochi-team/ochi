const std = @import("std");
const Allocator = std.mem.Allocator;

const encoding = @import("encoding");
const Encoder = encoding.Encoder;
const CompressionPool = @import("../compression/CompressionPool.zig");
const DecompressionPool = @import("../compression/DecompressionPool.zig");

const EntriesBlock = @import("EntriesBlock.zig");

const MemBlock = @import("MemBlock.zig");
const MemEntry = MemBlock.MemEntry;
const EncodedMemBlock = MemBlock.EncodedMemBlock;

const testing = std.testing;

fn createTestMemBlock(alloc: Allocator, items: []const []const u8) !*MemBlock {
    var total: u32 = 0;
    for (items) |item| total += @intCast(item.len);
    var block = try MemBlock.init(alloc, .{
        .maxMemBlockSize = total + 16,
        .blocksCountHint = items.len,
    });
    errdefer block.deinit(alloc);
    for (items) |item| {
        const ok = block.add(item);
        try testing.expect(ok);
    }
    return block;
}

fn allocFilled(alloc: Allocator, fill: u8, len: usize) ![]u8 {
    const buf = try alloc.alloc(u8, len);
    if (len == 0) return buf;
    buf[0] = fill;
    if (len > 1) @memset(buf[1..], fill + 1);
    return buf;
}

test "MemBlock.add respects max size and reset clears state" {
    const alloc = testing.allocator;

    var block = try MemBlock.init(alloc, .{
        .maxMemBlockSize = 6,
        .blocksCountHint = 3,
    });
    defer block.deinit(alloc);

    try testing.expect(block.add("abc"));
    try testing.expect(block.add("de"));
    try testing.expect(block.add("f"));
    try testing.expectEqual(@as(u32, 6), block.buf.items.len);
    try testing.expect(!block.add("g"));
    try testing.expectEqual(@as(u32, 6), block.buf.items.len);

    block.reset();
    try testing.expectEqual(@as(usize, 0), block.memEntries.items.len);
    try testing.expectEqual(@as(u32, 0), block.buf.items.len);
}

test "MemBlock.add returns false when memEntries capacity is exhausted first" {
    const alloc = testing.allocator;

    var block = try MemBlock.init(alloc, .{ .maxMemBlockSize = 99 });
    defer block.deinit(alloc);

    // Reproduce production-like shape where entry pointer capacity can be smaller
    // than available bytes in the backing buffer.
    block.memEntries.deinit(alloc);
    block.memEntries = try std.ArrayList(MemEntry).initCapacity(alloc, 3);

    try testing.expectEqual(@as(usize, 3), block.memEntries.capacity);
    try testing.expect(block.buf.capacity >= 99);

    try testing.expect(block.add("a"));
    try testing.expect(block.add("b"));
    try testing.expect(block.add("c"));

    // No panic: the block reports it cannot fit another item.
    try testing.expect(!block.add("d"));
    try testing.expectEqual(@as(usize, 3), block.memEntries.items.len);
    try testing.expectEqual(@as(u32, 3), block.buf.items.len);
}

test "MemBlock.encode/decode plain and zstd cases" {
    const alloc = testing.allocator;
    const Case = struct {
        expectedEncodedBlock: EncodedMemBlock,
        items: []const []const u8,
        itemsSorted: []const []const u8,
    };

    const len = 200;
    const a = try allocFilled(alloc, 'x', len);
    const b = try allocFilled(alloc, 'y', len);
    const c = try allocFilled(alloc, 'z', len);
    defer alloc.free(a);
    defer alloc.free(b);
    defer alloc.free(c);
    const zstdItems = [_][]const u8{ a, b, c };

    const plainItems = &[_][]const u8{ "pre-c", "pre-a", "pre-b" };
    const plainItemsSorted = &[_][]const u8{ "pre-a", "pre-b", "pre-c" };

    const cases = [_]Case{
        .{
            .expectedEncodedBlock = .{
                .encodingType = .plain,
                .firstEntry = plainItemsSorted[0],
                .prefix = "pre-",
                .itemsCount = 3,
            },
            .items = plainItems,
            .itemsSorted = plainItemsSorted,
        },
        .{
            .expectedEncodedBlock = .{
                .encodingType = .zstd,
                .firstEntry = zstdItems[0],
                .prefix = "",
                .itemsCount = 3,
            },
            .items = zstdItems[0..],
            .itemsSorted = zstdItems[0..],
        },
    };

    for (cases) |case| {
        const compressionPool = try CompressionPool.init(alloc, 1);
        defer compressionPool.deinit(alloc);
        const decompressionPool = try DecompressionPool.init(alloc, 1);
        defer decompressionPool.deinit(alloc);

        var block = try createTestMemBlock(alloc, case.items);
        defer block.deinit(alloc);
        block.sortData();

        var entriesBlock = EntriesBlock{};
        defer entriesBlock.deinit(alloc);
        const encoded = try block.encode(testing.io, alloc, compressionPool, &entriesBlock);
        try testing.expectEqualDeep(case.expectedEncodedBlock, encoded);

        var decoded = try MemBlock.init(alloc, .{
            .maxMemBlockSize = 16,
            .blocksCountHint = case.items.len,
        });
        defer decoded.deinit(alloc);
        try decoded.decode(testing.io, alloc, decompressionPool, &entriesBlock, encoded.firstEntry, encoded.prefix, encoded.itemsCount, encoded.encodingType);

        try testing.expectEqualStrings(block.prefix, decoded.prefix);
        try testing.expectEqualStrings(block.prefix, case.expectedEncodedBlock.prefix);
        try testing.expectEqual(block.memEntries.items.len, decoded.memEntries.items.len);
        try testing.expectEqual(block.memEntries.items.len, case.items.len);
        for (0..block.memEntries.items.len) |i| {
            try testing.expectEqualStrings(block.get(i), decoded.get(i));
            try testing.expectEqualStrings(case.itemsSorted[i], block.get(i));
        }
    }
}

test "MemBlock.decodePlain handles min and max lens values" {
    const alloc = testing.allocator;

    const Case = struct {
        secondLen: usize,
    };

    // Focus on varint boundary lengths for the plain decoder:
    const cases = [_]Case{
        // - 0 checks zero-length items and empty slice handling.
        .{ .secondLen = 0 },
        // - 16383 is the largest value that still fits in a 2-byte varint.
        .{ .secondLen = 16383 },
        // - 16384 is the first value that requires a 3-byte varint and would fail
        //   if decodePlain assumed lens data always fit in 2 bytes.
        .{ .secondLen = 16384 },
    };

    inline for (cases) |case| {
        var entriesBlock = EntriesBlock{};
        defer entriesBlock.deinit(alloc);

        // Only append item bytes when the second item is non-empty. This ensures
        // decodePlain doesn't assume a positive length or read past the buffer.
        if (case.secondLen > 0) {
            // const second = try allocFilled(alloc, 'b', case.secondLen);
            // defer alloc.free(second);
            var second: [case.secondLen]u8 = undefined;
            try entriesBlock.entriesBuf.appendSlice(alloc, &second);
        }

        // Encode a single varint length for the second item. The first item's length
        // is implicit in decodePlain (from firstItem and prefix), so lensData holds
        // only item[1..].
        const lensBound = Encoder.varIntBound(@intCast(case.secondLen));
        try entriesBlock.lensBuf.ensureUnusedCapacity(alloc, lensBound);
        var enc = Encoder.init(entriesBlock.lensBuf.unusedCapacitySlice());
        enc.writeVarInt(@intCast(case.secondLen));
        entriesBlock.lensBuf.items.len = enc.offset;

        var block = try MemBlock.init(alloc, .{
            .maxMemBlockSize = @intCast(1 + case.secondLen),
            .blocksCountHint = 2,
        });
        defer block.deinit(alloc);
        // Use an empty prefix so decodePlain must copy bytes directly from itemsData
        // (no prefix reconstruction or prefix lens logic involved).
        block.prefix = "";

        // Call decodePlain directly to isolate its varint length handling from the
        // full decode flow (which can route to zstd and other paths).
        try block.decodePlain(alloc, &entriesBlock, "a", 2);
        try testing.expectEqual(@as(usize, 2), block.memEntries.items.len);
        try testing.expectEqualSlices(u8, "a", block.get(0));
        // Ensure the decoded second item length matches the varint value, including zero.
        try testing.expectEqual(@as(usize, case.secondLen), block.get(1).len);
    }
}

test "MemBlock.encodePlain breaks on 3-byte varint len" {
    const alloc = testing.allocator;

    var second: [16384]u8 = undefined;
    @memset(&second, 'b');

    var block = try MemBlock.init(alloc, .{
        .maxMemBlockSize = @intCast(1 + second.len + 16),
        .blocksCountHint = 2,
    });
    defer block.deinit(alloc);
    try testing.expect(block.add("a"));
    try testing.expect(block.add(second[0..]));

    block.prefix = "";

    var entriesBlock = EntriesBlock{
        .entriesBuf = try std.ArrayList(u8).initCapacity(alloc, second.len + 1),
        .lensBuf = try std.ArrayList(u8).initCapacity(alloc, 2),
    };
    defer entriesBlock.deinit(alloc);

    // encodePlain reserves 2 bytes per len, but 16384 needs a 3-byte varint.
    // Keeping lensBuf capacity fixed at 2 makes this assumption fail reliably.
    try block.encodePlain(alloc, &entriesBlock);
}
