const std = @import("std");

const encoding = @import("encoding");

const IndexBlockHeader = @import("IndexBlockHeader.zig");
const CompressionPool = @import("../compression/CompressionPool.zig");
const DecompressionPool = @import("../compression/DecompressionPool.zig");
const EncodingType = @import("TimestampsEncoder.zig").EncodingType;

const BlockHeader = @import("BlockHeader.zig");

const testing = std.testing;

test "BlockHeader encode/decode and decodeIndexWindow" {
    const alloc = testing.allocator;

    const Case = struct {
        header: BlockHeader,
        expectedLen: usize,
    };

    const cases = [_]Case{
        .{
            .header = .{
                .sid = .{
                    .tenantID = 1,
                    .id = 1,
                },
                .size = 0,
                .len = 0,
                .timestampsHeader = .{
                    .offset = 0,
                    .size = 0,
                    .min = 0,
                    .max = 0,
                    .encodingType = .Undefined,
                },
                .columnsHeaderOffset = 0,
                .columnsHeaderSize = 0,
                .columnsHeaderIndexOffset = 0,
                .columnsHeaderIndexSize = 0,
            },
            .expectedLen = 69,
        },
        .{
            .header = .{
                .sid = .{
                    .tenantID = 1,
                    .id = 42,
                },
                .size = 1234,
                .len = 123,
                .timestampsHeader = .{
                    .offset = 1,
                    .size = 2,
                    .min = 50,
                    .max = 100,
                    .encodingType = .ZDeltapack,
                },
                .columnsHeaderOffset = 10,
                .columnsHeaderSize = 20,
                .columnsHeaderIndexOffset = 30,
                .columnsHeaderIndexSize = 40,
            },
            .expectedLen = 69,
        },
        .{
            .header = .{
                .sid = .{
                    .tenantID = 1,
                    .id = std.math.maxInt(u128),
                },
                .size = std.math.maxInt(u32),
                .len = std.math.maxInt(u32),
                .timestampsHeader = .{
                    .offset = std.math.maxInt(u64),
                    .size = std.math.maxInt(u64),
                    .min = std.math.maxInt(u64),
                    .max = std.math.maxInt(u64),
                    .encodingType = EncodingType.ZDeltapack,
                },
                .columnsHeaderOffset = std.math.maxInt(usize),
                .columnsHeaderSize = std.math.maxInt(usize),
                .columnsHeaderIndexOffset = std.math.maxInt(usize),
                .columnsHeaderIndexSize = std.math.maxInt(usize),
            },
            .expectedLen = BlockHeader.encodeExpectedSize,
        },
    };

    var headers: [cases.len]BlockHeader = undefined;
    for (cases, 0..) |case, i| {
        var encodeBuf: [BlockHeader.encodeExpectedSize]u8 = undefined;
        const offset = case.header.encode(&encodeBuf);
        try testing.expectEqual(case.expectedLen, offset);

        const decoded = BlockHeader.decode(encodeBuf[0..offset]);
        try testing.expectEqual(offset, decoded.offset);
        try testing.expectEqualDeep(case.header, decoded.header);

        headers[i] = case.header;
    }

    var indexBuf = std.ArrayList(u8).empty;
    defer indexBuf.deinit(alloc);

    const WindowCase = struct {
        offset: u64,
        size: u64,
        expected: []const BlockHeader,
    };

    var windowCases = [_]WindowCase{
        .{ .offset = 0, .size = 0, .expected = headers[0..2] },
        .{ .offset = 0, .size = 0, .expected = headers[1..3] },
        .{ .offset = 0, .size = 0, .expected = headers[1..2] },
        .{ .offset = 0, .size = 0, .expected = headers[2..3] },
    };
    const compressionPool = try CompressionPool.init(alloc, 1);
    defer compressionPool.deinit(alloc);
    const decompressionPool = try DecompressionPool.init(alloc, 1);
    defer decompressionPool.deinit(alloc);

    for (&windowCases) |*windowCase| {
        const rawLen = windowCase.expected.len * BlockHeader.encodeExpectedSize;
        const raw = try alloc.alloc(u8, rawLen);
        defer alloc.free(raw);

        var off: usize = 0;
        for (windowCase.expected) |header| {
            const n = header.encode(raw[off .. off + BlockHeader.encodeExpectedSize]);
            off += n;
        }

        const bound = try encoding.compressBound(off);
        const compressed = try alloc.alloc(u8, bound);
        defer alloc.free(compressed);

        windowCase.offset = @intCast(indexBuf.items.len);
        const n = try compressionPool.compressAuto(testing.io, compressed, raw[0..off]);
        windowCase.size = @intCast(n);
        try indexBuf.appendSlice(alloc, compressed[0..n]);
    }

    for (windowCases) |windowCase| {
        var decoded = try std.ArrayList(BlockHeader).initCapacity(alloc, 4);
        defer decoded.deinit(alloc);

        const window = IndexBlockHeader{
            .sid = windowCase.expected[0].sid,
            .minTs = windowCase.expected[0].timestampsHeader.min,
            .maxTs = windowCase.expected[windowCase.expected.len - 1].timestampsHeader.max,
            .offset = windowCase.offset,
            .size = windowCase.size,
        };

        const start: usize = @intCast(windowCase.offset);
        const end = start + windowCase.size;
        try BlockHeader.decodeIndexWindow(
            testing.io,
            alloc,
            decompressionPool,
            &decoded,
            indexBuf.items[start..end],
            window,
        );
        try testing.expectEqualDeep(windowCase.expected, decoded.items);
    }
}
