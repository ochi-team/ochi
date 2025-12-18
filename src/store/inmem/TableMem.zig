const std = @import("std");

const Field = @import("../lines.zig").Field;
const Line = @import("../lines.zig").Line;
const lineLessThan = @import("../lines.zig").lineLessThan;
const fieldLessThan = @import("../lines.zig").fieldLessThan;
const SID = @import("../lines.zig").SID;

const StreamWriter = @import("StreamWriter.zig");
const BlockWriter = @import("BlockWriter.zig");
const TableHeader = @import("TableHeader.zig");

// 2mb block size, on merging it takes double amount up to 4mb
// TODO: benchmark whether 2.5-3kb performs better
pub const maxBlockSize = 2 * 1024 * 1024;

pub const Error = error{
    EmptyLines,
};

const Self = @This();

streamWriter: *StreamWriter,
tableHeader: *TableHeader,

pub fn init(allocator: std.mem.Allocator) !*Self {
    const streamWriter = try StreamWriter.init(allocator, 1);
    errdefer streamWriter.deinit(allocator);

    const th = try TableHeader.init(allocator);
    errdefer th.deinit(allocator);

    const p = try allocator.create(Self);
    p.* = Self{
        .streamWriter = streamWriter,
        .tableHeader = th,
    };

    return p;
}
pub fn deinit(self: *Self, allocator: std.mem.Allocator) void {
    self.streamWriter.deinit(allocator);
    self.tableHeader.deinit(allocator);
    allocator.destroy(self);
}

pub fn addLines(self: *Self, allocator: std.mem.Allocator, lines: []*const Line) !void {
    if (lines.len == 0) {
        return Error.EmptyLines;
    }

    var blockWriter = try BlockWriter.init(allocator);
    defer blockWriter.deinit(allocator);

    var streamI: usize = 0;
    var blockSize: u32 = 0;
    var prevSID: SID = lines[0].sid;

    std.mem.sortUnstable(*const Line, lines, {}, lineLessThan);
    for (lines, 0..) |line, i| {
        std.mem.sortUnstable(Field, line.fields, {}, fieldLessThan);

        if (blockSize >= maxBlockSize or !line.sid.eql(&prevSID)) {
            try blockWriter.writeLines(allocator, prevSID, lines[streamI..i], self.streamWriter);
            prevSID = line.sid;
            blockSize = 0;
            streamI = i;
        }
        blockSize += line.fieldsSize();
    }
    if (streamI != lines.len) {
        try blockWriter.writeLines(allocator, prevSID, lines[streamI..], self.streamWriter);
    }
    try blockWriter.finish(allocator, self.streamWriter, self.tableHeader);
}

const BlockHeader = @import("block_header.zig").BlockHeader;
const IndexBlockHeader = @import("IndexBlockHeader.zig");
const TimestampsEncoder = @import("TimestampsEncoder.zig");
const EncodingType = @import("TimestampsEncoder.zig").EncodingType;
const encoding = @import("encoding");
test "addLines" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, testAddLines, .{});
}

fn testAddLines(allocator: std.mem.Allocator) !void {
    var fields1 = [_]Field{
        .{ .key = "level", .value = "info" },
        .{ .key = "app", .value = "seq" },
    };
    var fields2 = [_]Field{
        .{ .key = "level", .value = "warn" },
        .{ .key = "app", .value = "seq" },
    };
    // unordered timestamps in lines so that it tests its sorting
    var lines = [_]*const Line{
        &.{
            .timestampNs = 2,
            .sid = .{ .id = 1, .tenantID = "1234" },
            .fields = fields2[0..],
            .encodedTags = undefined,
        },
        &.{
            .timestampNs = 1,
            .sid = .{ .id = 1, .tenantID = "1234" },
            .fields = fields1[0..],
            .encodedTags = undefined,
        },
    };

    const memTable = try Self.init(allocator);
    defer memTable.deinit(allocator);
    try memTable.addLines(allocator, lines[0..]);

    const timestampsContent = memTable.streamWriter.timestampsBuf.items;
    const indexContent = memTable.streamWriter.indexBuf.items;

    // Validate timestamps
    {
        var dst: [2]u64 = undefined;
        const timestampsEncoder = try TimestampsEncoder.init(allocator);
        defer timestampsEncoder.deinit(allocator);
        try timestampsEncoder.decode(dst[0..], timestampsContent);

        try std.testing.expectEqualSlices(u64, &[_]u64{ 1, 2 }, &dst);
    }

    // Validate block header
    {
        const decompressedSize = try encoding.getFrameContentSize(indexContent);
        const decompressedBuf = try allocator.alloc(u8, decompressedSize);
        defer allocator.free(decompressedBuf);
        _ = try encoding.decompress(decompressedBuf, indexContent);

        const blockHeader = BlockHeader.decode(decompressedBuf);

        // TODO: compare all the fields in one expect
        try std.testing.expectEqualStrings("1234", blockHeader.sid.tenantID);
        try std.testing.expectEqual(1, blockHeader.sid.id);
        try std.testing.expectEqual(140, blockHeader.size);
        try std.testing.expectEqual(2, blockHeader.len);

        try std.testing.expectEqual(0, blockHeader.timestampsHeader.offset);
        try std.testing.expectEqual(17, blockHeader.timestampsHeader.size);
        try std.testing.expectEqual(1, blockHeader.timestampsHeader.min);
        try std.testing.expectEqual(2, blockHeader.timestampsHeader.max);
        try std.testing.expectEqual(EncodingType.ZDeltapack, blockHeader.timestampsHeader.encodingType);
    }

    // validate meta index
    {
        const metaIndexContent = memTable.streamWriter.metaIndexBuf.items;
        try std.testing.expect(metaIndexContent.len > 0);

        const decompressedSize = try encoding.getFrameContentSize(metaIndexContent);
        const decompressedBuf = try allocator.alloc(u8, decompressedSize);
        defer allocator.free(decompressedBuf);
        _ = try encoding.decompress(decompressedBuf, metaIndexContent);

        const decodedIndexBlockHeader = IndexBlockHeader.decode(decompressedBuf);

        // TODO: compare all the fields in one expect
        try std.testing.expectEqualStrings("1234", decodedIndexBlockHeader.sid.tenantID);
        try std.testing.expectEqual(1, decodedIndexBlockHeader.sid.id);
        try std.testing.expectEqual(1, decodedIndexBlockHeader.minTs);
        try std.testing.expectEqual(2, decodedIndexBlockHeader.maxTs);
        try std.testing.expectEqual(0, decodedIndexBlockHeader.offset);
        try std.testing.expectEqual(@as(u64, @intCast(indexContent.len)), decodedIndexBlockHeader.size);
    }

    // validate bloom filters
    {
        const messageBloomTokensContent = memTable.streamWriter.messageBloomTokensBuf.items;
        const bloomTokensList = memTable.streamWriter.bloomTokensList.items;
        const messageBloomValuesContent = memTable.streamWriter.messageBloomValuesBuf.items;
        const bloomValuesList = memTable.streamWriter.bloomValuesList.items;

        try std.testing.expectEqual(0, messageBloomTokensContent.len);
        try std.testing.expectEqual(0, messageBloomValuesContent.len);

        for (bloomTokensList) |bloomBuf| {
            try std.testing.expectEqual(0, bloomBuf.items.len);
        }
        for (bloomValuesList) |bloomValuesBuf| {
            try std.testing.expect(bloomValuesBuf.items.len > 0);
        }
    }
}

test "addLinesErrorOnEmpty" {
    var lines = [_]*const Line{};
    const memTable = try Self.init(std.testing.allocator);
    defer memTable.deinit(std.testing.allocator);
    const err = memTable.addLines(std.testing.allocator, lines[0..]);
    try std.testing.expectError(Error.EmptyLines, err);
}
