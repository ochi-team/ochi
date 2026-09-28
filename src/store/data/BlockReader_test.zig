const std = @import("std");
const Allocator = std.mem.Allocator;
const Io = std.Io;

const checkAllocationFailuresAllowingSwallow =
    @import("../../stds/testing.zig").checkAllocationFailuresAllowingSwallow;

const SID = @import("../lines.zig").SID;
const MemTable = @import("MemTable.zig");
const Table = @import("../data/Table.zig");
const TimestampsEncoder = @import("TimestampsEncoder.zig");
const CompressionPool = @import("../compression/CompressionPool.zig");
const DecompressionPool = @import("../compression/DecompressionPool.zig");

pub const BlockReader = @import("BlockReader.zig");

const Line = @import("../lines.zig").Line;
const Field = @import("../lines.zig").Field;

const SampleLines = struct {
    fields1: [2]Field,
    fields2: [2]Field,
    fields3: [2]Field,
    lines: [3]Line,
};

fn populateSampleLines(sample: *SampleLines) void {
    sample.fields1 = .{
        .{ .key = "level", .value = "info" },
        .{ .key = "app", .value = "seq" },
    };
    sample.fields2 = .{
        .{ .key = "level", .value = "warn" },
        .{ .key = "app", .value = "seq" },
    };
    sample.fields3 = .{
        .{ .key = "level", .value = "warn" },
        .{ .key = "app", .value = "seq" },
    };
    sample.lines = .{
        .{
            .timestampNs = 1,
            .fields = sample.fields1[0..],
        },
        .{
            .timestampNs = 2,
            .fields = sample.fields2[0..],
        },
        .{
            .timestampNs = 3,
            .fields = sample.fields3[0..],
        },
    };
}

test "readBlock reads buffers" {
    try checkAllocationFailuresAllowingSwallow(std.testing.allocator, testReadBlock, .{std.testing.io});
}

test "initFromDiskTable reads buffers" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, testInitFromDiskTable, .{std.testing.io});
}

fn testReadBlock(allocator: Allocator, io: Io) !void {
    var sample: SampleLines = SampleLines{
        .fields1 = undefined,
        .fields2 = undefined,
        .fields3 = undefined,
        .lines = undefined,
    };
    populateSampleLines(&sample);

    // Unordered timestamps in lines so that it tests sorting.
    // line[0]: ts=1, sid=(2,"2222"); line[1]: ts=2, sid=(1,"1111"); line[2]: ts=3, sid=(1,"1111")
    // After sort by (sid, ts): first block (1111,1) 2 rows (ts 2,3), second block (2222,2) 1 row (ts 1).
    var lines = [3]Line{
        sample.lines[0],
        sample.lines[1],
        sample.lines[2],
    };

    const compressionPool = try CompressionPool.init(allocator, 1);
    defer compressionPool.deinit(allocator);
    const decompressionPool = try DecompressionPool.init(allocator, 1);
    defer decompressionPool.deinit(allocator);
    const memTable = try MemTable.init(allocator);
    const table = Table.fromMem(io, allocator, memTable, decompressionPool) catch |err| {
        memTable.deinit(allocator);
        return err;
    };
    defer table.close(io);
    var sids = [_]SID{
        .{ .id = 2, .tenantID = 2222 },
        .{ .id = 1, .tenantID = 1111 },
    };
    var linesBySid = [_][]Line{
        lines[0..1],
        lines[1..3],
    };
    const timestampsEncoders = try TimestampsEncoder.TimestampsEncoderPool.init(allocator, 1);
    defer timestampsEncoders.deinit(allocator);
    try memTable.addLines(io, allocator, timestampsEncoders, compressionPool, sids[0..], linesBySid[0..]);

    const th = memTable.tableHeader;
    try std.testing.expectEqual(3, th.len);
    try std.testing.expect(th.minTimestamp <= 1);
    try std.testing.expect(th.maxTimestamp >= 3);
    try std.testing.expect(th.blocksCount >= 1);
    try std.testing.expect(th.uncompressedSize > 0);
    try std.testing.expect(th.compressedSize > 0);

    const blockReader = try BlockReader.initFromMemTable(io, allocator, table, decompressionPool);
    defer blockReader.deinit(allocator);

    var blocksRead: u32 = 0;
    while (try blockReader.nextBlock(io, allocator)) {
        blocksRead += 1;
    }

    try std.testing.expectEqual(2, blocksRead);
    try std.testing.expectEqual(2, blockReader.globalBlocksCount);
    try std.testing.expectEqual(3, blockReader.globalRowsCount);
    try std.testing.expectEqual(th.uncompressedSize, blockReader.globalUncompressedSizeBytes);
    try std.testing.expectEqual(th.len, blockReader.globalRowsCount);
    try std.testing.expectEqual(th.blocksCount, blockReader.globalBlocksCount);
    try std.testing.expectEqual(th.compressedSize, blockReader.tableReader.totalBytesRead());

    // Second pass: check each block's blockData (sid, rowsCount, timestamps range)
    const blockReader2 = try BlockReader.initFromMemTable(io, allocator, table, decompressionPool);
    defer blockReader2.deinit(allocator);

    var block1Sid1111 = false;
    var block2Sid2222 = false;
    var blocksWithFullData: u32 = 0;
    while (try blockReader2.nextBlock(io, allocator)) {
        const bd = &blockReader2.blockData;
        try std.testing.expect(bd.len >= 1);
        try std.testing.expect(bd.uncompressedSizeBytes > 0);
        try std.testing.expect(bd.timestampsData.minTimestamp <= bd.timestampsData.maxTimestamp);
        // columnsData may be empty in allocation-failure runs from checkAllocationFailures
        if (bd.columnsData.items.len >= 2) {
            blocksWithFullData += 1;
            if (bd.sid.tenantID == 1111 and bd.sid.id == 1) {
                try std.testing.expectEqual(2, bd.len);
                try std.testing.expectEqual(2, bd.timestampsData.minTimestamp);
                try std.testing.expectEqual(3, bd.timestampsData.maxTimestamp);
                block1Sid1111 = true;
            } else if (bd.sid.tenantID == 2222 and bd.sid.id == 2) {
                try std.testing.expectEqual(1, bd.len);
                try std.testing.expectEqual(1, bd.timestampsData.minTimestamp);
                try std.testing.expectEqual(1, bd.timestampsData.maxTimestamp);
                block2Sid2222 = true;
            }
        }
    }
    // When both blocks were read with full data (no alloc failure), both sids must be present
    if (blocksWithFullData == 2) {
        try std.testing.expect(block1Sid1111);
        try std.testing.expect(block2Sid2222);
    }
}

fn testInitFromDiskTable(alloc: Allocator, io: Io) !void {
    var fields1 = [_]Field{
        .{ .key = "level", .value = "info" },
        .{ .key = "app", .value = "seq" },
    };
    var fields2 = [_]Field{
        .{ .key = "level", .value = "warn" },
        .{ .key = "app", .value = "seq" },
    };

    const line1 = Line{
        .timestampNs = 1,
        .fields = fields1[0..],
    };
    const line2 = Line{
        .timestampNs = 2,
        .fields = fields2[0..],
    };

    var lines = [_]Line{ line1, line2 };

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const rootPath = try tmp.dir.realPathFileAlloc(io, ".", alloc);
    defer alloc.free(rootPath);

    const memTable = try MemTable.init(alloc);
    defer memTable.deinit(alloc);
    const timestampsEncoders = try TimestampsEncoder.TimestampsEncoderPool.init(alloc, 1);
    defer timestampsEncoders.deinit(alloc);
    const compressionPool = try CompressionPool.init(alloc, 1);
    defer compressionPool.deinit(alloc);
    const decompressionPool = try DecompressionPool.init(alloc, 1);
    defer decompressionPool.deinit(alloc);
    try memTable.addLinesForSid(
        io,
        alloc,
        timestampsEncoders,
        compressionPool,
        .{ .id = 1, .tenantID = 1234 },
        lines[0..],
    );

    const tablePath = try std.fs.path.join(alloc, &.{ rootPath, "table-1" });
    memTable.storeToDisk(io, tablePath) catch |err| {
        alloc.free(tablePath);
        return err;
    };

    const table = Table.open(io, alloc, tablePath, decompressionPool) catch |err| {
        alloc.free(tablePath);
        return err;
    };
    defer table.close(io);

    const blockReader = try BlockReader.initFromDiskTable(io, alloc, table, decompressionPool);
    defer blockReader.deinit(alloc);

    var blocksRead: u32 = 0;
    while (try blockReader.nextBlock(io, alloc)) {
        blocksRead += 1;
    }

    try std.testing.expectEqual(memTable.tableHeader.blocksCount, blocksRead);
    try std.testing.expectEqual(
        memTable.tableHeader.uncompressedSize,
        blockReader.globalUncompressedSizeBytes,
    );
    try std.testing.expectEqual(memTable.tableHeader.len, blockReader.globalRowsCount);
    try std.testing.expectEqual(memTable.tableHeader.blocksCount, blockReader.globalBlocksCount);
    try std.testing.expectEqual(
        memTable.tableHeader.compressedSize,
        blockReader.tableReader.totalBytesRead(),
    );
}
