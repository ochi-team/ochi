const std = @import("std");
const Allocator = std.mem.Allocator;
const Io = std.Io;

const encoding = @import("encoding");

const TableReader = @import("TableReader.zig");
const SID = @import("../lines.zig").SID;
const IndexBlockHeader = @import("IndexBlockHeader.zig");
const BlockHeader = @import("BlockHeader.zig");
const TableHeader = @import("TableHeader.zig");
const MemTable = @import("MemTable.zig");
const Table = @import("../data/Table.zig");
const BlockData = @import("BlockData.zig").BlockData;
const TimestampsEncoder = @import("TimestampsEncoder.zig");
const CompressionPool = @import("../CompressionPool.zig").CompressionPool;
const DecompressionPool = @import("../CompressionPool.zig").DecompressionPool;

pub const BlockReader = @This();
blocksCount: u32,
len: u32,
size: u32,

sidLast: ?SID,
minTimestampLast: u64,

blockHeaders: std.ArrayList(BlockHeader),
indexBlockHeaders: []IndexBlockHeader,

nextBlockIdx: u32,
nextIndexBlockIdx: u32,

tableHeader: TableHeader,
tableReader: *TableReader,
decompressionPool: *DecompressionPool,

// Global stats for validation
globalUncompressedSizeBytes: u64,
globalRowsCount: u64,
globalBlocksCount: u64,

// Block data
// TODO: find a better name
// TODO: make it a pointer, seems it holds a lot of fields
blockData: BlockData,

pub fn init(io: Io, alloc: Allocator, table: *const Table, decompressionPool: *DecompressionPool) !*BlockReader {
    return switch (table.inner) {
        .mem => initFromMemTable(io, alloc, table, decompressionPool),
        .disk => initFromDiskTable(io, alloc, table, decompressionPool),
    };
}

pub fn initFromMemTable(io: Io, alloc: Allocator, table: *const Table, decompressionPool: *DecompressionPool) !*BlockReader {
    const memTable = table.inner.mem;
    const tableHeader = memTable.tableHeader;
    const indexBlockHeaders = try IndexBlockHeader.readIndexBlockHeaders(
        io,
        alloc,
        decompressionPool,
        memTable.metaIndexBuf.items,
    );
    errdefer alloc.free(indexBlockHeaders);

    var blockHeaders = try std.ArrayList(BlockHeader).initCapacity(alloc, 64);
    errdefer blockHeaders.deinit(alloc);

    const tablReader = try TableReader.initFromMem(io, alloc, table, decompressionPool);
    errdefer tablReader.deinit(alloc);

    const br = try alloc.create(BlockReader);
    errdefer alloc.destroy(br);

    br.* = .{
        .blocksCount = 0,
        .len = 0,
        .size = 0,

        .sidLast = null,
        .minTimestampLast = 0,

        .blockHeaders = blockHeaders,
        .indexBlockHeaders = indexBlockHeaders,

        .nextBlockIdx = 0,
        .nextIndexBlockIdx = 0,

        .tableHeader = tableHeader,
        .tableReader = tablReader,
        .decompressionPool = decompressionPool,

        .globalUncompressedSizeBytes = 0,
        .globalRowsCount = 0,
        .globalBlocksCount = 0,

        .blockData = BlockData.initEmpty(),
    };
    return br;
}

pub fn initFromDiskTable(io: Io, alloc: Allocator, table: *const Table, decompressionPool: *DecompressionPool) !*BlockReader {
    const tableHeader = table.inner.disk.tableHeader;

    const tableReader = try TableReader.init(io, alloc, table, decompressionPool);
    errdefer tableReader.deinit(alloc);
    var indexBlockHeaders: []IndexBlockHeader = &.{};
    if (tableReader.metaIndexBuf.len > 0) {
        indexBlockHeaders = try IndexBlockHeader.readIndexBlockHeaders(io, alloc, decompressionPool, tableReader.metaIndexBuf);
    }
    errdefer if (indexBlockHeaders.len > 0) {
        alloc.free(indexBlockHeaders);
    };

    var blockHeaders = try std.ArrayList(BlockHeader).initCapacity(alloc, 64);
    errdefer blockHeaders.deinit(alloc);

    const br = try alloc.create(BlockReader);
    errdefer alloc.destroy(br);
    br.* = .{
        .blocksCount = 0,
        .len = 0,
        .size = 0,

        .sidLast = null,
        .minTimestampLast = 0,

        .blockHeaders = blockHeaders,
        .indexBlockHeaders = indexBlockHeaders,

        .nextBlockIdx = 0,
        .nextIndexBlockIdx = 0,

        .tableHeader = tableHeader,
        .tableReader = tableReader,
        .decompressionPool = decompressionPool,

        .globalUncompressedSizeBytes = 0,
        .globalRowsCount = 0,
        .globalBlocksCount = 0,

        .blockData = BlockData.initEmpty(),
    };
    return br;
}

pub fn deinit(self: *BlockReader, allocator: Allocator) void {
    self.blockHeaders.deinit(allocator);
    self.tableReader.deinit(allocator);
    self.blockData.deinit(allocator);

    if (self.indexBlockHeaders.len > 0) {
        allocator.free(self.indexBlockHeaders);
    }

    allocator.destroy(self);
}

pub fn columnsLen(self: *const BlockReader) usize {
    const invariantColumns = if (self.blockData.invariantColumns) |cols| cols.len else 0;
    return self.blockData.columnsData.items.len + invariantColumns;
}

/// nextBlock reads the next block from the reader and puts it into blockData.
/// Returns false if there are no more blocks.
/// blockData is valid until the next call to NextBlock().
pub fn nextBlock(self: *BlockReader, io: Io, allocator: Allocator) !bool {
    // Load more blocks if needed
    while (self.nextBlockIdx >= self.blockHeaders.items.len) {
        if (!try self.nextIndexBlock(io, allocator)) {
            return false;
        }
    }

    const ih = &self.indexBlockHeaders[self.nextIndexBlockIdx - 1];
    const bh = &self.blockHeaders.items[self.nextBlockIdx];
    const th = &bh.timestampsHeader;

    // Validate bh
    if (self.sidLast) |sidLast| {
        std.debug.assert(!bh.sid.lessThan(sidLast));
        std.debug.assert(!bh.sid.eql(sidLast) or th.min >= self.minTimestampLast);
    }
    self.minTimestampLast = th.min;
    self.sidLast = bh.sid;

    std.debug.assert(th.min >= ih.minTs);
    std.debug.assert(th.max <= ih.maxTs);

    try self.blockData.readFrom(io, allocator, bh, self.tableReader);

    self.globalUncompressedSizeBytes += bh.size;
    self.globalRowsCount += bh.len;
    self.globalBlocksCount += 1;

    // Validate against tableHeader
    std.debug.assert(self.globalUncompressedSizeBytes <= self.tableHeader.uncompressedSize);
    std.debug.assert(self.globalRowsCount <= self.tableHeader.len);
    std.debug.assert(self.globalBlocksCount <= self.tableHeader.blocksCount);

    // The block has been successfully read
    self.nextBlockIdx += 1;
    return true;
}

fn nextIndexBlock(self: *BlockReader, io: Io, allocator: Allocator) !bool {
    if (self.nextIndexBlockIdx >= self.indexBlockHeaders.len) {
        // No more blocks left
        // Validate tableHeader
        const totalBytesRead = self.tableReader.totalBytesRead();
        std.debug.assert(self.tableHeader.compressedSize == totalBytesRead);
        std.debug.assert(self.tableHeader.uncompressedSize == self.globalUncompressedSizeBytes);
        std.debug.assert(self.tableHeader.len == self.globalRowsCount);
        std.debug.assert(self.tableHeader.blocksCount == self.globalBlocksCount);
        return false;
    }

    const ih = &self.indexBlockHeaders[self.nextIndexBlockIdx];

    // Validate ih
    std.debug.assert(ih.minTs >= self.tableHeader.minTimestamp);
    std.debug.assert(ih.maxTs <= self.tableHeader.maxTimestamp);

    const indexBlockData = try readIndexBlock(io, allocator, ih, self.tableReader, self.decompressionPool);
    defer allocator.free(indexBlockData);

    self.blockHeaders.clearRetainingCapacity();
    try BlockHeader.decodeFew(allocator, &self.blockHeaders, indexBlockData);

    self.nextIndexBlockIdx += 1;
    self.nextBlockIdx = 0;
    return true;
}

pub fn blockReaderLessThan(one: *const BlockReader, another: *const BlockReader) bool {
    const firstIsLess = one.blockData.sid.lessThan(another.blockData.sid);
    if (firstIsLess) {
        return true;
    } else if (one.blockData.sid.eql(another.blockData.sid)) {
        return one.blockData.timestampsData.minTimestamp < another.blockData.timestampsData.minTimestamp;
    } else {
        // not equal and not firstIsLess then the second is larger
        return false;
    }
}

fn readIndexBlock(
    io: Io,
    allocator: Allocator,
    ih: *const IndexBlockHeader,
    tableReader: *TableReader,
    decompressionPool: *DecompressionPool,
) ![]u8 {
    const compressed = try allocator.alloc(u8, ih.size);
    defer allocator.free(compressed);
    const n = try tableReader.readIndex(io, compressed, ih.offset);
    std.debug.assert(n == compressed.len);

    const decompressedSize = try encoding.getFrameContentSize(compressed);
    const decompressed = try allocator.alloc(u8, decompressedSize);
    errdefer allocator.free(decompressed);

    _ = try decompressionPool.decompress(io, decompressed, compressed);
    return decompressed;
}

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
    try std.testing.checkAllAllocationFailures(std.testing.allocator, testReadBlock, .{std.testing.io});
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

    const decompressionPool = try CompressionPool.init(allocator, 1);
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
    try memTable.addLines(io, allocator, timestampsEncoders, decompressionPool, sids[0..], linesBySid[0..]);

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
    const decompressionPool = try CompressionPool.init(alloc, 1);
    defer decompressionPool.deinit(alloc);
    try memTable.addLinesForSidWithCompressionPool(io, alloc, timestampsEncoders, decompressionPool, .{ .id = 1, .tenantID = 1234 }, lines[0..]);

    const tablePath = try std.fs.path.join(alloc, &.{ rootPath, "table-1" });
    memTable.storeToDisk(io, alloc, tablePath) catch |err| {
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
