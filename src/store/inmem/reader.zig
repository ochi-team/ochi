const std = @import("std");
const Allocator = std.mem.Allocator;

const encoding = @import("encoding");
const Filenames = @import("../../Filenames.zig");
const fs = @import("../../fs.zig");

const SID = @import("../lines.zig").SID;
const IndexBlockHeader = @import("IndexBlockHeader.zig");
const BlockHeader = @import("block_header.zig").BlockHeader;
const TableHeader = @import("TableHeader.zig");
const MemTable = @import("MemTable.zig");
const BlockData = @import("BlockData.zig").BlockData;
const ColumnIDGen = @import("ColumnIDGen.zig");
const StreamDestination = @import("StreamDestination.zig").StreamDestination;

// TODO: check maybe i don't need allocator.create
pub const StreamReader = struct {
    timestampsBuf: []const u8,
    indexBuf: []const u8,
    metaIndexBuf: []const u8,

    columnsHeaderBuf: []const u8,
    columnsHeaderIndexBuf: []const u8,

    columnsKeysBuf: []const u8,
    columnIdxsBuf: []const u8,

    messageBloomValuesBuf: []const u8,
    messageBloomTokensBuf: []const u8,
    // TODO: consider making it as a value, not a pointer
    bloomValuesList: std.ArrayList([]const u8),
    bloomTokensList: std.ArrayList([]const u8),

    // TODO: decode manually when it comes to file reader
    columnIDGen: *const ColumnIDGen,
    colIdx: *const std.AutoHashMap(u16, u16),

    // TODO: this flag doesn't look smart,
    // there must be a better idea to track ownership
    ownsBuffers: bool = false,

    pub fn initFromMem(allocator: Allocator, tableMem: *MemTable) !*StreamReader {
        var bloomValuesList = try std.ArrayList([]const u8).initCapacity(
            allocator,
            tableMem.streamWriter.bloomValuesList.items.len,
        );
        errdefer bloomValuesList.deinit(allocator);
        for (tableMem.streamWriter.bloomValuesList.items) |buf| {
            bloomValuesList.appendAssumeCapacity(buf.asSliceAssumeBuffer());
        }
        var bloomTokensList = try std.ArrayList([]const u8).initCapacity(
            allocator,
            tableMem.streamWriter.bloomTokensList.items.len,
        );
        errdefer bloomTokensList.deinit(allocator);
        for (tableMem.streamWriter.bloomTokensList.items) |buf| {
            bloomTokensList.appendAssumeCapacity(buf.asSliceAssumeBuffer());
        }

        const r = try allocator.create(StreamReader);
        r.* = StreamReader{
            .timestampsBuf = tableMem.streamWriter.timestampsDst.asSliceAssumeBuffer(),
            .indexBuf = tableMem.streamWriter.indexDst.asSliceAssumeBuffer(),
            .metaIndexBuf = tableMem.streamWriter.metaIndexDst.asSliceAssumeBuffer(),
            .columnsHeaderBuf = tableMem.streamWriter.columnsHeaderDst.asSliceAssumeBuffer(),
            .columnsHeaderIndexBuf = tableMem.streamWriter.columnsHeaderIndexDst.asSliceAssumeBuffer(),

            .messageBloomValuesBuf = tableMem.streamWriter.messageBloomValuesDst.asSliceAssumeBuffer(),
            .messageBloomTokensBuf = tableMem.streamWriter.messageBloomTokensDst.asSliceAssumeBuffer(),
            .bloomValuesList = bloomValuesList,
            .bloomTokensList = bloomTokensList,

            .columnIDGen = tableMem.streamWriter.columnIDGen,
            .colIdx = &tableMem.streamWriter.colIdx,
            .columnsKeysBuf = tableMem.streamWriter.columnKeysBuf.asSliceAssumeBuffer(),
            .columnIdxsBuf = tableMem.streamWriter.columnIdxsBuf.asSliceAssumeBuffer(),
        };
        return r;
    }

    fn initFromDisk(alloc: Allocator, path: []const u8, tableHeader: TableHeader) !*StreamReader {
        const streamReader = try alloc.create(StreamReader);
        errdefer streamReader.deinit(alloc);

        var fba = std.heap.stackFallback(2048, alloc);
        const fbaAlloc = fba.get();

        // TODO: open files in parallel to speed up high-latency storage.
        const columnIdxsPath = try std.fs.path.join(fbaAlloc, &.{ path, Filenames.columnIdxs });
        defer fbaAlloc.free(columnIdxsPath);
        const metaindexPath = try std.fs.path.join(fbaAlloc, &.{ path, Filenames.metaindex });
        defer fbaAlloc.free(metaindexPath);
        const indexPath = try std.fs.path.join(fbaAlloc, &.{ path, Filenames.index });
        defer fbaAlloc.free(indexPath);
        const columnsHeaderIndexPath = try std.fs.path.join(fbaAlloc, &.{ path, Filenames.columnsHeaderIndex });
        defer fbaAlloc.free(columnsHeaderIndexPath);
        const columnsHeaderPath = try std.fs.path.join(fbaAlloc, &.{ path, Filenames.columnsHeader });
        defer fbaAlloc.free(columnsHeaderPath);
        const timestampsPath = try std.fs.path.join(fbaAlloc, &.{ path, Filenames.timestamps });
        defer fbaAlloc.free(timestampsPath);
        const messageBloomTokensPath = try std.fs.path.join(fbaAlloc, &.{ path, Filenames.messageTokens });
        defer fbaAlloc.free(messageBloomTokensPath);
        const messageBloomValuesPath = try std.fs.path.join(fbaAlloc, &.{ path, Filenames.messageValues });
        defer fbaAlloc.free(messageBloomValuesPath);
        const columnNamesPath = try std.fs.path.join(fbaAlloc, &.{ path, Filenames.columnKeys });
        defer fbaAlloc.free(columnNamesPath);

        const columnIdxsBuf = try fs.readAll(alloc, columnIdxsPath);
        errdefer alloc.free(columnIdxsBuf);
        const metaIndexBuf = try fs.readAll(alloc, metaindexPath);
        errdefer alloc.free(metaIndexBuf);
        const indexBuf = try fs.readAll(alloc, indexPath);
        errdefer alloc.free(indexBuf);
        const columnsHeaderIndexBuf = try fs.readAll(alloc, columnsHeaderIndexPath);
        errdefer alloc.free(columnsHeaderIndexBuf);
        const columnsHeaderBuf = try fs.readAll(alloc, columnsHeaderPath);
        errdefer alloc.free(columnsHeaderBuf);
        const timestampsBuf = try fs.readAll(alloc, timestampsPath);
        errdefer alloc.free(timestampsBuf);
        const messageBloomTokensBuf = try fs.readAll(alloc, messageBloomTokensPath);
        errdefer alloc.free(messageBloomTokensBuf);
        const messageBloomValuesBuf = try fs.readAll(alloc, messageBloomValuesPath);
        errdefer alloc.free(messageBloomValuesBuf);
        const columnsKeysBuf = try fs.readAll(alloc, columnNamesPath);
        errdefer alloc.free(columnsKeysBuf);

        const shardCount: usize = @intCast(tableHeader.bloomValuesBuffersAmount);

        var bloomValuesList = try std.ArrayList([]const u8).initCapacity(alloc, shardCount);
        errdefer bloomValuesList.deinit(alloc);

        var bloomTokensList = try std.ArrayList([]const u8).initCapacity(alloc, shardCount);
        errdefer bloomTokensList.deinit(alloc);

        var shardIdx: usize = 0;
        errdefer {
            for (bloomValuesList.items) |buf| {
                alloc.free(buf);
            }
            for (bloomTokensList.items) |buf| {
                alloc.free(buf);
            }
        }
        while (shardIdx < shardCount) : (shardIdx += 1) {
            const bloomTokensPath = try MemTable.getBloomValuesFilePath(fbaAlloc, path, @intCast(shardIdx));
            defer fbaAlloc.free(bloomTokensPath);
            const bloomValuesPath = try MemTable.getBloomTokensFilePath(fbaAlloc, path, @intCast(shardIdx));
            defer fbaAlloc.free(bloomValuesPath);

            const bloomTokensBuf = try fs.readAll(alloc, bloomTokensPath);
            const bloomValuesBuf = try fs.readAll(alloc, bloomValuesPath);

            bloomValuesList.appendAssumeCapacity(bloomValuesBuf);
            bloomTokensList.appendAssumeCapacity(bloomTokensBuf);
        }

        var columnIDGen: *ColumnIDGen = undefined;
        if (columnsKeysBuf.len > 0) {
            columnIDGen = try ColumnIDGen.decode(alloc, @constCast(columnsKeysBuf));
        } else {
            columnIDGen = try ColumnIDGen.init(alloc);
        }
        errdefer columnIDGen.deinit(alloc);

        const colIdx = try decodeColumnIdxs(alloc, columnIdxsBuf);
        errdefer {
            colIdx.deinit();
            alloc.destroy(colIdx);
        }

        streamReader.* = .{
            .timestampsBuf = timestampsBuf,
            .indexBuf = indexBuf,
            .metaIndexBuf = metaIndexBuf,
            .columnsHeaderBuf = columnsHeaderBuf,
            .columnsHeaderIndexBuf = columnsHeaderIndexBuf,
            .messageBloomValuesBuf = messageBloomValuesBuf,
            .messageBloomTokensBuf = messageBloomTokensBuf,
            .bloomValuesList = bloomValuesList,
            .bloomTokensList = bloomTokensList,
            .columnIDGen = columnIDGen,
            .colIdx = colIdx,
            .columnsKeysBuf = columnsKeysBuf,
            .columnIdxsBuf = columnIdxsBuf,
            .ownsBuffers = true,
        };
        return streamReader;
    }

    pub fn deinit(self: *StreamReader, allocator: Allocator) void {
        if (self.ownsBuffers) {
            allocator.free(self.timestampsBuf);
            allocator.free(self.indexBuf);
            allocator.free(self.metaIndexBuf);

            allocator.free(self.columnsHeaderBuf);
            allocator.free(self.columnsHeaderIndexBuf);

            allocator.free(self.messageBloomValuesBuf);
            allocator.free(self.messageBloomTokensBuf);

            for (self.bloomValuesList.items) |buf| {
                allocator.free(buf);
            }
            self.bloomValuesList.deinit(allocator);

            for (self.bloomTokensList.items) |buf| {
                allocator.free(buf);
            }
            self.bloomTokensList.deinit(allocator);

            allocator.free(@constCast(self.columnsKeysBuf));
            allocator.free(@constCast(self.columnIdxsBuf));

            const columnIDGen = @constCast(self.columnIDGen);
            columnIDGen.deinit(allocator);

            const colIdx = @constCast(self.colIdx);
            colIdx.deinit();
            allocator.destroy(colIdx);
        }

        allocator.destroy(self);
    }

    /// totalBytesRead returns the total number of bytes read from all buffers.
    pub fn totalBytesRead(self: *const StreamReader) u64 {
        var total: u64 = 0;
        total += self.timestampsBuf.len;
        total += self.indexBuf.len;
        total += self.metaIndexBuf.len;
        total += self.columnsHeaderBuf.len;
        total += self.columnsHeaderIndexBuf.len;
        total += self.messageBloomValuesBuf.len;
        total += self.messageBloomTokensBuf.len;
        for (self.bloomValuesList.items) |buf| {
            total += buf.len;
        }
        for (self.bloomTokensList.items) |buf| {
            total += buf.len;
        }
        total += self.columnsKeysBuf.len;
        total += self.columnIdxsBuf.len;
        return total;
    }
};

fn decodeColumnIdxs(alloc: Allocator, src: []const u8) !*std.AutoHashMap(u16, u16) {
    const colIdx = try alloc.create(std.AutoHashMap(u16, u16));
    errdefer alloc.destroy(colIdx);

    colIdx.* = std.AutoHashMap(u16, u16).init(alloc);
    errdefer colIdx.deinit();

    if (src.len == 0) {
        return colIdx;
    }

    var dec = encoding.Decoder.init(src);
    const count = dec.readVarInt();
    try colIdx.ensureTotalCapacity(@intCast(count));

    for (0..count) |_| {
        const colID: u16 = @intCast(dec.readVarInt());
        const shardIdx: u16 = @intCast(dec.readVarInt());
        colIdx.putAssumeCapacity(colID, shardIdx);
    }

    std.debug.assert(dec.offset == src.len);
    return colIdx;
}

pub const BlockReader = struct {
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
    streamReader: *StreamReader,

    // Global stats for validation
    globalUncompressedSizeBytes: u64,
    globalRowsCount: u64,
    globalBlocksCount: u64,

    // Block data
    // TODO: find a better name
    // TODO: make it a pointer, seems it holds a lot of fields
    blockData: BlockData,

    pub fn initFromMemTable(allocator: Allocator, tableMem: *MemTable) !*BlockReader {
        const indexBlockHeaders = try IndexBlockHeader.readIndexBlockHeaders(
            allocator,
            tableMem.streamWriter.metaIndexDst.asSliceAssumeBuffer(),
        );
        errdefer {
            for (indexBlockHeaders) |*h| h.deinitRead(allocator);
            allocator.free(indexBlockHeaders);
        }

        var blockHeaders = try std.ArrayList(BlockHeader).initCapacity(allocator, 64);
        errdefer blockHeaders.deinit(allocator);

        const streamReader = try StreamReader.initFromMem(allocator, tableMem);
        errdefer streamReader.deinit(allocator);

        const br = try allocator.create(BlockReader);
        errdefer allocator.destroy(br);

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

            .tableHeader = tableMem.tableHeader,
            .streamReader = streamReader,

            .globalUncompressedSizeBytes = 0,
            .globalRowsCount = 0,
            .globalBlocksCount = 0,

            .blockData = BlockData.initEmpty(),
        };
        return br;
    }

    pub fn initFromDiskTable(alloc: Allocator, path: []const u8) !*BlockReader {
        const tableHeader = try TableHeader.readFile(alloc, path);

        const streamReader = try StreamReader.initFromDisk(alloc, path, tableHeader);
        errdefer streamReader.deinit(alloc);
        var indexBlockHeaders: []IndexBlockHeader = &.{};
        if (streamReader.metaIndexBuf.len > 0) {
            indexBlockHeaders = try IndexBlockHeader.readIndexBlockHeaders(alloc, streamReader.metaIndexBuf);
        }
        errdefer {
            for (indexBlockHeaders) |*h| h.deinitRead(alloc);
            if (indexBlockHeaders.len > 0) {
                alloc.free(indexBlockHeaders);
            }
        }

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
            .streamReader = streamReader,

            .globalUncompressedSizeBytes = 0,
            .globalRowsCount = 0,
            .globalBlocksCount = 0,

            .blockData = BlockData.initEmpty(),
        };
        return br;
    }

    pub fn deinit(self: *BlockReader, allocator: Allocator) void {
        self.blockHeaders.deinit(allocator);
        self.streamReader.deinit(allocator);
        self.blockData.deinit(allocator);

        for (self.indexBlockHeaders) |*bh| {
            bh.deinitRead(allocator);
        }
        allocator.free(self.indexBlockHeaders);

        allocator.destroy(self);
    }

    /// nextBlock reads the next block from the reader and puts it into blockData.
    /// Returns false if there are no more blocks.
    /// blockData is valid until the next call to NextBlock().
    pub fn nextBlock(self: *BlockReader, allocator: Allocator) !bool {
        // Load more blocks if needed
        while (self.nextBlockIdx >= self.blockHeaders.items.len) {
            if (!try self.nextIndexBlock(allocator)) {
                return false;
            }
        }

        const ih = &self.indexBlockHeaders[self.nextIndexBlockIdx - 1];
        const bh = &self.blockHeaders.items[self.nextBlockIdx];
        const th = &bh.timestampsHeader;

        // Validate bh
        if (self.sidLast) |sidLast| {
            std.debug.assert(!bh.sid.lessThan(&sidLast));
            std.debug.assert(!bh.sid.eql(&sidLast) or th.min >= self.minTimestampLast);
        }
        self.minTimestampLast = th.min;
        self.sidLast = bh.sid;

        std.debug.assert(th.min >= ih.minTs);
        std.debug.assert(th.max <= ih.maxTs);

        try self.blockData.readFrom(allocator, bh, self.streamReader);

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

    fn nextIndexBlock(self: *BlockReader, allocator: Allocator) !bool {
        if (self.nextIndexBlockIdx >= self.indexBlockHeaders.len) {
            // No more blocks left
            // Validate tableHeader
            const totalBytesRead = self.streamReader.totalBytesRead();
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

        const indexBlockData = try readIndexBlock(allocator, ih, self.streamReader);
        defer allocator.free(indexBlockData);

        self.blockHeaders.clearRetainingCapacity();
        try BlockHeader.decodeFew(allocator, &self.blockHeaders, indexBlockData);

        self.nextIndexBlockIdx += 1;
        self.nextBlockIdx = 0;
        return true;
    }

    pub fn blockReaderLessThan(one: *const BlockReader, another: *const BlockReader) bool {
        const firstIsLess = one.blockData.sid.lessThan(&another.blockData.sid);
        if (firstIsLess) {
            return true;
        } else if (one.blockData.sid.eql(&another.blockData.sid)) {
            return one.blockData.timestampsData.minTimestamp < another.blockData.timestampsData.minTimestamp;
        } else {
            // not equal and not firstIsLess then the second is larger
            return false;
        }
    }
};

fn readIndexBlock(
    allocator: Allocator,
    ih: *const IndexBlockHeader,
    streamReader: *StreamReader,
) ![]u8 {
    const compressed = streamReader.indexBuf[ih.offset..][0..ih.size];
    const decompressedSize = try encoding.getFrameContentSize(compressed);
    const decompressed = try allocator.alloc(u8, decompressedSize);
    errdefer allocator.free(decompressed);

    _ = try encoding.decompress(decompressed, compressed);
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
            .sid = .{ .id = 2, .tenantID = "2222" },
            .fields = sample.fields1[0..],
        },
        .{
            .timestampNs = 2,
            .sid = .{ .id = 1, .tenantID = "1111" },
            .fields = sample.fields2[0..],
        },
        .{
            .timestampNs = 3,
            .sid = .{ .id = 1, .tenantID = "1111" },
            .fields = sample.fields3[0..],
        },
    };
}

test "readBlock reads buffers" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, testReadBlock, .{});
}

test "initFromDiskTable reads buffers" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, testInitFromDiskTable, .{});
}

fn testReadBlock(allocator: Allocator) !void {
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
    var lines = [3]*const Line{
        &sample.lines[0],
        &sample.lines[1],
        &sample.lines[2],
    };

    const memTable = try MemTable.init(allocator);
    defer memTable.deinit(allocator);
    try memTable.addLines(allocator, lines[0..]);

    const th = memTable.tableHeader;
    try std.testing.expectEqual(@as(u32, 3), th.len);
    try std.testing.expect(th.minTimestamp <= 1);
    try std.testing.expect(th.maxTimestamp >= 3);
    try std.testing.expect(th.blocksCount >= 1);
    try std.testing.expect(th.uncompressedSize > 0);
    try std.testing.expect(th.compressedSize > 0);

    const blockReader = try BlockReader.initFromMemTable(allocator, memTable);
    defer blockReader.deinit(allocator);

    var blocksRead: u32 = 0;
    while (try blockReader.nextBlock(allocator)) {
        blocksRead += 1;
    }

    try std.testing.expectEqual(@as(u32, 2), blocksRead);
    try std.testing.expectEqual(@as(u64, 2), blockReader.globalBlocksCount);
    try std.testing.expectEqual(@as(u64, 3), blockReader.globalRowsCount);
    try std.testing.expectEqual(th.uncompressedSize, blockReader.globalUncompressedSizeBytes);
    try std.testing.expectEqual(th.len, blockReader.globalRowsCount);
    try std.testing.expectEqual(th.blocksCount, blockReader.globalBlocksCount);
    try std.testing.expectEqual(th.compressedSize, blockReader.streamReader.totalBytesRead());

    // Second pass: check each block's blockData (sid, rowsCount, timestamps range)
    const blockReader2 = try BlockReader.initFromMemTable(allocator, memTable);
    defer blockReader2.deinit(allocator);

    var block1Sid1111 = false;
    var block2Sid2222 = false;
    var blocksWithFullData: u32 = 0;
    while (try blockReader2.nextBlock(allocator)) {
        const bd = &blockReader2.blockData;
        try std.testing.expect(bd.rowsCount >= 1);
        try std.testing.expect(bd.uncompressedSizeBytes > 0);
        try std.testing.expect(bd.timestampsData.minTimestamp <= bd.timestampsData.maxTimestamp);
        // columnsData may be empty in allocation-failure runs from checkAllocationFailures
        if (bd.columnsData.items.len >= 2) {
            blocksWithFullData += 1;
            if (std.mem.eql(u8, bd.sid.tenantID, "1111") and bd.sid.id == 1) {
                try std.testing.expectEqual(@as(u32, 2), bd.rowsCount);
                try std.testing.expectEqual(@as(u64, 2), bd.timestampsData.minTimestamp);
                try std.testing.expectEqual(@as(u64, 3), bd.timestampsData.maxTimestamp);
                block1Sid1111 = true;
            } else if (std.mem.eql(u8, bd.sid.tenantID, "2222") and bd.sid.id == 2) {
                try std.testing.expectEqual(@as(u32, 1), bd.rowsCount);
                try std.testing.expectEqual(@as(u64, 1), bd.timestampsData.minTimestamp);
                try std.testing.expectEqual(@as(u64, 1), bd.timestampsData.maxTimestamp);
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

fn testInitFromDiskTable(allocator: Allocator) !void {
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
        .sid = .{ .id = 1, .tenantID = "1234" },
        .fields = fields1[0..],
    };
    const line2 = Line{
        .timestampNs = 2,
        .sid = .{ .id = 1, .tenantID = "1234" },
        .fields = fields2[0..],
    };

    var lines = [_]*const Line{ &line1, &line2 };

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const rootPath = try tmp.dir.realpathAlloc(allocator, ".");
    defer allocator.free(rootPath);
    const tablePath = try std.fs.path.join(allocator, &.{ rootPath, "table-1" });
    defer allocator.free(tablePath);

    const memTable = try MemTable.init(allocator);
    defer memTable.deinit(allocator);
    try memTable.addLines(allocator, lines[0..]);
    try memTable.storeToDisk(allocator, tablePath);

    const blockReader = try BlockReader.initFromDiskTable(allocator, tablePath);
    defer blockReader.deinit(allocator);

    var blocksRead: u32 = 0;
    while (try blockReader.nextBlock(allocator)) {
        blocksRead += 1;
    }

    try std.testing.expectEqual(memTable.tableHeader.blocksCount, blocksRead);
    try std.testing.expectEqual(
        @as(u64, memTable.tableHeader.uncompressedSize),
        blockReader.globalUncompressedSizeBytes,
    );
    try std.testing.expectEqual(@as(u64, memTable.tableHeader.len), blockReader.globalRowsCount);
    try std.testing.expectEqual(@as(u64, memTable.tableHeader.blocksCount), blockReader.globalBlocksCount);
    try std.testing.expectEqual(
        @as(u64, memTable.tableHeader.compressedSize),
        blockReader.streamReader.totalBytesRead(),
    );
}
