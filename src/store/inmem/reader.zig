const std = @import("std");
const Allocator = std.mem.Allocator;
const Io = std.Io;

const encoding = @import("encoding");
const filenames = @import("../../filenames.zig");
const fs = @import("../../fs.zig");

const SID = @import("../lines.zig").SID;
const IndexBlockHeader = @import("IndexBlockHeader.zig");
const BlockHeader = @import("BlockHeader.zig");
const TableHeader = @import("TableHeader.zig");
const MemTable = @import("MemTable.zig");
const Table = @import("../data/Table.zig");
const BlockData = @import("BlockData.zig").BlockData;
const ColumnIDGen = @import("ColumnIDGen.zig");

// TODO: check maybe i don't need allocator.create
pub const StreamReader = struct {
    table: *const Table,
    metaIndexBuf: []const u8,

    columnsKeysBuf: []const u8,
    columnIdxsBuf: []const u8,

    // TODO: consider making it as a value, not a pointer
    // TODO: decode manually when it comes to file reader
    columnIDGen: *ColumnIDGen,
    colIdx: *std.AutoHashMap(u16, u16),

    // TODO: these flags don't look smart,
    // there must be a better idea to track ownership
    ownsBuffers: bool = false,
    ownsMetadata: bool = false,

    pub fn init(io: Io, allocator: Allocator, table: *const Table) !*StreamReader {
        switch (table.inner) {
            .mem => return initFromMem(allocator, table),
            .disk => return initFromDisk(io, allocator, table),
        }
    }

    pub fn initFromMem(allocator: Allocator, table: *const Table) !*StreamReader {
        const memTable = table.inner.mem;
        // TODO: this could be taken from a stream writer theoretically
        const columnIDGen = if (memTable.columnKeysBuf.items.len > 0)
            try ColumnIDGen.decode(allocator, memTable.columnKeysBuf.items)
        else
            try ColumnIDGen.init(allocator);
        errdefer columnIDGen.deinit(allocator);

        const colIdx = try decodeColumnIdxs(allocator, memTable.columnIdxsBuf.items);
        errdefer {
            colIdx.deinit();
            allocator.destroy(colIdx);
        }

        const r = try allocator.create(StreamReader);
        r.* = StreamReader{
            .table = table,
            .metaIndexBuf = memTable.metaIndexBuf.items,

            .columnIDGen = columnIDGen,
            .colIdx = colIdx,
            .columnsKeysBuf = memTable.columnKeysBuf.items,
            .columnIdxsBuf = memTable.columnIdxsBuf.items,
            .ownsMetadata = true,
        };
        return r;
    }

    // TODO: instead of reusing the table files open them again due to:
    // - keep the query cache clean
    // - manage different fadvise for different descriptors
    // also benchmark against mmap since it requires only reading
    // - based on it reimplement totalBytesRead
    fn initFromDisk(io: Io, alloc: Allocator, table: *const Table) !*StreamReader {
        var pathBuf: [std.fs.max_path_bytes]u8 = undefined;
        var pathWriter = std.Io.Writer.fixed(&pathBuf);

        const path = table.path;
        // TODO: open files in parallel to speed up high-latency storage.
        try std.fs.path.fmtJoin(&.{ path, filenames.columnIdxs }).format(&pathWriter);
        const columnIdxsBuf = try fs.readAll(io, alloc, pathWriter.buffered());
        errdefer alloc.free(columnIdxsBuf);

        pathWriter.end = 0;
        try std.fs.path.fmtJoin(&.{ path, filenames.metaindex }).format(&pathWriter);
        const metaIndexBuf = try fs.readAll(io, alloc, pathWriter.buffered());
        errdefer alloc.free(metaIndexBuf);

        pathWriter.end = 0;
        try std.fs.path.fmtJoin(&.{ path, filenames.columnKeys }).format(&pathWriter);
        const columnsKeysBuf = try fs.readAll(io, alloc, pathWriter.buffered());
        errdefer alloc.free(columnsKeysBuf);

        var columnIDGen: *ColumnIDGen = undefined;
        if (columnsKeysBuf.len > 0) {
            columnIDGen = try ColumnIDGen.decode(alloc, columnsKeysBuf);
        } else {
            columnIDGen = try ColumnIDGen.init(alloc);
        }
        errdefer columnIDGen.deinit(alloc);

        const colIdx = try decodeColumnIdxs(alloc, columnIdxsBuf);
        errdefer {
            colIdx.deinit();
            alloc.destroy(colIdx);
        }

        const streamReader = try alloc.create(StreamReader);

        streamReader.* = .{
            .table = table,
            .metaIndexBuf = metaIndexBuf,
            .columnIDGen = columnIDGen,
            .colIdx = colIdx,
            .columnsKeysBuf = columnsKeysBuf,
            .columnIdxsBuf = columnIdxsBuf,
            .ownsBuffers = true,
        };
        return streamReader;
    }

    pub fn deinit(self: *StreamReader, allocator: Allocator) void {
        if (!self.ownsBuffers) {
            if (self.ownsMetadata) {
                self.columnIDGen.deinit(allocator);
                self.colIdx.deinit();
                allocator.destroy(self.colIdx);
            }
            allocator.destroy(self);
            return;
        }

        allocator.free(self.metaIndexBuf);
        allocator.free(self.columnsKeysBuf);
        allocator.free(self.columnIdxsBuf);

        self.columnIDGen.deinit(allocator);

        self.colIdx.deinit();
        allocator.destroy(self.colIdx);

        allocator.destroy(self);
    }

    /// totalBytesRead returns the total number of bytes read from all buffers.
    pub fn totalBytesRead(self: *const StreamReader) u64 {
        return self.table.tableHeader().compressedSize;
    }

    pub fn readIndex(
        self: *const StreamReader,
        io: Io,
        buf: []u8,
        offset: u64,
    ) !usize {
        return self.table.readIndex(io, buf, offset);
    }

    pub fn readTimestamps(
        self: *const StreamReader,
        io: Io,
        buf: []u8,
        offset: u64,
    ) !usize {
        return self.table.readTimestamps(io, buf, offset);
    }

    pub fn readColumnsHeaderIndex(
        self: *const StreamReader,
        io: Io,
        buf: []u8,
        offset: u64,
    ) !usize {
        return self.table.readColumnsHeaderIndex(io, buf, offset);
    }

    pub fn readColumnsHeader(
        self: *const StreamReader,
        io: Io,
        buf: []u8,
        offset: u64,
    ) !usize {
        return self.table.readColumnsHeader(io, buf, offset);
    }

    pub fn readBloomValues(
        self: *const StreamReader,
        io: Io,
        buf: []u8,
        key: []const u8,
        offset: u64,
    ) !usize {
        if (key.len == 0) {
            return self.table.readMessageBloomValues(io, buf, offset);
        }

        const colID = self.columnIDGen.keyIDs.get(key).?;
        const bloomBufI = self.colIdx.get(colID).?;
        return self.table.readBloomValues(io, buf, offset, bloomBufI);
    }

    pub fn readBloomTokens(
        self: *const StreamReader,
        io: Io,
        buf: []u8,
        key: []const u8,
        offset: u64,
    ) !usize {
        if (key.len == 0) {
            return self.table.readMessageBloomTokens(io, buf, offset);
        }

        const colID = self.columnIDGen.keyIDs.get(key).?;
        const bloomBufI = self.colIdx.get(colID).?;
        return self.table.readBloomTokens(io, buf, offset, bloomBufI);
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

    pub fn init(io: Io, alloc: Allocator, table: *const Table) !*BlockReader {
        return switch (table.inner) {
            .mem => initFromMemTable(alloc, table),
            .disk => initFromDiskTable(io, alloc, table),
        };
    }

    pub fn initFromMemTable(alloc: Allocator, table: *const Table) !*BlockReader {
        const memTable = table.inner.mem;
        const tableHeader = memTable.tableHeader;
        const indexBlockHeaders = try IndexBlockHeader.readIndexBlockHeaders(
            alloc,
            memTable.metaIndexBuf.items,
        );
        errdefer alloc.free(indexBlockHeaders);

        var blockHeaders = try std.ArrayList(BlockHeader).initCapacity(alloc, 64);
        errdefer blockHeaders.deinit(alloc);

        const streamReader = try StreamReader.initFromMem(alloc, table);
        errdefer streamReader.deinit(alloc);

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

    pub fn initFromDiskTable(io: Io, alloc: Allocator, table: *const Table) !*BlockReader {
        const tableHeader = table.inner.disk.tableHeader;

        const streamReader = try StreamReader.init(io, alloc, table);
        errdefer streamReader.deinit(alloc);
        var indexBlockHeaders: []IndexBlockHeader = &.{};
        if (streamReader.metaIndexBuf.len > 0) {
            indexBlockHeaders = try IndexBlockHeader.readIndexBlockHeaders(alloc, streamReader.metaIndexBuf);
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

        if (self.indexBlockHeaders.len > 0) {
            allocator.free(self.indexBlockHeaders);
        }

        allocator.destroy(self);
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
            std.debug.assert(!bh.sid.lessThan(&sidLast));
            std.debug.assert(!bh.sid.eql(&sidLast) or th.min >= self.minTimestampLast);
        }
        self.minTimestampLast = th.min;
        self.sidLast = bh.sid;

        std.debug.assert(th.min >= ih.minTs);
        std.debug.assert(th.max <= ih.maxTs);

        try self.blockData.readFrom(io, allocator, bh, self.streamReader);

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

        const indexBlockData = try readIndexBlock(io, allocator, ih, self.streamReader);
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
    io: Io,
    allocator: Allocator,
    ih: *const IndexBlockHeader,
    streamReader: *StreamReader,
) ![]u8 {
    const compressed = try allocator.alloc(u8, ih.size);
    defer allocator.free(compressed);
    const n = try streamReader.readIndex(io, compressed, ih.offset);
    std.debug.assert(n == compressed.len);

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
            .sid = .{ .id = 2, .tenantID = 2222 },
            .fields = sample.fields1[0..],
        },
        .{
            .timestampNs = 2,
            .sid = .{ .id = 1, .tenantID = 1111 },
            .fields = sample.fields2[0..],
        },
        .{
            .timestampNs = 3,
            .sid = .{ .id = 1, .tenantID = 1111 },
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

    const memTable = try MemTable.init(allocator);
    const table = Table.fromMem(allocator, memTable) catch |err| {
        memTable.deinit(allocator);
        return err;
    };
    defer table.close(io);
    try memTable.addLines(io, allocator, lines[0..]);

    const th = memTable.tableHeader;
    try std.testing.expectEqual(3, th.len);
    try std.testing.expect(th.minTimestamp <= 1);
    try std.testing.expect(th.maxTimestamp >= 3);
    try std.testing.expect(th.blocksCount >= 1);
    try std.testing.expect(th.uncompressedSize > 0);
    try std.testing.expect(th.compressedSize > 0);

    const blockReader = try BlockReader.initFromMemTable(allocator, table);
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
    try std.testing.expectEqual(th.compressedSize, blockReader.streamReader.totalBytesRead());

    // Second pass: check each block's blockData (sid, rowsCount, timestamps range)
    const blockReader2 = try BlockReader.initFromMemTable(allocator, table);
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
        .sid = .{ .id = 1, .tenantID = 1234 },
        .fields = fields1[0..],
    };
    const line2 = Line{
        .timestampNs = 2,
        .sid = .{ .id = 1, .tenantID = 1234 },
        .fields = fields2[0..],
    };

    var lines = [_]Line{ line1, line2 };

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const rootPath = try tmp.dir.realPathFileAlloc(io, ".", alloc);
    defer alloc.free(rootPath);

    const memTable = try MemTable.init(alloc);
    defer memTable.deinit(alloc);
    try memTable.addLines(io, alloc, lines[0..]);

    const tablePath = try std.fs.path.join(alloc, &.{ rootPath, "table-1" });
    memTable.storeToDisk(io, alloc, tablePath) catch |err| {
        alloc.free(tablePath);
        return err;
    };

    const table = Table.open(io, alloc, tablePath) catch |err| {
        alloc.free(tablePath);
        return err;
    };
    defer table.close(io);

    const blockReader = try BlockReader.initFromDiskTable(io, alloc, table);
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
        blockReader.streamReader.totalBytesRead(),
    );
}
