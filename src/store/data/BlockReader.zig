const std = @import("std");
const Allocator = std.mem.Allocator;
const Io = std.Io;

const encoding = @import("encoding");

const TableReader = @import("TableReader.zig");
const SID = @import("../lines.zig").SID;
const IndexBlockHeader = @import("IndexBlockHeader.zig");
const BlockHeader = @import("BlockHeader.zig");
const TableHeader = @import("TableHeader.zig");
const Table = @import("../data/Table.zig");
const BlockData = @import("BlockData.zig").BlockData;
const DecompressionPool = @import("../compression/DecompressionPool.zig");

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
// backs blockData
arena: std.heap.ArenaAllocator,

pub fn initFromMemTable(
    io: Io,
    alloc: Allocator,
    table: *const Table,
    decompressionPool: *DecompressionPool,
) !*BlockReader {
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
        .arena = .init(alloc),
    };
    return br;
}

pub fn initFromDiskTable(
    io: Io,
    alloc: Allocator,
    table: *const Table,
    decompressionPool: *DecompressionPool,
) !*BlockReader {
    const tableHeader = table.inner.disk.tableHeader;

    const tableReader = try TableReader.init(io, alloc, table, decompressionPool);
    errdefer tableReader.deinit(alloc);
    var indexBlockHeaders: []IndexBlockHeader = &.{};
    if (tableReader.metaIndexBuf.len > 0) {
        indexBlockHeaders = try IndexBlockHeader.readIndexBlockHeaders(
            io,
            alloc,
            decompressionPool,
            tableReader.metaIndexBuf,
        );
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
        .arena = .init(alloc),
    };
    return br;
}

pub fn deinit(self: *BlockReader, allocator: Allocator) void {
    self.blockHeaders.deinit(allocator);
    self.tableReader.deinit(allocator);
    // deinit blockData
    self.arena.deinit();

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
pub fn nextBlock(self: *BlockReader, io: Io, alloc: Allocator) !bool {
    // Load more blocks if needed
    while (self.nextBlockIdx >= self.blockHeaders.items.len) {
        if (!try self.nextIndexBlock(io, alloc)) {
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

    _ = self.arena.reset(.retain_capacity);
    self.blockData.resetArena();
    try self.blockData.readFrom(io, self.arena.allocator(), bh, self.tableReader);

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

fn nextIndexBlock(self: *BlockReader, io: Io, alloc: Allocator) !bool {
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

    const arena = self.arena.allocator();
    const indexBlockData = try readIndexBlock(io, arena, ih, self.tableReader, self.decompressionPool);
    defer arena.free(indexBlockData);

    self.blockHeaders.clearRetainingCapacity();
    try BlockHeader.decodeFew(alloc, &self.blockHeaders, indexBlockData);

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
    alloc: Allocator,
    ih: *const IndexBlockHeader,
    tableReader: *TableReader,
    decompressionPool: *DecompressionPool,
) ![]u8 {
    const compressed = try alloc.alloc(u8, ih.size);
    defer alloc.free(compressed);
    const n = try tableReader.readIndex(io, compressed, ih.offset);
    std.debug.assert(n == compressed.len);

    const decompressedSize = try encoding.getFrameContentSize(compressed);
    const decompressed = try alloc.alloc(u8, decompressedSize);
    errdefer alloc.free(decompressed);

    _ = try decompressionPool.decompress(io, decompressed, compressed);
    return decompressed;
}
