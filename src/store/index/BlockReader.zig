const std = @import("std");
const Allocator = std.mem.Allocator;
const Io = std.Io;
const builtin = @import("builtin");

const encoding = @import("encoding");

const MemBlock = @import("MemBlock.zig");
const Table = @import("Table.zig");
const MetaIndex = @import("MetaIndex.zig");
const EntriesBlock = @import("EntriesBlock.zig");
const TableHeader = @import("TableHeader.zig");
const BlockHeader = @import("BlockHeader.zig");
const Logger = @import("logging");
const DecompressionPool = @import("../compression/DecompressionPool.zig");

const BlockReader = @This();

block: *MemBlock,
// TODO: no idea yet how to avoid it
ownsStorage: bool = false,
tableHeader: TableHeader,
table: *const Table,
decompressionPool: *DecompressionPool,

// state

// currentI defines a current item of the block
currentI: usize,
// read defines if the block has been read
isRead: bool,

// metaindex state
metaIndexI: usize = 0,
// metaindex passed on init
metaIndexRecords: []MetaIndex = &.{},
// compressed buf
compressedBuf: std.ArrayList(u8) = .empty,
// uncompressed buf
uncompressedBuf: std.ArrayList(u8) = .empty,

// current block header index
blockHeaderI: usize = 0,
// all block headers read from the buffers
blockHeaders: []BlockHeader = &.{},
// current block header
// SAFETY: it's a state and it's not initialized by default,
// it relies on the correct order of the API calls
blockHeader: *BlockHeader = undefined,
// current storage block
entriesBlock: EntriesBlock = .{},
// number of blocks read
blocksRead: usize = 0,
// number of items read
itemsRead: usize = 0,

firstItemChecked: bool = false,

/// Takes ownership of block.
pub fn initFromMovedMemBlock(alloc: Allocator, block: *MemBlock, decompressionPool: *DecompressionPool) !*BlockReader {
    errdefer block.deinit(alloc);
    std.debug.assert(block.memEntries.items.len > 0);
    block.sortData();

    Logger.log(.debug, "index.BlockReader: open mem block", .{ .prefix = block.prefix });
    const r = try alloc.create(BlockReader);
    r.* = .{
        .block = block,
        .ownsStorage = false,
        .tableHeader = .{},
        .currentI = 0,
        .isRead = false,
        .table = undefined,
        .decompressionPool = decompressionPool,
    };
    return r;
}

pub fn initFromMemTable(io: Io, alloc: Allocator, table: *const Table, decompressionPool: *DecompressionPool) !*BlockReader {
    const memTable = table.inner.mem;
    std.debug.assert(memTable.tableHeader.blocksCount > 0);
    const metaIndexRecords = try MetaIndex.decodeDecompress(
        io,
        alloc,
        decompressionPool,
        memTable.metaindexBuf.items,
        memTable.tableHeader.blocksCount,
    );
    errdefer alloc.free(metaIndexRecords.records);

    Logger.log(.debug, "index.BlockReader: open mem table", .{
        .metaIndexRecordsLen = metaIndexRecords.records.len,
        .metaIndexRecordsSize = metaIndexRecords.compressedSize,
    });

    const block = try MemBlock.init(alloc, .{
        .blocksCountHint = @intCast(memTable.tableHeader.entriesCount),
    });
    errdefer block.deinit(alloc);

    const r = try alloc.create(BlockReader);
    r.* = .{
        .block = block,
        .ownsStorage = false,
        .metaIndexRecords = metaIndexRecords.records,
        .tableHeader = memTable.tableHeader,
        .table = table,
        .decompressionPool = decompressionPool,
        .currentI = 0,
        .isRead = false,
    };

    std.debug.assert(r.tableHeader.blocksCount != 0);
    std.debug.assert(r.tableHeader.entriesCount != 0);
    return r;
}

pub fn initFromDiskTable(
    io: Io,
    alloc: Allocator,
    table: *const Table,
    decompressionPool: *DecompressionPool,
) !*BlockReader {
    const path = table.path;
    const tableHeader = try TableHeader.readFile(io, alloc, path);
    errdefer tableHeader.deinit(alloc);

    const metaIndex = try MetaIndex.readFile(io, alloc, decompressionPool, path, tableHeader.blocksCount);
    errdefer {
        for (metaIndex.records) |*index| {
            index.deinit(alloc);
        }
        alloc.free(metaIndex.records);
    }

    Logger.log(.debug, "index.BlockReader: open disk table", .{ .path = table.path });

    const block = try MemBlock.init(alloc, .{
        .blocksCountHint = @intCast(tableHeader.entriesCount),
    });
    errdefer block.deinit(alloc);

    const r = try alloc.create(BlockReader);
    errdefer alloc.destroy(r);
    r.* = .{
        .block = block,
        .ownsStorage = true,
        .metaIndexRecords = metaIndex.records,
        .tableHeader = tableHeader,
        .table = table,
        .decompressionPool = decompressionPool,
        .currentI = 0,
        .isRead = false,
    };

    std.debug.assert(r.tableHeader.blocksCount != 0);
    std.debug.assert(r.tableHeader.entriesCount != 0);
    return r;
}

pub fn deinit(self: *BlockReader, alloc: Allocator) void {
    self.block.deinit(alloc);

    if (self.ownsStorage) self.tableHeader.deinit(alloc);

    for (self.metaIndexRecords) |*rec| rec.deinit(alloc);
    if (self.metaIndexRecords.len > 0) alloc.free(self.metaIndexRecords);
    if (self.blockHeaders.len > 0) alloc.free(self.blockHeaders);
    self.entriesBlock.deinit(alloc);
    self.compressedBuf.deinit(alloc);
    self.uncompressedBuf.deinit(alloc);

    alloc.destroy(self);
}

pub fn blockReaderLessThan(one: *BlockReader, another: *BlockReader) bool {
    const first = one.current();
    const second = another.current();
    return std.mem.lessThan(u8, first, second);
}

pub fn current(self: *BlockReader) []const u8 {
    return self.block.get(self.currentI);
}

pub fn next(self: *BlockReader, io: Io, alloc: Allocator) !bool {
    if (self.isRead) return false;

    // TODO: perhaps it's worth adding read mode enum to show
    // we either read from mem block or decoding from mem table
    if (self.metaIndexRecords.len == 0) {
        self.isRead = true;
        return true;
    }

    if (self.blockHeaders.len == 0 or self.blockHeaderI >= self.blockHeaders.len) {
        const ok = try self.readNextBlockHeaders(io, alloc);
        if (!ok) {
            const lastItem = self.block.last();
            std.debug.assert(std.mem.eql(u8, self.tableHeader.lastEntry, lastItem));
            self.isRead = true;
            return ok;
        }
    }

    self.blockHeader = &self.blockHeaders[self.blockHeaderI];
    self.blockHeaderI += 1;

    // TODO: for chunked buffer find a way just to  transfer a chunk ownership, perhaps via std.mem.swap,
    // for a file reader we must just read the content
    self.entriesBlock.entriesBuf.clearRetainingCapacity();
    try self.entriesBlock.entriesBuf.ensureUnusedCapacity(alloc, self.blockHeader.entriesBlockSize);
    const itemsDest = self.entriesBlock.entriesBuf.unusedCapacitySlice()[0..self.blockHeader.entriesBlockSize];
    const itemsSize: usize = @intCast(self.blockHeader.entriesBlockSize);
    const itemsLen = try self.readEntries(io, itemsDest, self.blockHeader.entriesBlockOffset);
    if (itemsLen != itemsSize) {
        return error.InvalidEntriesBlockRange;
    }
    self.entriesBlock.entriesBuf.items.len = self.blockHeader.entriesBlockSize;

    self.entriesBlock.lensBuf.clearRetainingCapacity();
    try self.entriesBlock.lensBuf.ensureUnusedCapacity(alloc, self.blockHeader.lensBlockSize);
    const lensDest = self.entriesBlock.lensBuf.unusedCapacitySlice()[0..self.blockHeader.lensBlockSize];
    const lensSize: usize = @intCast(self.blockHeader.lensBlockSize);
    const lensLen = try self.readLens(io, lensDest, self.blockHeader.lensBlockOffset);
    if (lensLen != lensSize) {
        return error.InvalidLensBlockRange;
    }
    self.entriesBlock.lensBuf.items.len = self.blockHeader.lensBlockSize;

    try self.block.decode(
        io,
        alloc,
        self.decompressionPool,
        &self.entriesBlock,
        self.blockHeader.firstEntry,
        self.blockHeader.prefix,
        self.blockHeader.entriesCount,
        self.blockHeader.encodingType,
    );
    self.blocksRead += 1;
    std.debug.assert(self.blocksRead <= self.tableHeader.blocksCount);
    self.currentI = 0;
    self.itemsRead += self.block.memEntries.items.len;
    std.debug.assert(self.itemsRead <= self.tableHeader.entriesCount);

    if (builtin.is_test and !self.firstItemChecked) {
        self.firstItemChecked = true;
        const firstEntry = self.block.get(0);
        std.debug.assert(std.mem.eql(u8, self.tableHeader.firstEntry, firstEntry));
    }
    return true;
}

fn readNextBlockHeaders(self: *BlockReader, io: Io, alloc: Allocator) !bool {
    if (self.metaIndexI >= self.metaIndexRecords.len) {
        return false;
    }

    const currentMetaIndexI = self.metaIndexI;
    const mi = &self.metaIndexRecords[currentMetaIndexI];
    self.metaIndexI += 1;

    self.compressedBuf.clearRetainingCapacity();
    try self.compressedBuf.ensureUnusedCapacity(alloc, mi.indexBlockSize);

    const indexDest = self.compressedBuf.unusedCapacitySlice()[0..mi.indexBlockSize];
    const indexSize: usize = @intCast(mi.indexBlockSize);
    const indexLen = try self.readIndex(io, indexDest, mi.indexBlockOffset);
    if (indexLen != indexSize) {
        self.logInvalidIndexRange(currentMetaIndexI, mi, indexSize, indexLen, "readNextBlockHeaders");
        return error.InvalidIndexBlockRange;
    }
    self.compressedBuf.items.len = mi.indexBlockSize;

    self.uncompressedBuf.clearRetainingCapacity();
    const uncompressedSize = try encoding.getFrameContentSize(self.compressedBuf.items);
    try self.uncompressedBuf.ensureUnusedCapacity(alloc, uncompressedSize);
    const bufOffset = try self.decompressionPool.decompress(
        io,
        self.uncompressedBuf.unusedCapacitySlice(),
        self.compressedBuf.items,
    );
    self.uncompressedBuf.items.len = bufOffset;

    if (self.blockHeaders.len > 0) alloc.free(self.blockHeaders);
    self.blockHeaders = try BlockHeader.decodeMany(alloc, self.uncompressedBuf.items, mi.blockHeadersCount);
    self.blockHeaderI = 0;
    return true;
}

fn readEntries(self: *const BlockReader, io: Io, buf: []u8, offset: u64) !usize {
    return self.table.readEntries(io, buf, offset);
}

fn readLens(self: *const BlockReader, io: Io, buf: []u8, offset: u64) !usize {
    return self.table.readLens(io, buf, offset);
}

fn readIndex(self: *const BlockReader, io: Io, buf: []u8, offset: u64) !usize {
    return self.table.readIndex(io, buf, offset);
}

// TODO: better to append data to diagnostic and log on the upper level
fn logInvalidIndexRange(
    self: *const BlockReader,
    metaIndexI: usize,
    mi: *const MetaIndex,
    expectedSize: usize,
    actualSize: usize,
    stage: []const u8,
) void {
    Logger.log(.err, "invalid index block range", .{
        .stage = stage,
        .metaindexI = metaIndexI,
        .indexOffset = mi.indexBlockOffset,
        .expectedSize = expectedSize,
        .actualSize = actualSize,
        .tableBlocks = self.tableHeader.blocksCount,
        .tableEntries = self.tableHeader.entriesCount,
        .miBlockHeaders = mi.blockHeadersCount,
    });

    if (metaIndexI > 0) {
        const prev = self.metaIndexRecords[metaIndexI - 1];
        Logger.log(.err, "invalid index block previous metaindex", .{
            .metaindexI = metaIndexI - 1,
            .offset = prev.indexBlockOffset,
            .size = prev.indexBlockSize,
            .headers = prev.blockHeadersCount,
        });
    }

    if (metaIndexI + 1 < self.metaIndexRecords.len) {
        const nextMi = self.metaIndexRecords[metaIndexI + 1];
        Logger.log(.err, "invalid index block next metaindex", .{
            .metaindexI = metaIndexI + 1,
            .offset = nextMi.indexBlockOffset,
            .size = nextMi.indexBlockSize,
            .headers = nextMi.blockHeadersCount,
        });
    }
}

test {
    _ = @import("BlockReader_test.zig");
}
