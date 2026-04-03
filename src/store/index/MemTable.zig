const std = @import("std");
const Allocator = std.mem.Allocator;

const encoding = @import("encoding");

const fs = @import("../../fs.zig");
const Filenames = @import("../../Filenames.zig");

const BlockHeader = @import("BlockHeader.zig");
const TableHeader = @import("TableHeader.zig");
const MetaIndex = @import("MetaIndex.zig");
const MemBlock = @import("MemBlock.zig");
const BlockReader = @import("BlockReader.zig");
const EntriesBlock = @import("EntrieBlock.zig");
const BlockWriter = @import("BlockWriter.zig");
const BlockMerger = @import("BlockMerger.zig");

const MemTable = @This();

const flush = @import("../table/flush.zig");

blockHeader: BlockHeader,
tableHeader: TableHeader,

entriesBuf: std.ArrayList(u8) = .empty,
lensBuf: std.ArrayList(u8) = .empty,
indexBuf: std.ArrayList(u8) = .empty,

metaindexBuf: std.ArrayList(u8) = .empty,

flushAtUs: i64 = std.math.maxInt(i64),

pub fn empty(alloc: Allocator) !*MemTable {
    const t = try alloc.create(MemTable);
    t.* = .{
        .blockHeader = undefined,
        .tableHeader = undefined,
    };
    return t;
}

pub fn init(alloc: Allocator, blocks: []*MemBlock) !*MemTable {
    var readers = try std.ArrayList(*BlockReader).initCapacity(alloc, blocks.len);
    defer {
        for (readers.items) |reader| reader.deinit(alloc);
        readers.deinit(alloc);
    }
    const t = try empty(alloc);
    errdefer alloc.destroy(t);

    if (blocks.len == 1) {
        // nothing to merge
        const b = blocks[0];

        const flushAtUs = std.time.microTimestamp() + std.time.us_per_s;
        try t.setup(alloc, b, flushAtUs);
        return t;
    }

    for (0..blocks.len) |i| {
        const reader = try BlockReader.initFromMemBlock(alloc, blocks[i]);
        readers.appendAssumeCapacity(reader);
    }

    const flushAtUs = std.time.microTimestamp() + std.time.us_per_s;
    try t.mergeIntoMemTable(alloc, &readers, flushAtUs);
    return t;
}

pub fn mergeMemTables(alloc: Allocator, memTables: []*MemTable) !*MemTable {
    var readers = try std.ArrayList(*BlockReader).initCapacity(alloc, memTables.len);
    errdefer {
        for (readers.items) |r| r.deinit(alloc);
        readers.deinit(alloc);
    }
    for (memTables) |table| {
        const reader = try BlockReader.initFromMemTable(alloc, table);
        readers.appendAssumeCapacity(reader);
    }
    const t = try empty(alloc);
    errdefer alloc.destroy(t);

    const flushToDiskAtUs = flush.getFlushMemTableToDiskDeadline(*MemTable, memTables);
    try t.mergeIntoMemTable(alloc, &readers, flushToDiskAtUs);
    return t;
}

pub fn deinit(self: *MemTable, alloc: Allocator) void {
    self.entriesBuf.deinit(alloc);
    self.lensBuf.deinit(alloc);
    self.indexBuf.deinit(alloc);
    self.metaindexBuf.deinit(alloc);
    alloc.destroy(self);
}

fn setup(self: *MemTable, alloc: Allocator, block: *MemBlock, flushAtUs: i64) !void {
    block.sortData();
    self.flushAtUs = flushAtUs;

    var entriesBlock = EntriesBlock{};
    defer entriesBlock.deinit(alloc);
    const encodedBlock = try block.encode(alloc, &entriesBlock);
    self.blockHeader.firstItem = encodedBlock.firstItem;
    self.blockHeader.prefix = encodedBlock.prefix;
    self.blockHeader.entriesCount = encodedBlock.itemsCount;
    self.blockHeader.encodingType = encodedBlock.encodingType;

    self.tableHeader = .{
        .itemsCount = @intCast(block.items.items.len),
        .blocksCount = 1,
        .firstItem = block.items.items[0],
        .lastItem = block.items.items[block.items.items.len - 1],
    };

    try self.entriesBuf.appendSlice(alloc, entriesBlock.entriesBuf.items);
    self.blockHeader.entriesBlockOffset = 0;
    self.blockHeader.entriesBlockSize = @intCast(entriesBlock.entriesBuf.items.len);

    try self.lensBuf.appendSlice(alloc, entriesBlock.lensBuf.items);
    self.blockHeader.lensBlockOffset = 0;
    self.blockHeader.lensBlockSize = @intCast(entriesBlock.lensBuf.items.len);

    const encodedBlockHeader = try self.blockHeader.encodeAlloc(alloc);
    defer alloc.free(encodedBlockHeader);

    var bound = try encoding.compressBound(encodedBlockHeader.len);
    const compressed = try alloc.alloc(u8, bound);
    defer alloc.free(compressed);
    var n = try encoding.compressAuto(compressed, encodedBlockHeader);
    try self.indexBuf.appendSlice(alloc, compressed[0..n]);

    const metaIndex = MetaIndex{
        .firstItem = self.blockHeader.firstItem,
        .blockHeadersCount = 1,
        .indexBlockOffset = 0,
        .indexBlockSize = @intCast(n),
    };

    var fbaFallback = std.heap.stackFallback(128, alloc);
    var fba = fbaFallback.get();
    const encodedMetaIndex = try metaIndex.encodeAlloc(fba);
    defer fba.free(encodedMetaIndex);

    bound = try encoding.compressBound(encodedMetaIndex.len);
    const compressedMetaIndex = try alloc.alloc(u8, bound);
    defer alloc.free(compressedMetaIndex);
    n = try encoding.compressAuto(compressedMetaIndex, encodedMetaIndex);

    try self.metaindexBuf.appendSlice(alloc, compressedMetaIndex[0..n]);
}

fn mergeIntoMemTable(
    self: *MemTable,
    alloc: Allocator,
    readers: *std.ArrayList(*BlockReader),
    flushAtUs: i64,
) !void {
    self.flushAtUs = flushAtUs;

    var writer = BlockWriter.initFromMemTable(self);
    defer writer.deinit(alloc);
    self.tableHeader = try mergeBlocks(alloc, &writer, readers, null);
}

// TODO: move to a better place, it's not related to mem table
pub fn mergeBlocks(
    alloc: Allocator,
    writer: *BlockWriter,
    readers: *std.ArrayList(*BlockReader),
    stopped: ?*const std.atomic.Value(bool),
) !TableHeader {
    var merger = try BlockMerger.init(alloc, readers);
    defer merger.deinit(alloc);

    const tableHeader = try merger.merge(alloc, writer, stopped);
    try writer.close(alloc);

    return tableHeader;
}

pub fn size(self: *MemTable) u64 {
    return @intCast(
        self.entriesBuf.items.len + self.lensBuf.items.len + self.indexBuf.items.len + self.metaindexBuf.items.len,
    );
}

pub fn storeToDisk(self: *MemTable, alloc: Allocator, path: []const u8) !void {
    fs.makeDirAssert(path);

    var fba = std.heap.stackFallback(512, alloc);
    const fbaAlloc = fba.get();

    // TODO: open files in parallel to speed up work on high-latency storages, e.g. Ceph
    const metaindexPath = try std.fs.path.join(fbaAlloc, &.{ path, Filenames.metaindex });
    defer fbaAlloc.free(metaindexPath);
    const indexPath = try std.fs.path.join(fbaAlloc, &.{ path, Filenames.index });
    defer fbaAlloc.free(indexPath);
    const entriesPath = try std.fs.path.join(fbaAlloc, &.{ path, Filenames.entries });
    defer fbaAlloc.free(entriesPath);
    const lensPath = try std.fs.path.join(fbaAlloc, &.{ path, Filenames.lens });
    defer fbaAlloc.free(lensPath);

    try fs.writeBufferValToFile(metaindexPath, self.metaindexBuf.items);
    try fs.writeBufferValToFile(indexPath, self.indexBuf.items);
    try fs.writeBufferValToFile(entriesPath, self.entriesBuf.items);
    try fs.writeBufferValToFile(lensPath, self.lensBuf.items);

    try self.tableHeader.writeFile(alloc, path);

    fs.syncPathAndParentDir(path);
}
