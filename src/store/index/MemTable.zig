const std = @import("std");
const Allocator = std.mem.Allocator;

const encoding = @import("encoding");

const BlockHeader = @import("BlockHeader.zig");
const TableHeader = @import("TableHeader.zig");
const MetaIndex = @import("MetaIndex.zig");
const MemBlock = @import("MemBlock.zig");
const BlockReader = @import("BlockReader.zig");
const StorageBlock = @import("StorageBlock.zig");
const BlockWriter = @import("BlockWriter.zig");
const BlockMerger = @import("BlockMerger.zig");

const MemTable = @This();

const flush = @import("flush/flush.zig");

blockHeader: BlockHeader,
tableHeader: TableHeader,
metaIndex: MetaIndex,

dataBuf: std.ArrayList(u8) = .empty,
lensBuf: std.ArrayList(u8) = .empty,
indexBuf: std.ArrayList(u8) = .empty,

metaindexBuf: std.ArrayList(u8) = .empty,

flushAtUs: ?i64 = null,

pub fn empty(alloc: Allocator) !*MemTable {
    const t = try alloc.create(MemTable);
    t.* = .{
        .blockHeader = undefined,
        .tableHeader = undefined,
        .metaIndex = undefined,
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

    const flushToDiskAtUs = flush.getFlushToDiskDeadline(memTables);
    try t.mergeIntoMemTable(alloc, &readers, flushToDiskAtUs);
    return t;
}

pub fn deinit(self: *MemTable, alloc: Allocator) void {
    self.dataBuf.deinit(alloc);
    self.lensBuf.deinit(alloc);
    self.indexBuf.deinit(alloc);
    self.metaindexBuf.deinit(alloc);
    alloc.destroy(self);
}

fn setup(self: *MemTable, alloc: Allocator, block: *MemBlock, flushAtUs: i64) !void {
    block.sortData();
    self.flushAtUs = flushAtUs;

    var sb = StorageBlock{};
    defer sb.deinit(alloc);
    const encodedBlock = try block.encode(alloc, &sb);
    self.blockHeader.firstItem = encodedBlock.firstItem;
    self.blockHeader.prefix = encodedBlock.prefix;
    self.blockHeader.itemsCount = encodedBlock.itemsCount;
    self.blockHeader.encodingType = encodedBlock.encodingType;

    self.tableHeader = .{
        .itemsCount = @intCast(block.items.items.len),
        .blocksCount = 1,
        .firstItem = block.items.items[0],
        .lastItem = block.items.items[block.items.items.len - 1],
    };

    try self.dataBuf.appendSlice(alloc, sb.itemsData.items);
    self.blockHeader.itemsBlockOffset = 0;
    self.blockHeader.itemsBlockSize = @intCast(sb.itemsData.items.len);

    try self.lensBuf.appendSlice(alloc, sb.lensData.items);
    self.blockHeader.lensBlockOffset = 0;
    self.blockHeader.lensBlockSize = @intCast(sb.lensData.items.len);

    const encodedBlockHeader = try self.blockHeader.encodeAlloc(alloc);
    defer alloc.free(encodedBlockHeader);

    var bound = try encoding.compressBound(encodedBlockHeader.len);
    const compressed = try alloc.alloc(u8, bound);
    defer alloc.free(compressed);
    var n = try encoding.compressAuto(compressed, encodedBlockHeader);
    try self.indexBuf.appendSlice(alloc, compressed[0..n]);

    self.metaIndex.firstItem = self.blockHeader.firstItem;
    self.metaIndex.blockHeadersCount = 1;
    self.metaIndex.indexBlockOffset = 0;
    self.metaIndex.indexBlockSize = @intCast(n);

    var fbaFallback = std.heap.stackFallback(128, alloc);
    var fba = fbaFallback.get();
    const encodedMetaIndex = try self.metaIndex.encodeAlloc(fba);
    defer fba.free(encodedMetaIndex);

    bound = try encoding.compressBound(encodedMetaIndex.len);
    const compressedMr = try alloc.alloc(u8, bound);
    defer alloc.free(compressedMr);
    n = try encoding.compressAuto(compressedMr, encodedMetaIndex);

    try self.metaindexBuf.appendSlice(alloc, compressedMr[0..n]);
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
    self.tableHeader = try mergeBlocks(alloc, "", &writer, readers, null);
}

pub fn mergeBlocks(
    alloc: Allocator,
    tablePath: []const u8,
    writer: *BlockWriter,
    readers: *std.ArrayList(*BlockReader),
    stopped: ?*const std.atomic.Value(bool),
) !TableHeader {
    var merger = try BlockMerger.init(alloc, readers);
    defer merger.deinit(alloc);

    const tableHeader = try merger.merge(alloc, writer, stopped);
    try writer.close(alloc);

    if (tablePath.len != 0) {
        var fbaFallback = std.heap.stackFallback(512, alloc);
        const fba = fbaFallback.get();
        try tableHeader.writeFile(fba, tablePath);
    }

    return tableHeader;
}

pub fn size(self: *MemTable) u64 {
    return @intCast(
        self.dataBuf.items.len + self.lensBuf.items.len + self.indexBuf.items.len + self.metaindexBuf.items.len,
    );
}
