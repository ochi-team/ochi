const std = @import("std");
const Allocator = std.mem.Allocator;
const Io = std.Io;

const encoding = @import("encoding");

const fs = @import("../../fs.zig");
const filenames = @import("../../filenames.zig");

const BlockHeader = @import("BlockHeader.zig");
const TableHeader = @import("TableHeader.zig");
const MetaIndex = @import("MetaIndex.zig");
const MemBlock = @import("MemBlock.zig");
const BlockReader = @import("BlockReader.zig");
const EntriesBlock = @import("EntriesBlock.zig");
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
        .tableHeader = .{},
    };
    return t;
}

pub fn init(io: Io, alloc: Allocator, blocks: []*MemBlock) !*MemTable {
    var readers = try std.ArrayList(*BlockReader).initCapacity(alloc, blocks.len);
    defer {
        for (readers.items) |reader| reader.deinit(alloc);
        readers.deinit(alloc);
    }
    const t = try empty(alloc);
    errdefer t.deinit(alloc);

    if (blocks.len == 1) {
        // nothing to merge
        const b = blocks[0];

        const flushAtUs = Io.Timestamp.now(io, .real).toMicroseconds() + std.time.us_per_s;
        try t.setup(alloc, b, flushAtUs);
        std.debug.assert(t.metaindexBuf.items.len > 0);
        return t;
    }

    for (0..blocks.len) |i| {
        const reader = try BlockReader.initFromMemBlock(alloc, blocks[i]);
        readers.appendAssumeCapacity(reader);
    }

    const flushAtUs = Io.Timestamp.now(io, .real).toMicroseconds() + std.time.us_per_s;
    try t.mergeIntoMemTable(io, alloc, &readers, flushAtUs);
    std.debug.assert(t.metaindexBuf.items.len > 0);
    return t;
}

pub fn mergeMemTables(io: Io, alloc: Allocator, memTables: []*MemTable) !*MemTable {
    var readers = try std.ArrayList(*BlockReader).initCapacity(alloc, memTables.len);
    defer {
        for (readers.items) |r| r.deinit(alloc);
        readers.deinit(alloc);
    }
    for (memTables) |table| {
        const reader = try BlockReader.initFromMemTable(alloc, table);
        readers.appendAssumeCapacity(reader);
    }
    const t = try empty(alloc);
    errdefer t.deinit(alloc);

    const flushToDiskAtUs = flush.getFlushMemTableToDiskDeadline(io, *MemTable, memTables);
    try t.mergeIntoMemTable(io, alloc, &readers, flushToDiskAtUs);
    return t;
}

pub fn deinit(self: *MemTable, alloc: Allocator) void {
    self.entriesBuf.deinit(alloc);
    self.lensBuf.deinit(alloc);
    self.indexBuf.deinit(alloc);
    self.metaindexBuf.deinit(alloc);
    self.tableHeader.deinit(alloc);
    alloc.destroy(self);
}

fn setup(self: *MemTable, alloc: Allocator, block: *MemBlock, flushAtUs: i64) !void {
    block.sortData();
    self.flushAtUs = flushAtUs;
    self.tableHeader.deinit(alloc);

    var entriesBlock = EntriesBlock{};
    defer entriesBlock.deinit(alloc);
    const encodedBlock = try block.encode(alloc, &entriesBlock);
    self.blockHeader.firstEntry = encodedBlock.firstEntry;
    self.blockHeader.prefix = encodedBlock.prefix;
    self.blockHeader.entriesCount = encodedBlock.itemsCount;
    self.blockHeader.encodingType = encodedBlock.encodingType;

    self.tableHeader = .{
        .entriesCount = @intCast(block.memEntries.items.len),
        .blocksCount = 1,
        .firstEntry = block.memEntries.items[0],
        .lastEntry = block.memEntries.items[block.memEntries.items.len - 1],
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
        .firstItem = self.blockHeader.firstEntry,
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

    const firstEntry = self.tableHeader.firstEntry;
    const lastEntry = self.tableHeader.lastEntry;
    self.tableHeader.firstEntry = try alloc.dupe(u8, firstEntry);
    errdefer alloc.free(self.tableHeader.firstEntry);
    self.tableHeader.lastEntry = try alloc.dupe(u8, lastEntry);
    errdefer alloc.free(self.tableHeader.lastEntry);
}

fn mergeIntoMemTable(
    self: *MemTable,
    io: Io,
    alloc: Allocator,
    readers: *std.ArrayList(*BlockReader),
    flushAtUs: i64,
) !void {
    self.flushAtUs = flushAtUs;

    var writer = BlockWriter.initFromMemTable(self);
    defer writer.deinit(alloc);
    self.tableHeader.deinit(alloc);
    self.tableHeader = try mergeBlocks(io, alloc, &writer, readers, null);
}

// TODO: move to a better place, it's not related to mem table
pub fn mergeBlocks(
    io: Io,
    alloc: Allocator,
    writer: *BlockWriter,
    readers: *std.ArrayList(*BlockReader),
    stopped: ?*const std.atomic.Value(bool),
) !TableHeader {
    var merger = try BlockMerger.init(alloc, readers);
    defer merger.deinit(alloc);

    const tableHeader = try merger.merge(io, alloc, writer, stopped);
    try writer.close(io, alloc);

    return tableHeader;
}

pub fn size(self: *MemTable) u64 {
    return @intCast(
        self.entriesBuf.items.len + self.lensBuf.items.len + self.indexBuf.items.len + self.metaindexBuf.items.len,
    );
}

pub fn storeToDisk(self: *MemTable, io: Io, alloc: Allocator, path: []const u8) !void {
    fs.createDirAssert(io, path);

    var fba = std.heap.stackFallback(512, alloc);
    const fbaAlloc = fba.get();

    // TODO: open files in parallel to speed up work on high-latency storages, e.g. Ceph
    const metaindexPath = try std.fs.path.join(fbaAlloc, &.{ path, filenames.metaindex });
    defer fbaAlloc.free(metaindexPath);
    const indexPath = try std.fs.path.join(fbaAlloc, &.{ path, filenames.index });
    defer fbaAlloc.free(indexPath);
    const entriesPath = try std.fs.path.join(fbaAlloc, &.{ path, filenames.entries });
    defer fbaAlloc.free(entriesPath);
    const lensPath = try std.fs.path.join(fbaAlloc, &.{ path, filenames.lens });
    defer fbaAlloc.free(lensPath);

    try fs.writeBufferValToFile(metaindexPath, self.metaindexBuf.items);
    try fs.writeBufferValToFile(indexPath, self.indexBuf.items);
    try fs.writeBufferValToFile(entriesPath, self.entriesBuf.items);
    try fs.writeBufferValToFile(lensPath, self.lensBuf.items);

    try self.tableHeader.writeFile(alloc, path);

    fs.syncPathAndParentDir(io, path);
}
