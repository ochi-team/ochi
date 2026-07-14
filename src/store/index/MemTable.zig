const std = @import("std");
const Allocator = std.mem.Allocator;
const Io = std.Io;

const encoding = @import("encoding");

const Stop = @import("../../stds/Stop.zig");

const fs = @import("../../fs.zig");
const filenames = @import("../../filenames.zig");

const BlockHeader = @import("BlockHeader.zig");
const TableHeader = @import("TableHeader.zig");
const MetaIndex = @import("MetaIndex.zig");
const MemBlock = @import("MemBlock.zig");
const BlockReader = @import("BlockReader.zig");
const Logger = @import("logging");
const EntriesBlock = @import("EntriesBlock.zig");
const BlockWriter = @import("BlockWriter.zig");
const BlockMerger = @import("BlockMerger.zig");
const Table = @import("Table.zig");
const CompressionPool = @import("../compression/CompressionPool.zig");
const DecompressionPool = @import("../compression/DecompressionPool.zig");

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

// TODO: log mem tables buffers size on init
pub fn init(
    io: Io,
    alloc: Allocator,
    blocks: []*MemBlock,
    compressionPool: *CompressionPool,
    decompressionPool: *DecompressionPool,
) !*MemTable {
    var movedBlocks: usize = 0;
    errdefer {
        for (blocks[movedBlocks..]) |block| block.deinit(alloc);
    }

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
        movedBlocks = 1;
        // we don't move it's ownership, so deinit the block here
        defer b.deinit(alloc);

        const flushAtUs = Io.Timestamp.now(io, .real).toMicroseconds() + std.time.us_per_s;
        try t.setup(io, alloc, b, flushAtUs, compressionPool);
        std.debug.assert(t.metaindexBuf.items.len > 0);
        return t;
    }

    for (0..blocks.len) |i| {
        movedBlocks = i + 1;
        const reader = try BlockReader.initFromMovedMemBlock(alloc, blocks[i], decompressionPool);
        readers.appendAssumeCapacity(reader);
    }

    const flushAtUs = Io.Timestamp.now(io, .real).toMicroseconds() + std.time.us_per_s;
    try t.mergeIntoMemTable(io, alloc, &readers, flushAtUs, compressionPool);
    std.debug.assert(t.metaindexBuf.items.len > 0);
    return t;
}

pub fn mergeMemTables(
    io: Io,
    alloc: Allocator,
    memTables: []*Table,
    compressionPool: *CompressionPool,
    decompressionPool: *DecompressionPool,
) !*MemTable {
    var readers = try std.ArrayList(*BlockReader).initCapacity(alloc, memTables.len);
    defer {
        for (readers.items) |r| r.deinit(alloc);
        readers.deinit(alloc);
    }
    for (memTables) |table| {
        const reader = try BlockReader.initFromMemTable(io, alloc, table, decompressionPool);
        readers.appendAssumeCapacity(reader);
    }
    const t = try empty(alloc);
    errdefer t.deinit(alloc);

    const flushToDiskAtUs = flush.getFlushTablesToDiskDeadline(io, *Table, memTables);
    try t.mergeIntoMemTable(io, alloc, &readers, flushToDiskAtUs, compressionPool);
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

fn setup(
    self: *MemTable,
    io: Io,
    alloc: Allocator,
    block: *MemBlock,
    flushAtUs: i64,
    compressionPool: *CompressionPool,
) !void {
    block.sortData();
    self.flushAtUs = flushAtUs;
    self.tableHeader.deinit(alloc);

    var entriesBlock = EntriesBlock{};
    defer entriesBlock.deinit(alloc);
    const encodedBlock = try block.encode(io, alloc, compressionPool, &entriesBlock);
    self.blockHeader.firstEntry = encodedBlock.firstEntry;
    self.blockHeader.prefix = encodedBlock.prefix;
    self.blockHeader.entriesCount = encodedBlock.itemsCount;
    self.blockHeader.encodingType = encodedBlock.encodingType;

    self.tableHeader = .{
        .entriesCount = @intCast(block.memEntries.items.len),
        .blocksCount = 1,
        .firstEntry = block.get(0),
        .lastEntry = block.last(),
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
    try self.indexBuf.ensureUnusedCapacity(alloc, bound);
    var n = try compressionPool.compressAuto(io, self.indexBuf.unusedCapacitySlice(), encodedBlockHeader);
    self.indexBuf.items.len += n;

    const metaIndex = MetaIndex{
        .firstEntry = self.blockHeader.firstEntry,
        .blockHeadersCount = 1,
        .indexBlockOffset = 0,
        .indexBlockSize = @intCast(n),
    };

    var fbaFallback = std.heap.stackFallback(128, alloc);
    var fba = fbaFallback.get();
    const encodedMetaIndex = try metaIndex.encodeAlloc(fba);
    defer fba.free(encodedMetaIndex);

    bound = try encoding.compressBound(encodedMetaIndex.len);
    try self.metaindexBuf.ensureUnusedCapacity(alloc, bound);
    n = try compressionPool.compressAuto(io, self.metaindexBuf.unusedCapacitySlice(), encodedMetaIndex);
    self.metaindexBuf.items.len += n;

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
    compressionPool: *CompressionPool,
) !void {
    self.flushAtUs = flushAtUs;

    var writer = BlockWriter.initFromMemTable(self, compressionPool);
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
    stopped: ?*const Stop,
) !TableHeader {
    defer writer.close(io, alloc) catch |err| {
        Logger.log(.err, "failed to close index block writer", .{ .err = err });
    };

    var merger = try BlockMerger.init(io, alloc, readers);
    defer merger.deinit(alloc);

    const tableHeader = try merger.merge(io, alloc, writer, stopped);
    return tableHeader;
}

pub fn size(self: *MemTable) u64 {
    return @intCast(
        self.entriesBuf.items.len + self.lensBuf.items.len + self.indexBuf.items.len + self.metaindexBuf.items.len,
    );
}

pub fn storeToDisk(self: *MemTable, io: Io, alloc: Allocator, path: []const u8) !void {
    try fs.createDirAssert(io, path);

    // TODO: open files in parallel to speed up work on high-latency storages, e.g. Ceph
    var pathBuf: [std.fs.max_path_bytes]u8 = undefined;
    var pathWriter = std.Io.Writer.fixed(&pathBuf);

    try std.fs.path.fmtJoin(&.{ path, filenames.metaindex }).format(&pathWriter);
    try fs.writeBufferValToFile(io, pathWriter.buffered(), self.metaindexBuf.items);
    pathWriter.end = 0;

    try std.fs.path.fmtJoin(&.{ path, filenames.index }).format(&pathWriter);
    try fs.writeBufferValToFile(io, pathWriter.buffered(), self.indexBuf.items);
    pathWriter.end = 0;

    try std.fs.path.fmtJoin(&.{ path, filenames.entries }).format(&pathWriter);
    try fs.writeBufferValToFile(io, pathWriter.buffered(), self.entriesBuf.items);
    pathWriter.end = 0;

    try std.fs.path.fmtJoin(&.{ path, filenames.lens }).format(&pathWriter);
    try fs.writeBufferValToFile(io, pathWriter.buffered(), self.lensBuf.items);

    try self.tableHeader.writeFile(io, alloc, path);

    try fs.syncPathAndParentDir(io, path);
}
