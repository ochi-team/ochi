const std = @import("std");
const Allocator = std.mem.Allocator;
const Io = std.Io;

const encoding = @import("encoding");

const fs = @import("../../fs.zig");
const filenames = @import("../../filenames.zig");

const BlockHeader = @import("BlockHeader.zig");
const MetaIndex = @import("MetaIndex.zig");
const EntriesBlock = @import("EntriesBlock.zig");
const MemTable = @import("MemTable.zig");
const MemBlock = @import("MemBlock.zig");

const maxIndexBlockSize = 64 * 1024;

const MemDestination = struct {
    entriesBuf: *std.ArrayList(u8),
    lensBuf: *std.ArrayList(u8),
    indexBuf: *std.ArrayList(u8),
    metaindexBuf: *std.ArrayList(u8),
};

// TODO: refactor this garbage to work with Wrter interface
const DiskDestination = struct {
    entriesFile: Io.File,
    lensFile: Io.File,
    indexFile: Io.File,
    metaindexFile: Io.File,

    fn sync(self: *DiskDestination) !void {
        try self.entriesFile.sync();
        try self.lensFile.sync();
        try self.indexFile.sync();
        try self.metaindexFile.sync();
    }

    fn close(self: *DiskDestination) void {
        self.entriesFile.close();
        self.lensFile.close();
        self.indexFile.close();
        self.metaindexFile.close();
    }
};

const Destination = union(enum) {
    mem: MemDestination,
    disk: DiskDestination,
};

const BlockWriter = @This();

destination: Destination,

bh: BlockHeader = .{ .firstEntry = "", .prefix = "", .encodingType = .plain },
mi: MetaIndex = .{},

itemsBlockOffset: u64 = 0,
lensBlockOffset: u64 = 0,

entriesBlock: EntriesBlock = .{},
uncompressedIndexBlockBuf: std.ArrayList(u8) = .empty,
uncompressedMetaindexBuf: std.ArrayList(u8) = .empty,
compressedBuf: std.ArrayList(u8) = .empty,

indexBlockOffset: u64 = 0,

pub fn initFromMemTable(memTable: *MemTable) BlockWriter {
    return .{
        .destination = .{
            .mem = .{
                .entriesBuf = &memTable.entriesBuf,
                .lensBuf = &memTable.lensBuf,
                .indexBuf = &memTable.indexBuf,
                .metaindexBuf = &memTable.metaindexBuf,
            },
        },
    };
}

pub fn initFromDiskTable(io: Io, alloc: Allocator, path: []const u8, fitsInCache: bool) !BlockWriter {
    // TODO: apply fitsInCache to create a component to write into a file taking OS cache into account
    _ = fitsInCache;

    fs.createDirAssert(io, path);

    var fba = std.heap.stackFallback(512, alloc);
    const fbaAlloc = fba.get();

    // TODO: open files in parallel to speed up work on high-latency storages, e.g. Ceph
    const indexPath = try std.fs.path.join(fbaAlloc, &.{ path, filenames.index });
    defer fbaAlloc.free(indexPath);
    const entriesPath = try std.fs.path.join(fbaAlloc, &.{ path, filenames.entries });
    defer fbaAlloc.free(entriesPath);
    const lensPath = try std.fs.path.join(fbaAlloc, &.{ path, filenames.lens });
    defer fbaAlloc.free(lensPath);
    const metaIndexPath = try std.fs.path.join(fbaAlloc, &.{ path, filenames.metaindex });
    defer fbaAlloc.free(metaIndexPath);

    var indexFile = try std.fs.createFileAbsolute(indexPath, .{ .truncate = true });
    errdefer indexFile.close();
    var entriesFile = try std.fs.createFileAbsolute(entriesPath, .{ .truncate = true });
    errdefer entriesFile.close();
    var lensFile = try std.fs.createFileAbsolute(lensPath, .{ .truncate = true });
    errdefer lensFile.close();
    var metaindexFile = try std.fs.createFileAbsolute(metaIndexPath, .{ .truncate = true });
    errdefer metaindexFile.close();

    return .{
        .destination = .{
            .disk = .{
                .entriesFile = entriesFile,
                .lensFile = lensFile,
                .indexFile = indexFile,
                .metaindexFile = metaindexFile,
            },
        },
    };
}

pub fn deinit(self: *BlockWriter, alloc: Allocator) void {
    self.entriesBlock.deinit(alloc);
    self.uncompressedIndexBlockBuf.deinit(alloc);
    self.uncompressedMetaindexBuf.deinit(alloc);
    self.compressedBuf.deinit(alloc);
}

pub fn writeBlock(self: *BlockWriter, alloc: Allocator, block: *MemBlock) !void {
    const encoded = try block.encode(alloc, &self.entriesBlock);
    std.debug.assert(block.memEntries.items.len > 0);

    self.bh.firstEntry = encoded.firstEntry;
    self.bh.prefix = encoded.prefix;
    self.bh.entriesCount = encoded.itemsCount;
    self.bh.encodingType = encoded.encodingType;

    // Write data
    try self.writeData(alloc, self.entriesBlock.entriesBuf.items);
    self.bh.entriesBlockSize = @intCast(self.entriesBlock.entriesBuf.items.len);
    self.bh.entriesBlockOffset = self.itemsBlockOffset;
    self.itemsBlockOffset += self.bh.entriesBlockSize;

    // Write lens
    try self.writeLens(alloc, self.entriesBlock.lensBuf.items);
    self.bh.lensBlockSize = @intCast(self.entriesBlock.lensBuf.items.len);
    self.bh.lensBlockOffset = self.lensBlockOffset;
    self.lensBlockOffset += self.bh.lensBlockSize;

    // Write block header
    const bhEncodeBound = self.bh.bound();
    if (self.uncompressedIndexBlockBuf.items.len + bhEncodeBound > maxIndexBlockSize) {
        try self.flushIndexData(alloc);
    }
    try self.uncompressedIndexBlockBuf.ensureUnusedCapacity(alloc, bhEncodeBound);
    self.bh.encode(self.uncompressedIndexBlockBuf.unusedCapacitySlice());
    self.uncompressedIndexBlockBuf.items.len += bhEncodeBound;

    // Write block header
    if (self.mi.firstItem.len == 0) {
        self.mi.firstItem = self.bh.firstEntry;
    }
    self.bh.reset();
    self.mi.blockHeadersCount += 1;
}

pub fn close(self: *BlockWriter, alloc: Allocator) !void {
    try self.flushIndexData(alloc);
    try self.writeMetaindex(alloc);
    switch (self.destination) {
        .mem => {},
        .disk => |*disk| disk.close(),
    }
}

fn flushIndexData(self: *BlockWriter, alloc: Allocator) !void {
    if (self.uncompressedIndexBlockBuf.items.len == 0) {
        // Nothing to flush.
        return;
    }

    // Write indexBlock
    const n = try self.writeIndexBlock(alloc);

    self.mi.indexBlockSize = @intCast(n);
    self.mi.indexBlockOffset = self.indexBlockOffset;
    self.indexBlockOffset += self.mi.indexBlockSize;
    self.uncompressedIndexBlockBuf.clearRetainingCapacity();

    // Write metaindex
    const mrBound = self.mi.bound();
    try self.uncompressedMetaindexBuf.ensureUnusedCapacity(alloc, mrBound);
    self.mi.encode(self.uncompressedMetaindexBuf.unusedCapacitySlice());
    self.uncompressedMetaindexBuf.items.len += mrBound;

    self.mi.reset();
}

fn writeData(self: *BlockWriter, alloc: Allocator, data: []const u8) !void {
    switch (self.destination) {
        .mem => |mem| try mem.entriesBuf.appendSlice(alloc, data),
        .disk => |*disk| try disk.entriesFile.writeAll(data),
    }
}

fn writeLens(self: *BlockWriter, alloc: Allocator, data: []const u8) !void {
    switch (self.destination) {
        .mem => |mem| try mem.lensBuf.appendSlice(alloc, data),
        .disk => |*disk| try disk.lensFile.writeAll(data),
    }
}

fn writeIndexBlock(self: *BlockWriter, alloc: Allocator) !usize {
    switch (self.destination) {
        .mem => |mem| return compressIntoArrayList(alloc, mem.indexBuf, self.uncompressedIndexBlockBuf.items),
        .disk => |*disk| {
            const compressed = try self.compressToScratch(alloc, self.uncompressedIndexBlockBuf.items);
            try disk.indexFile.writeAll(compressed);
            return compressed.len;
        },
    }
}

fn writeMetaindex(self: *BlockWriter, alloc: Allocator) !void {
    switch (self.destination) {
        .mem => |mem| {
            _ = try compressIntoArrayList(alloc, mem.metaindexBuf, self.uncompressedMetaindexBuf.items);
        },
        .disk => |*disk| {
            const compressed = try self.compressToScratch(alloc, self.uncompressedMetaindexBuf.items);
            try disk.metaindexFile.writeAll(compressed);
        },
    }
}

fn compressToScratch(self: *BlockWriter, alloc: Allocator, src: []const u8) ![]const u8 {
    self.compressedBuf.clearRetainingCapacity();
    const n = try compressIntoArrayList(alloc, &self.compressedBuf, src);
    return self.compressedBuf.items[0..n];
}

fn compressIntoArrayList(alloc: Allocator, dst: *std.ArrayList(u8), src: []const u8) !usize {
    const bound = try encoding.compressBound(src.len);
    try dst.ensureUnusedCapacity(alloc, bound);
    const n = try encoding.compressAuto(
        dst.unusedCapacitySlice(),
        src,
    );
    dst.items.len += n;
    return n;
}

const testing = std.testing;

fn createTestMemBlock(alloc: Allocator, items: []const []const u8) !*MemBlock {
    var total: u32 = 0;
    for (items) |item| total += @intCast(item.len);

    var block = try MemBlock.init(alloc, total + 16);
    errdefer block.deinit(alloc);
    for (items) |item| {
        const ok = block.add(item);
        try testing.expect(ok);
    }
    block.sortData();
    return block;
}

fn readTableFile(alloc: Allocator, tablePath: []const u8, fileName: []const u8) ![]u8 {
    const filePath = try std.fs.path.join(alloc, &.{ tablePath, fileName });
    defer alloc.free(filePath);
    return fs.readAll(alloc, filePath);
}

test "BlockWriter disk output matches mem output" {
    const alloc = testing.allocator;
    const io = testing.io;

    const blockOneItems = [_][]const u8{
        "item-a3",
        "item-a1",
        "item-a2",
    };
    const blockTwoItems = [_][]const u8{
        "item-b1-" ++ ("x" ** 160),
        "item-b2-" ++ ("y" ** 160),
        "item-b3-" ++ ("z" ** 160),
    };

    var blockOne = try createTestMemBlock(alloc, &blockOneItems);
    defer blockOne.deinit(alloc);
    var blockTwo = try createTestMemBlock(alloc, &blockTwoItems);
    defer blockTwo.deinit(alloc);

    var memTable = try MemTable.empty(alloc);
    defer memTable.deinit(alloc);
    var memWriter = BlockWriter.initFromMemTable(memTable);
    defer memWriter.deinit(alloc);
    try memWriter.writeBlock(alloc, blockOne);
    try memWriter.writeBlock(alloc, blockTwo);
    try memWriter.close(alloc);

    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    const rootPath = try tmp.dir.realPathFileAlloc(io, alloc, ".");
    defer alloc.free(rootPath);
    const tablePath = try std.fs.path.join(alloc, &.{ rootPath, "table" });
    defer alloc.free(tablePath);

    var diskWriter = try BlockWriter.initFromDiskTable(alloc, tablePath, true);
    defer diskWriter.deinit(alloc);
    try diskWriter.writeBlock(alloc, blockOne);
    try diskWriter.writeBlock(alloc, blockTwo);
    try diskWriter.close(alloc);

    const entries = try readTableFile(alloc, tablePath, filenames.entries);
    defer alloc.free(entries);
    const lens = try readTableFile(alloc, tablePath, filenames.lens);
    defer alloc.free(lens);
    const index = try readTableFile(alloc, tablePath, filenames.index);
    defer alloc.free(index);
    const metaindex = try readTableFile(alloc, tablePath, filenames.metaindex);
    defer alloc.free(metaindex);

    try testing.expectEqualSlices(u8, memTable.entriesBuf.items, entries);
    try testing.expectEqualSlices(u8, memTable.lensBuf.items, lens);
    try testing.expectEqualSlices(u8, memTable.indexBuf.items, index);
    try testing.expectEqualSlices(u8, memTable.metaindexBuf.items, metaindex);
}

test "BlockWriter metaindexBuf may contain multiple records" {
    const alloc = testing.allocator;
    const blocksCount: usize = 1400;

    var memTable = try MemTable.empty(alloc);
    defer memTable.deinit(alloc);

    var writer = BlockWriter.initFromMemTable(memTable);
    defer writer.deinit(alloc);

    var itemsOwned = try std.ArrayList([]u8).initCapacity(alloc, blocksCount);
    defer {
        for (itemsOwned.items) |item| alloc.free(item);
        itemsOwned.deinit(alloc);
    }

    var blocks = try std.ArrayList(*MemBlock).initCapacity(alloc, blocksCount);
    defer {
        for (blocks.items) |block| block.deinit(alloc);
        blocks.deinit(alloc);
    }

    // Simulate a compaction/merge path calling writeBlock() many times.
    for (0..blocksCount) |i| {
        const item = try std.fmt.allocPrint(alloc, "merge-item-{d:0>6}", .{i});
        errdefer alloc.free(item);
        try itemsOwned.append(alloc, item);

        const items = [_][]const u8{item};
        const block = try createTestMemBlock(alloc, &items);
        errdefer block.deinit(alloc);
        try blocks.append(alloc, block);

        try writer.writeBlock(alloc, block);
    }
    try writer.close(alloc);

    const decoded = try MetaIndex.decodeDecompress(
        alloc,
        memTable.metaindexBuf.items,
        @intCast(blocksCount),
    );
    defer {
        for (decoded.records) |*rec| rec.deinit(alloc);
        if (decoded.records.len > 0) alloc.free(decoded.records);
    }

    try testing.expect(decoded.records.len > 1);

    var totalBlockHeaders: u64 = 0;
    for (decoded.records) |rec| {
        try testing.expect(rec.blockHeadersCount > 0);
        totalBlockHeaders += rec.blockHeadersCount;
    }
    try testing.expectEqual(@as(u64, @intCast(blocksCount)), totalBlockHeaders);
}
