const std = @import("std");
const Allocator = std.mem.Allocator;

const encoding = @import("encoding");

const fs = @import("../../fs.zig");
const Filenames = @import("../../Filenames.zig");

const BlockHeader = @import("BlockHeader.zig");
const MetaIndex = @import("MetaIndex.zig");
const StorageBlock = @import("StorageBlock.zig");
const MemTable = @import("MemTable.zig");
const MemBlock = @import("MemBlock.zig");

const maxIndexBlockSize = 64 * 1024;

const BlockWriter = @This();

dataBuf: *std.ArrayList(u8),
lensBuf: *std.ArrayList(u8),
indexBuf: *std.ArrayList(u8),
metaindexBuf: *std.ArrayList(u8),

bh: BlockHeader = .{ .firstItem = undefined, .prefix = undefined, .encodingType = undefined },
mi: MetaIndex = .{},

itemsBlockOffset: u64 = 0,
lensBlockOffset: u64 = 0,

sb: StorageBlock = .{},
uncompressedIndexBlockBuf: std.ArrayList(u8) = .empty,
uncompressedMetaindexBuf: std.ArrayList(u8) = .empty,

indexBlockOffset: u64 = 0,

pub fn initFromMemTable(memTable: *MemTable) BlockWriter {
    return .{
        .dataBuf = &memTable.dataBuf,
        .lensBuf = &memTable.lensBuf,
        .indexBuf = &memTable.indexBuf,
        .metaindexBuf = &memTable.metaindexBuf,
    };
}

pub fn initFromDiskTable(alloc: Allocator, path: []const u8, fitsInCache: bool) !BlockWriter {
    // TODO: apply fitsInCache to create a component to write into a file taking OS cache into account
    _ = fitsInCache;

    fs.makeDirAssert(path);

    var fba = std.heap.stackFallback(512, alloc);
    const fbaAlloc = fba.get();

    // TODO: open files in parallel to speed up work on high-latency storages, e.g. Ceph
    const indexPath = try std.fs.path.join(fbaAlloc, &.{ path, Filenames.index });
    defer fbaAlloc.free(indexPath);
    const entriesPath = try std.fs.path.join(fbaAlloc, &.{ path, Filenames.entries });
    defer fbaAlloc.free(entriesPath);
    const lensPath = try std.fs.path.join(fbaAlloc, &.{ path, Filenames.lens });
    defer fbaAlloc.free(lensPath);
    const metaIndexPath = try std.fs.path.join(fbaAlloc, &.{ path, Filenames.metaindex });
    defer fbaAlloc.free(metaIndexPath);

    unreachable;
}

pub fn deinit(self: *BlockWriter, alloc: Allocator) void {
    self.sb.deinit(alloc);
    self.uncompressedIndexBlockBuf.deinit(alloc);
    self.uncompressedMetaindexBuf.deinit(alloc);
}

pub fn writeBlock(self: *BlockWriter, alloc: Allocator, block: *MemBlock) !void {
    const encoded = try block.encode(alloc, &self.sb);
    self.bh.firstItem = encoded.firstItem;
    self.bh.prefix = encoded.prefix;
    self.bh.itemsCount = encoded.itemsCount;
    self.bh.encodingType = encoded.encodingType;

    // Write data
    try self.dataBuf.appendSlice(alloc, self.sb.itemsData.items);
    self.bh.itemsBlockSize = @intCast(self.sb.itemsData.items.len);
    self.bh.itemsBlockOffset = self.itemsBlockOffset;
    self.itemsBlockOffset += self.bh.itemsBlockSize;

    // Write lens
    try self.lensBuf.appendSlice(alloc, self.sb.lensData.items);
    self.bh.lensBlockSize = @intCast(self.sb.lensData.items.len);
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
        self.mi.firstItem = self.bh.firstItem;
    }
    self.bh.reset();
    self.mi.blockHeadersCount += 1;
}

pub fn close(self: *BlockWriter, alloc: Allocator) !void {
    try self.flushIndexData(alloc);

    const bound = try encoding.compressBound(self.uncompressedMetaindexBuf.items.len);
    try self.metaindexBuf.ensureUnusedCapacity(alloc, bound);
    const n = try encoding.compressAuto(
        self.metaindexBuf.unusedCapacitySlice(),
        self.uncompressedMetaindexBuf.items,
    );
    self.metaindexBuf.items.len += n;
}

fn flushIndexData(self: *BlockWriter, alloc: Allocator) !void {
    if (self.uncompressedIndexBlockBuf.items.len == 0) {
        // Nothing to flush.
        return;
    }

    // Write indexBlock
    const bound = try encoding.compressBound(self.uncompressedIndexBlockBuf.items.len);
    try self.indexBuf.ensureUnusedCapacity(alloc, bound);
    const n = try encoding.compressAuto(
        self.indexBuf.unusedCapacitySlice(),
        self.uncompressedIndexBlockBuf.items,
    );
    self.indexBuf.items.len += n;

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
