const std = @import("std");
const Allocator = std.mem.Allocator;
const builtin = @import("builtin");

const encoding = @import("encoding");

const Filenames = @import("../../Filenames.zig");
const fs = @import("../../fs.zig");

const MemBlock = @import("MemBlock.zig");
const MemTable = @import("MemTable.zig");
const MetaIndex = @import("MetaIndex.zig");
const StorageBlock = @import("StorageBlock.zig");
const TableHeader = @import("TableHeader.zig");
const BlockHeader = @import("BlockHeader.zig");

const BlockReader = @This();

// TODO: make it non optional, it was historically optional
// to show mem block is not owned on decoding it from mem table,
// but apparently it's owning
block: ?*MemBlock,
// TODO: so far it's used for validation purpose only,
// it might be useful to expose it as metrics,
// so eventually we can include it to fully observable build and make void otherwise;
// theoretically it also could be used to adjust zstd compression level
tableHeader: TableHeader,

// TODO: these buffers could be files, on zig 0.16 implement a reader API,
// this change will require implementing a proper close method
indexBuf: std.ArrayList(u8),
dataBuf: std.ArrayList(u8),
lensBuf: std.ArrayList(u8),

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
// TODO: perhaps remove it in order not to hold the pointer
blockHeader: *BlockHeader = undefined,
// current storage block
sb: StorageBlock = .{},
// number of blocks read
blocksRead: usize = 0,
// number of items read
itemsRead: usize = 0,

firstItemChecked: if (builtin.is_test) bool else void = if (builtin.is_test) false else {},

pub fn initFromMemBlock(alloc: Allocator, block: *MemBlock) !*BlockReader {
    std.debug.assert(block.items.items.len > 0);
    block.sortData();

    const r = try alloc.create(BlockReader);
    r.* = .{
        .block = block,
        .tableHeader = undefined,
        .currentI = 0,
        .isRead = false,
        .indexBuf = .empty,
        .dataBuf = .empty,
        .lensBuf = .empty,
    };
    return r;
}

pub fn initFromMemTable(alloc: Allocator, memTable: *MemTable) !*BlockReader {
    std.debug.assert(memTable.tableHeader.blocksCount > 0);
    const metaIndexRecords = try MetaIndex.decodeDecompress(
        alloc,
        memTable.metaindexBuf.items,
        memTable.tableHeader.blocksCount,
    );
    errdefer alloc.free(metaIndexRecords.records);

    const block = try MemBlock.init(alloc, @intCast(memTable.tableHeader.itemsCount));
    errdefer block.deinit(alloc);

    const r = try alloc.create(BlockReader);
    r.* = .{
        .block = block,
        .metaIndexRecords = metaIndexRecords.records,
        .tableHeader = memTable.tableHeader,
        .indexBuf = memTable.indexBuf,
        .dataBuf = memTable.dataBuf,
        .lensBuf = memTable.lensBuf,
        .currentI = 0,
        .isRead = false,
    };

    std.debug.assert(r.tableHeader.blocksCount != 0);
    std.debug.assert(r.tableHeader.itemsCount != 0);
    return r;
}

// TODO: this part must be already open by Table,
// we don't reuse it because open tables are used for random access,
// but if we can identify it's not used by readers we could lock them here,
// most likely it will require tracking of the usages of those files/buffers
//
// TODO: we must use Reader interface here instead of plain reading in order to 
// save required RAM to hold the content til it's merged, it lets us opening files one by one
pub fn initFromDiskTable(alloc: Allocator, path: []const u8) !*BlockReader {
    const tableHeader = try TableHeader.readFile(alloc, path);
    errdefer tableHeader.deinit(alloc);

    const metaIndex = try MetaIndex.readFile(alloc, path, tableHeader.blocksCount);
    errdefer {
        for (metaIndex.records) |*index| {
            index.deinit(alloc);
        }
        alloc.free(metaIndex.records);
    }

    var fba = std.heap.stackFallback(512, alloc);
    const fbaAlloc = fba.get();

    // TODO: open files in parallel to speed up work on high-latency storages, e.g. Ceph
    const indexPath = try std.fs.path.join(fbaAlloc, &.{ path, Filenames.index });
    defer fbaAlloc.free(indexPath);
    const entriesPath = try std.fs.path.join(fbaAlloc, &.{ path, Filenames.entries });
    defer fbaAlloc.free(entriesPath);
    const lensPath = try std.fs.path.join(fbaAlloc, &.{ path, Filenames.lens });
    defer fbaAlloc.free(lensPath);

    const indexBuf = try fs.readAll(alloc, indexPath);
    const entriesBuf = try fs.readAll(alloc, entriesPath);
    const lensBuf = try fs.readAll(alloc, lensPath);

    const r = try alloc.create(BlockReader);
    r.* = .{
        .block = null,
        .metaIndexRecords = metaIndex.records,
        .tableHeader = tableHeader,
        .currentI = 0,
        .isRead = false,
        .indexBuf = .initBuffer(indexBuf),
        .dataBuf = .initBuffer(entriesBuf),
        .lensBuf = .initBuffer(lensBuf),
    };

    std.debug.assert(r.tableHeader.blocksCount != 0);
    std.debug.assert(r.tableHeader.itemsCount != 0);
    return r;
}

pub fn deinit(self: *BlockReader, alloc: Allocator) void {
    if (self.block) |block| block.deinit(alloc);

    for (self.metaIndexRecords) |*rec| rec.deinit(alloc);
    if (self.metaIndexRecords.len > 0) alloc.free(self.metaIndexRecords);
    if (self.blockHeaders.len > 0) alloc.free(self.blockHeaders);
    self.sb.deinit(alloc);
    self.compressedBuf.deinit(alloc);
    self.uncompressedBuf.deinit(alloc);

    alloc.destroy(self);
}

pub fn blockReaderLessThan(one: *BlockReader, another: *BlockReader) bool {
    const first = one.current();
    const second = another.current();
    return std.mem.lessThan(u8, first, second);
}

pub inline fn current(self: *BlockReader) []const u8 {
    return self.block.?.items.items[self.currentI];
}

pub fn next(self: *BlockReader, alloc: Allocator) !bool {
    if (self.isRead) return false;

    // TODO: perhaps it's worth adding read mode enum to show
    // we either read from mem block or decoding from mem table
    if (self.metaIndexRecords.len == 0) {
        self.isRead = true;
        return true;
    }

    if (self.blockHeaders.len == 0 or self.blockHeaderI >= self.blockHeaders.len) {
        const ok = try self.readNextBlockHeaders(alloc);
        if (!ok) {
            const lastItem = self.block.?.items.items[self.block.?.items.items.len - 1];
            std.debug.assert(std.mem.eql(u8, self.tableHeader.lastItem, lastItem));
            self.isRead = true;
            return ok;
        }
    }

    self.blockHeader = &self.blockHeaders[self.blockHeaderI];
    self.blockHeaderI += 1;

    // TODO: for chunked buffer find a way just to  transfer a chunk ownership, perhaps via std.mem.swap,
    // for a file reader we must just read the content
    self.sb.itemsData.clearRetainingCapacity();
    try self.sb.itemsData.ensureUnusedCapacity(alloc, self.blockHeader.itemsBlockSize);
    const itemsDest = self.sb.itemsData.unusedCapacitySlice()[0..self.blockHeader.itemsBlockSize];
    const itemsStart: usize = @intCast(self.blockHeader.itemsBlockOffset);
    const itemsEnd = itemsStart + self.blockHeader.itemsBlockSize;
    @memmove(itemsDest, self.dataBuf.items[itemsStart..itemsEnd]);
    self.sb.itemsData.items.len = self.blockHeader.itemsBlockSize;

    self.sb.lensData.clearRetainingCapacity();
    try self.sb.lensData.ensureUnusedCapacity(alloc, self.blockHeader.lensBlockSize);
    const lensDest = self.sb.lensData.unusedCapacitySlice()[0..self.blockHeader.lensBlockSize];
    const lensStart: usize = @intCast(self.blockHeader.lensBlockOffset);
    const lensEnd = lensStart + self.blockHeader.lensBlockSize;
    @memmove(lensDest, self.lensBuf.items[lensStart..lensEnd]);
    self.sb.lensData.items.len = self.blockHeader.lensBlockSize;

    try self.block.?.decode(
        alloc,
        &self.sb,
        self.blockHeader.firstItem,
        self.blockHeader.prefix,
        self.blockHeader.itemsCount,
        self.blockHeader.encodingType,
    );
    self.blocksRead += 1;
    std.debug.assert(self.blocksRead <= self.tableHeader.blocksCount);
    self.currentI = 0;
    self.itemsRead += self.block.?.items.items.len;
    std.debug.assert(self.itemsRead <= self.tableHeader.itemsCount);

    if (builtin.is_test and !self.firstItemChecked) {
        self.firstItemChecked = true;
        const firstItem = self.block.?.items.items[0];
        std.debug.assert(std.mem.eql(u8, self.tableHeader.firstItem, firstItem));
    }
    return true;
}

fn readNextBlockHeaders(self: *BlockReader, alloc: Allocator) !bool {
    if (self.metaIndexI >= self.metaIndexRecords.len) {
        return false;
    }

    const mi = &self.metaIndexRecords[self.metaIndexI];
    self.metaIndexI += 1;

    self.compressedBuf.clearRetainingCapacity();
    try self.compressedBuf.ensureUnusedCapacity(alloc, mi.indexBlockSize);
    const indexDest = self.compressedBuf.unusedCapacitySlice()[0..mi.indexBlockSize];
    @memmove(indexDest, self.indexBuf.items);
    self.compressedBuf.items.len = self.indexBuf.items.len;

    self.uncompressedBuf.clearRetainingCapacity();
    const uncompressedSize = try encoding.getFrameContentSize(self.compressedBuf.items);
    try self.uncompressedBuf.ensureUnusedCapacity(alloc, uncompressedSize);
    const bufOffset = try encoding.decompress(
        self.uncompressedBuf.unusedCapacitySlice(),
        self.compressedBuf.items,
    );
    self.uncompressedBuf.items.len = bufOffset;

    if (self.blockHeaders.len > 0) alloc.free(self.blockHeaders);
    self.blockHeaders = try BlockHeader.decodeMany(alloc, self.uncompressedBuf.items, mi.blockHeadersCount);
    self.blockHeaderI = 0;
    return true;
}

const testing = std.testing;

fn itemsTotalSize(items: []const []const u8) u32 {
    var total: u32 = 0;
    for (items) |item| total += @intCast(item.len);
    return total;
}

fn createTestMemBlock(alloc: Allocator, items: []const []const u8) !*MemBlock {
    return createTestMemBlockWithMax(alloc, items, itemsTotalSize(items) + 16);
}

fn createTestMemBlockWithMax(alloc: Allocator, items: []const []const u8, maxMemBlockSize: u32) !*MemBlock {
    var block = try MemBlock.init(alloc, maxMemBlockSize);
    errdefer block.deinit(alloc);

    for (items) |item| {
        const ok = block.add(item);
        try testing.expect(ok);
    }

    return block;
}

fn allocIndexedItem(alloc: Allocator, index: usize, totalLen: usize) ![]u8 {
    const buf = try alloc.alloc(u8, totalLen);
    const head = try std.fmt.bufPrint(buf, "item-{d:0>4}", .{index});
    if (head.len < totalLen) {
        for (head.len..totalLen) |i| {
            buf[i] = @intCast((index + i) % 251);
        }
    }
    return buf;
}

test "BlockReader.blockReaderLessThan compares items correctly" {
    const alloc = testing.allocator;

    const items1 = [_][]const u8{ "apple", "banana", "cherry" };
    const items2 = [_][]const u8{ "apricot", "blueberry", "date" };

    const block1 = try createTestMemBlock(alloc, &items1);
    const block2 = try createTestMemBlock(alloc, &items2);

    var reader1 = try BlockReader.initFromMemBlock(alloc, block1);
    defer reader1.deinit(alloc);

    var reader2 = try BlockReader.initFromMemBlock(alloc, block2);
    defer reader2.deinit(alloc);

    // After sorting, "apple" < "apricot"
    const less = BlockReader.blockReaderLessThan(reader1, reader2);
    try testing.expect(less);
    try testing.expect(reader1.currentI == 0);
    try testing.expect(reader2.currentI == 0);
}

test "BlockReader.current returns correct item at currentI" {
    const alloc = testing.allocator;

    const items = [_][]const u8{ "first", "second", "third" };

    const block = try createTestMemBlock(alloc, &items);
    var reader = try BlockReader.initFromMemBlock(alloc, block);
    defer reader.deinit(alloc);

    // After sorting, test that current() returns the item at currentI
    // First item should be "first"
    const first = reader.current();
    try testing.expectEqualSlices(u8, "first", first);

    // Manually change currentI and verify current() updates
    reader.currentI = 1;
    const second = reader.current();
    try testing.expectEqualSlices(u8, "second", second);

    reader.currentI = 2;
    const third = reader.current();
    try testing.expectEqualSlices(u8, "third", third);
}

test "BlockReader.initFromMemTable reads items" {
    const alloc = testing.allocator;

    const Case = struct {
        name: []const u8,
        items: []const []const u8,
        maxMemBlockSize: u32,
        expected: []const []const u8,
        useMultiBlock: bool = false,
    };

    // case 1
    const items_sorted = [_][]const u8{ "alpha", "beta", "delta" };
    // case 2
    const items_unsorted = [_][]const u8{ "delta", "alpha", "beta" };

    // case 3
    const long_len = 200;
    var long_items = try alloc.alloc([]const u8, 3);
    defer alloc.free(long_items);

    const long_a = try alloc.alloc(u8, long_len);
    const long_b = try alloc.alloc(u8, long_len);
    const long_c = try alloc.alloc(u8, long_len);
    defer alloc.free(long_a);
    defer alloc.free(long_b);
    defer alloc.free(long_c);

    long_a[0] = 'x';
    long_b[0] = 'y';
    long_c[0] = 'z';
    @memset(long_a[1..], 'a');
    @memset(long_b[1..], 'a');
    @memset(long_c[1..], 'a');

    long_items[0] = long_a;
    long_items[1] = long_b;
    long_items[2] = long_c;

    // case 4
    const item_count = 80;
    const item_len = 500;
    const full_items = try alloc.alloc([]const u8, item_count);
    defer alloc.free(full_items);

    const block_count = item_count / 2;
    const block1_items = try alloc.alloc([]const u8, block_count);
    const block2_items = try alloc.alloc([]const u8, block_count);
    defer alloc.free(block1_items);
    defer alloc.free(block2_items);

    var owned = try std.ArrayList([]u8).initCapacity(alloc, item_count);
    defer {
        for (owned.items) |buf| alloc.free(buf);
        owned.deinit(alloc);
    }

    var b1: usize = 0;
    var b2: usize = 0;
    for (0..item_count) |i| {
        const item = try allocIndexedItem(alloc, i, item_len);
        try owned.append(alloc, item);
        full_items[i] = item;
        if (i < block_count) {
            block1_items[b1] = item;
            b1 += 1;
        } else {
            block2_items[b2] = item;
            b2 += 1;
        }
    }

    const cases = [_]Case{
        .{
            .name = "plain sorted",
            .items = &items_sorted,
            .maxMemBlockSize = itemsTotalSize(&items_sorted) + 16,
            .expected = &items_sorted,
        },
        .{
            .name = "plain unsorted",
            .items = &items_unsorted,
            .maxMemBlockSize = itemsTotalSize(&items_unsorted) + 16,
            .expected = &items_sorted,
        },
        .{
            .name = "zstd long",
            .items = long_items,
            .maxMemBlockSize = @intCast(long_len * long_items.len + 16),
            .expected = long_items,
        },
        .{
            .name = "multi-block merge",
            .items = full_items,
            .maxMemBlockSize = itemsTotalSize(block1_items) + 16,
            .expected = full_items,
            .useMultiBlock = true,
        },
    };

    for (cases) |case| {
        const block1_items_for_case = if (case.useMultiBlock) block1_items else case.items;
        const block1 = try createTestMemBlockWithMax(alloc, block1_items_for_case, case.maxMemBlockSize);
        defer if (!case.useMultiBlock) block1.deinit(alloc);

        var memTable: *MemTable = undefined;
        var block2: ?*MemBlock = null;
        if (case.useMultiBlock) {
            block2 = try createTestMemBlockWithMax(alloc, block2_items, itemsTotalSize(block2_items) + 16);
            var blocks = [_]*MemBlock{ block1, block2.? };
            memTable = try MemTable.init(alloc, blocks[0..]);
        } else {
            var blocks = [_]*MemBlock{block1};
            memTable = try MemTable.init(alloc, blocks[0..]);
        }
        defer memTable.deinit(alloc);

        var reader = try BlockReader.initFromMemTable(alloc, memTable);
        defer reader.deinit(alloc);

        if (case.useMultiBlock) {
            var expectedI: usize = 0;
            while (try reader.next(alloc)) {
                const decoded = reader.block.?.items.items;
                for (decoded) |item| {
                    try testing.expectEqualSlices(u8, case.expected[expectedI], item);
                    expectedI += 1;
                }
            }
            try testing.expectEqual(case.expected.len, expectedI);
        } else {
            try testing.expect(try reader.next(alloc));
            try testing.expect(reader.block != null);

            const decoded = reader.block.?.items.items;
            try testing.expectEqual(case.expected.len, decoded.len);
            try testing.expectEqualDeep(case.expected, decoded);

            try testing.expect(!try reader.next(alloc));
        }
    }
}
