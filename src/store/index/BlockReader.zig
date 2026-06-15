const std = @import("std");
const Allocator = std.mem.Allocator;
const Io = std.Io;
const builtin = @import("builtin");

const encoding = @import("encoding");

const filenames = @import("../../filenames.zig");
const fs = @import("../../fs.zig");

const MemBlock = @import("MemBlock.zig");
const Table = @import("Table.zig");
const MemTable = @import("MemTable.zig");
const MetaIndex = @import("MetaIndex.zig");
const EntriesBlock = @import("EntriesBlock.zig");
const TableHeader = @import("TableHeader.zig");
const BlockHeader = @import("BlockHeader.zig");

const BlockReader = @This();

// TODO: make it non optional, it was historically optional
// to show mem block is not owned on decoding it from mem table,
// but apparently it's owning
block: ?*MemBlock,
// TODO: this is not ok, we must a better ownership model,
// it must take it from outside
ownsBlock: bool = false,
// TODO: no idea yet how to avoid it
ownsStorage: bool = false,
tableHeader: TableHeader,
table: *const Table,

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

firstItemChecked: if (builtin.is_test) bool else void = if (builtin.is_test) false else {},

pub fn initFromMemBlock(alloc: Allocator, block: *MemBlock) !*BlockReader {
    std.debug.assert(block.memEntries.items.len > 0);
    block.sortData();

    std.debug.print(
        "index.BlockReader: open mem block, prefix={s}\n",
        .{
            block.prefix,
        },
    );
    const r = try alloc.create(BlockReader);
    r.* = .{
        .block = block,
        .ownsBlock = false,
        .ownsStorage = false,
        .tableHeader = .{},
        .currentI = 0,
        .isRead = false,
        .table = undefined,
    };
    return r;
}

pub fn initFromMemTable(alloc: Allocator, table: *const Table) !*BlockReader {
    const memTable = table.inner.mem;
    std.debug.assert(memTable.tableHeader.blocksCount > 0);
    const metaIndexRecords = try MetaIndex.decodeDecompress(
        alloc,
        memTable.metaindexBuf.items,
        memTable.tableHeader.blocksCount,
    );
    errdefer alloc.free(metaIndexRecords.records);

    std.debug.print(
        "index.BlockReader: open mem table, metaIndexRecordsLen={d}, metaIndexRecordsSize={d}\n",
        .{
            metaIndexRecords.records.len,
            metaIndexRecords.compressedSize,
        },
    );

    const block = try MemBlock.init(alloc, .{
        .blocksCountHint = @intCast(memTable.tableHeader.entriesCount),
    });
    errdefer block.deinit(alloc);

    const r = try alloc.create(BlockReader);
    r.* = .{
        .block = block,
        .ownsBlock = true,
        .ownsStorage = false,
        .metaIndexRecords = metaIndexRecords.records,
        .tableHeader = memTable.tableHeader,
        .table = table,
        .currentI = 0,
        .isRead = false,
    };

    std.debug.assert(r.tableHeader.blocksCount != 0);
    std.debug.assert(r.tableHeader.entriesCount != 0);
    return r;
}

pub fn initFromDiskTable(io: Io, alloc: Allocator, table: *const Table) !*BlockReader {
    const path = table.path;
    const tableHeader = try TableHeader.readFile(io, alloc, path);
    errdefer tableHeader.deinit(alloc);

    const metaIndex = try MetaIndex.readFile(io, alloc, path, tableHeader.blocksCount);
    errdefer {
        for (metaIndex.records) |*index| {
            index.deinit(alloc);
        }
        alloc.free(metaIndex.records);
    }

    std.debug.print(
        "index.BlockReader: open disk table path={s}\n",
        .{
            table.path,
        },
    );

    const block = try MemBlock.init(alloc, .{
        .blocksCountHint = @intCast(tableHeader.entriesCount),
    });
    errdefer block.deinit(alloc);

    const r = try alloc.create(BlockReader);
    errdefer alloc.destroy(r);
    r.* = .{
        .block = block,
        .ownsBlock = true,
        .ownsStorage = true,
        .metaIndexRecords = metaIndex.records,
        .tableHeader = tableHeader,
        .table = table,
        .currentI = 0,
        .isRead = false,
    };

    std.debug.assert(r.tableHeader.blocksCount != 0);
    std.debug.assert(r.tableHeader.entriesCount != 0);
    return r;
}

pub fn deinit(self: *BlockReader, alloc: Allocator) void {
    if (self.ownsBlock) {
        if (self.block) |block| block.deinit(alloc);
    }

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
    return self.block.?.get(self.currentI);
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
            const lastItem = self.block.?.last();
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

    try self.block.?.decode(
        alloc,
        &self.entriesBlock,
        self.blockHeader.firstEntry,
        self.blockHeader.prefix,
        self.blockHeader.entriesCount,
        self.blockHeader.encodingType,
    );
    self.blocksRead += 1;
    std.debug.assert(self.blocksRead <= self.tableHeader.blocksCount);
    self.currentI = 0;
    self.itemsRead += self.block.?.memEntries.items.len;
    std.debug.assert(self.itemsRead <= self.tableHeader.entriesCount);

    if (builtin.is_test and !self.firstItemChecked) {
        self.firstItemChecked = true;
        const firstEntry = self.block.?.get(0);
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
    std.debug.print(
        "InvalidIndexBlockRange stage={s} metaindexI={d} indexOffset={d}" ++
            " expectedSize={d} actualSize={d} tableBlocks={d} tableEntries={d} miBlockHeaders={d}\n",
        .{
            stage,
            metaIndexI,
            mi.indexBlockOffset,
            expectedSize,
            actualSize,
            self.tableHeader.blocksCount,
            self.tableHeader.entriesCount,
            mi.blockHeadersCount,
        },
    );

    if (metaIndexI > 0) {
        const prev = self.metaIndexRecords[metaIndexI - 1];
        std.debug.print(
            "prevMetaindexI={d} prevOffset={d} prevSize={d} prevHeaders={d}\n",
            .{ metaIndexI - 1, prev.indexBlockOffset, prev.indexBlockSize, prev.blockHeadersCount },
        );
    }

    if (metaIndexI + 1 < self.metaIndexRecords.len) {
        const nextMi = self.metaIndexRecords[metaIndexI + 1];
        std.debug.print(
            "hint: nextMetaindexI={d} nextOffset={d} nextSize={d} nextHeaders={d}\n",
            .{ metaIndexI + 1, nextMi.indexBlockOffset, nextMi.indexBlockSize, nextMi.blockHeadersCount },
        );
    }
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
    var block = try MemBlock.init(alloc, .{
        .maxMemBlockSize = maxMemBlockSize,
        .blocksCountHint = items.len,
    });
    errdefer block.deinit(alloc);

    for (items) |item| {
        const ok = block.add(item);
        try testing.expect(ok);
    }

    return block;
}

fn allocIndexedItem(alloc: Allocator, index: usize, totalLen: usize) ![]u8 {
    const buf = try alloc.alloc(u8, totalLen);
    errdefer alloc.free(buf);

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
    defer block1.deinit(alloc);

    const block2 = try createTestMemBlock(alloc, &items2);
    defer block2.deinit(alloc);

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
    defer block.deinit(alloc);

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
    const io = testing.io;

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
    defer alloc.free(long_a);

    const long_b = try alloc.alloc(u8, long_len);
    defer alloc.free(long_b);

    const long_c = try alloc.alloc(u8, long_len);
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
    defer alloc.free(block1_items);

    const block2_items = try alloc.alloc([]const u8, block_count);
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
        errdefer alloc.free(item);

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
        defer block1.deinit(alloc);

        const memTable: *MemTable = blk: {
            var block2: ?*MemBlock = null;
            if (case.useMultiBlock) {
                block2 = try createTestMemBlockWithMax(alloc, block2_items, itemsTotalSize(block2_items) + 16);
                defer block2.?.deinit(alloc);
                var blocks = [_]*MemBlock{ block1, block2.? };
                break :blk try MemTable.init(io, alloc, blocks[0..]);
            } else {
                var blocks = [_]*MemBlock{block1};
                break :blk try MemTable.init(io, alloc, blocks[0..]);
            }
        };
        const table = try Table.fromMem(alloc, memTable);
        defer table.close(io);

        var reader = try BlockReader.initFromMemTable(alloc, table);
        defer reader.deinit(alloc);

        if (case.useMultiBlock) {
            var expectedI: usize = 0;
            while (try reader.next(io, alloc)) {
                var iter = reader.block.?.iterator();
                while (iter.next()) |item| {
                    try testing.expectEqualStrings(case.expected[expectedI], item);
                    expectedI += 1;
                }
            }
            try testing.expectEqual(case.expected.len, expectedI);
        } else {
            try testing.expect(try reader.next(io, alloc));
            try testing.expect(reader.block != null);

            const decoded = reader.block.?.memEntries.items;
            try testing.expectEqual(case.expected.len, decoded.len);
            for (0..decoded.len) |i| {
                try testing.expectEqualStrings(case.expected[i], reader.block.?.get(i));
            }

            try testing.expect(!try reader.next(io, alloc));
        }
    }
}

test "BlockReader.next returns InvalidIndexBlockRange on empty index buffer with non-empty metaindex" {
    const alloc = testing.allocator;
    const io = testing.io;

    const items = [_][]const u8{
        "item-0",
        "item-1",
        "item-2",
    };

    const block = try createTestMemBlock(alloc, &items);
    defer block.deinit(alloc);

    var blocks = [_]*MemBlock{block};
    const memTable = try MemTable.init(io, alloc, &blocks);
    const table = try Table.fromMem(alloc, memTable);
    defer table.close(io);

    var reader = try BlockReader.initFromMemTable(alloc, table);
    defer reader.deinit(alloc);

    try testing.expect(reader.metaIndexRecords.len > 0);
    try testing.expect(reader.metaIndexRecords[0].indexBlockSize > 0);

    memTable.indexBuf.clearRetainingCapacity();

    try testing.expectError(error.InvalidIndexBlockRange, reader.next(io, alloc));
}

test "BlockReader.initFromDiskTable decodes blocks without null crash" {
    const alloc = testing.allocator;
    const io = testing.io;

    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    const rootPath = try tmp.dir.realPathFileAlloc(io, ".", alloc);
    defer alloc.free(rootPath);

    const tablePath = try std.fs.path.join(alloc, &.{ rootPath, "table" });

    const items = [_][]const u8{ "delta", "alpha", "beta" };
    const expected = [_][]const u8{ "alpha", "beta", "delta" };

    const block = try createTestMemBlock(alloc, &items);
    defer block.deinit(alloc);

    var blocks = [_]*MemBlock{block};
    const memTable = try MemTable.init(io, alloc, &blocks);
    defer memTable.deinit(alloc);

    try memTable.storeToDisk(io, alloc, tablePath);

    const diskTable = try Table.open(io, alloc, tablePath);
    defer diskTable.close(io);

    var reader = try BlockReader.initFromDiskTable(io, alloc, diskTable);
    defer reader.deinit(alloc);

    const hasNext = try reader.next(io, alloc);
    try testing.expect(hasNext);
    try testing.expect(reader.block != null);
    for (0..expected.len) |i| {
        try testing.expectEqualStrings(expected[i], reader.block.?.get(i));
    }
    try testing.expect(!try reader.next(io, alloc));
}
