const std = @import("std");
const Allocator = std.mem.Allocator;
const Io = std.Io;

const Heap = @import("../../stds/heap.zig").Heap;

const sizing = @import("../data/sizing.zig");

const TableHeader = @import("../data/TableHeader.zig");
const copyFields = @import("../lines.zig").copyFields;
const freeFields = @import("../lines.zig").freeFields;
const deinitLinesFull = @import("../lines.zig").deinitLinesFull;
const SID = @import("../lines.zig").SID;
const Line = @import("../lines.zig").Line;
const Field = @import("../lines.zig").Field;

const TableWriter = @import("../data/TableWriter.zig");
const BlockWriter = @import("../data/BlockWriter.zig");
const MemTable = @import("../data/MemTable.zig");
const Table = @import("Table.zig");
const Block = @import("../data/Block.zig");
const BlockData = @import("../data/BlockData.zig").BlockData;
const BlockReader = @import("../data/BlockReader.zig");
const Unpacker = @import("../data/Unpacker.zig");
const ValuesDecoder = @import("../data/ValuesDecoder.zig");

const Consts = @import("../../Consts.zig");

const maxBlockSize = Consts.maxBlockSize;

// TODO: rename this crap
pub fn mergeData(
    io: Io,
    alloc: Allocator,
    writer: *TableWriter,
    readers: *std.ArrayList(*BlockReader),
    stopped: ?*const std.atomic.Value(bool),
) !TableHeader {
    defer writer.close(io);

    var merger = try StreamMerger.init(io, alloc, readers);
    defer merger.deinit(alloc);

    const blockWriter = try BlockWriter.init(alloc);
    defer blockWriter.deinit(alloc);

    while (merger.heap.array.items.len > 0) {
        if (stopped) |stop| {
            if (stop.load(.acquire)) {
                // TODO: test whether break cleans the resources
                return error.Stopped;
            }
        }

        const reader = merger.heap.peek().?;
        try merger.writeBlock(io, alloc, blockWriter, writer, &reader.blockData);
        if (try reader.nextBlock(io, alloc)) {
            merger.heap.fix(0);
        } else {
            const exhaustedReader = merger.heap.pop();
            exhaustedReader.deinit(alloc);
        }
    }

    try merger.flushStream(io, alloc, blockWriter, writer);
    var tableHeader = TableHeader{};
    try blockWriter.finish(io, alloc, writer, &tableHeader);
    return tableHeader;
}

pub const StreamMerger = struct {
    heap: Heap(*BlockReader, BlockReader.blockReaderLessThan),

    // state

    // TODO: add block data as a merger state in order to do less decoding to lines,
    // reference to:
    // commit 0d6a3a45f7c4095101726ef1945c6988ea265fed
    // remove block content from data merger state

    sid: SID = .{ .tenantID = 0, .id = 0 },
    totalKeys: usize = 0,
    size: usize = 0,
    lines: std.ArrayList(Line) = .empty,
    mergeBufferLines: std.ArrayList(Line) = .empty,

    unpacker: *Unpacker,
    decoder: *ValuesDecoder,

    /// init creates a StreamMerger instance from the readers
    /// be aware it mutates readers list inside
    pub fn init(io: Io, alloc: Allocator, readers: *std.ArrayList(*BlockReader)) !StreamMerger {
        // TODO: collect metrics and experiment with flat array on 1-3 elements

        // TODO: experiment with Loser tree intead of heap:
        // https://grafana.com/blog/the-loser-tree-data-structure-how-to-optimize-merges-and-make-your-programs-run-faster/

        const unpacker = try Unpacker.init(alloc);
        errdefer unpacker.deinit(alloc);
        const decoder = try ValuesDecoder.init(alloc);
        errdefer decoder.deinit();

        var i: usize = 0;
        while (i < readers.items.len) {
            const reader = readers.items[i];
            const hasNext = try reader.nextBlock(io, alloc);
            if (!hasNext) {
                reader.deinit(alloc);
                _ = readers.swapRemove(i);
                continue;
            }
            i += 1;
        }

        var heap = Heap(*BlockReader, BlockReader.blockReaderLessThan).init(alloc, readers);
        heap.heapify();

        return .{
            .heap = heap,
            .unpacker = unpacker,
            .decoder = decoder,
        };
    }

    fn reset(self: *StreamMerger, alloc: Allocator) void {
        self.totalKeys = 0;
        self.size = 0;
        self.sid = .{ .tenantID = 0, .id = 0 };

        // TODO: if Lines holds all the fields slice we can reuse the array capacity
        for (self.lines.items) |line| freeFields(alloc, line.fields);
        self.lines.clearRetainingCapacity();
    }

    fn deinit(self: *StreamMerger, alloc: Allocator) void {
        deinitLinesFull(alloc, &self.lines);
        self.mergeBufferLines.deinit(alloc);
        self.unpacker.deinit(alloc);
        self.decoder.deinit();
    }

    pub fn writeBlock(
        self: *StreamMerger,
        io: Io,
        alloc: Allocator,
        blockWriter: *BlockWriter,
        writer: *TableWriter,
        blockData: *BlockData,
    ) !void {
        // TODO: assert the data and merger state

        const totalKeys = blockData.columnsData.items.len + if (blockData.invariantColumns) |invariantCol| invariantCol.len else 0;

        if (!blockData.sid.eql(self.sid)) {
            // it means next stream begins, we have to flush the data
            try self.flushStream(io, alloc, blockWriter, writer);
            self.sid = blockData.sid;

            if (blockData.uncompressedSizeBytes >= maxBlockSize) {
                try blockWriter.writeData(io, alloc, blockData, writer);
            } else {
                try self.decodeLines(io, alloc, blockData);
                self.totalKeys = totalKeys;
            }
        } else if (self.totalKeys + totalKeys > Block.maxColumns) {
            // we have to flush the data before we can add more
            try self.flushStream(io, alloc, blockWriter, writer);
            if (totalKeys > Block.maxColumns) {
                try blockWriter.writeData(io, alloc, blockData, writer);
            } else {
                try self.decodeLines(io, alloc, blockData);
                self.totalKeys = totalKeys;
            }
        } else if (self.size >= maxBlockSize) {
            try self.flushStream(io, alloc, blockWriter, writer);
            try blockWriter.writeData(io, alloc, blockData, writer);
        } else {
            try self.merge(io, alloc, blockData, blockWriter, writer);
            self.totalKeys += totalKeys;
        }
    }

    // TODO: this and many more demonstartes obvious dependece of block and stream writers,
    // they always go together, I have to inject one into another probably
    fn flushStream(self: *StreamMerger, io: Io, alloc: Allocator, writer: *BlockWriter, streamWriter: *TableWriter) !void {
        if (self.lines.items.len > 0) {
            try writer.writeLines(io, alloc, self.sid, self.lines.items, streamWriter);
        }

        self.reset(alloc);
    }

    fn merge(
        self: *StreamMerger,
        io: Io,
        alloc: Allocator,
        blockData: *BlockData,
        blockWriter: *BlockWriter,
        writer: *TableWriter,
    ) !void {
        const len = self.lines.items.len;
        try self.decodeLines(io, alloc, blockData);
        std.debug.assert(self.lines.items.len > len);

        try self.mergeBufferLines.ensureTotalCapacity(alloc, self.lines.items.len);
        defer self.mergeBufferLines.clearRetainingCapacity();

        mergeLines(&self.mergeBufferLines, self.lines.items[0..len], self.lines.items[len..]);
        std.mem.swap(std.ArrayList(Line), &self.mergeBufferLines, &self.lines);

        if (self.size >= maxBlockSize) {
            try self.flushStream(io, alloc, blockWriter, writer);
        }
    }

    fn decodeLines(self: *StreamMerger, io: Io, alloc: Allocator, blockData: *BlockData) !void {
        const block = try Block.initFromData(io, alloc, blockData, self.unpacker, self.decoder);
        defer block.deinit(alloc);

        const offset = self.lines.items.len;
        try block.gatherLines(alloc, &self.lines);

        for (offset..self.lines.items.len) |lineI| {
            const fields = self.lines.items[lineI].fields;
            // data is short living, so we need to copy key values buffers,
            // TODO: we may move field array instead of copying it, do it for every copyFields usage
            const copiedFields = try copyFields(alloc, fields);
            alloc.free(fields);
            self.lines.items[lineI].fields = copiedFields;
        }

        // TODO: understand whether I can use sizing.blockJsonSize,
        // (test is implemented to confirm it, good to have it for merger),
        // then understand whether I can use blockData.uncompressedSizeBytes
        self.size += sizing.linesJsonSize(self.lines.items[offset..]);
    }
};

/// expects dst as a preallocated array,
/// merges left and right into dst
// TODO: find a way to make a merge inplace if possible
fn mergeLines(dst: *std.ArrayList(Line), left: []const Line, right: []const Line) void {
    var i: usize = 0;
    var j: usize = 0;

    while (i < left.len and j < right.len) {
        if (left[i].timestampNs <= right[j].timestampNs) {
            dst.appendAssumeCapacity(left[i]);
            i += 1;
        } else {
            dst.appendAssumeCapacity(right[j]);
            j += 1;
        }
    }

    if (i < left.len) {
        dst.appendSliceAssumeCapacity(left[i..]);
    }
    if (j < right.len) {
        dst.appendSliceAssumeCapacity(right[j..]);
    }
}

test "mergeLines" {
    const alloc = std.testing.allocator;
    const Case = struct {
        left: []const Line,
        right: []const Line,
        expected: []const Line,
    };

    const cases = [_]Case{
        .{
            .left = &.{},
            .right = &.{},
            .expected = &.{},
        },
        .{
            .left = &[_]Line{
                .{
                    .timestampNs = 123,
                    .fields = @constCast(&[_]Field{.{ .key = "a", .value = "left" }}),
                },
            },
            .right = &.{},
            .expected = &[_]Line{
                .{
                    .timestampNs = 123,
                    .fields = @constCast(&[_]Field{.{ .key = "a", .value = "left" }}),
                },
            },
        },
        .{
            .left = &[_]Line{
                .{
                    .timestampNs = 123,
                    .fields = @constCast(&[_]Field{.{ .key = "a", .value = "left" }}),
                },
            },
            .right = &[_]Line{
                .{
                    .timestampNs = 456,
                    .fields = @constCast(&[_]Field{.{ .key = "b", .value = "right" }}),
                },
            },
            .expected = &[_]Line{
                .{
                    .timestampNs = 123,
                    .fields = @constCast(&[_]Field{.{ .key = "a", .value = "left" }}),
                },
                .{
                    .timestampNs = 456,
                    .fields = @constCast(&[_]Field{.{ .key = "b", .value = "right" }}),
                },
            },
        },
        .{
            .left = &[_]Line{
                .{
                    .timestampNs = 123,
                    .fields = @constCast(&[_]Field{.{ .key = "a", .value = "left-1" }}),
                },
                .{
                    .timestampNs = 456,
                    .fields = @constCast(&[_]Field{.{ .key = "a", .value = "left-2" }}),
                },
            },
            .right = &[_]Line{
                .{
                    .timestampNs = 123,
                    .fields = @constCast(&[_]Field{.{ .key = "b", .value = "right-1" }}),
                },
                .{
                    .timestampNs = 456,
                    .fields = @constCast(&[_]Field{.{ .key = "b", .value = "right-2" }}),
                },
            },
            .expected = &[_]Line{
                .{
                    .timestampNs = 123,
                    .fields = @constCast(&[_]Field{.{ .key = "a", .value = "left-1" }}),
                },
                .{
                    .timestampNs = 123,
                    .fields = @constCast(&[_]Field{.{ .key = "b", .value = "right-1" }}),
                },
                .{
                    .timestampNs = 456,
                    .fields = @constCast(&[_]Field{.{ .key = "a", .value = "left-2" }}),
                },
                .{
                    .timestampNs = 456,
                    .fields = @constCast(&[_]Field{.{ .key = "b", .value = "right-2" }}),
                },
            },
        },
        .{
            .left = &[_]Line{
                .{
                    .timestampNs = 12,
                    .fields = @constCast(&[_]Field{.{ .key = "a", .value = "left-1" }}),
                },
                .{
                    .timestampNs = 123456,
                    .fields = @constCast(&[_]Field{.{ .key = "a", .value = "left-2" }}),
                },
            },
            .right = &[_]Line{
                .{
                    .timestampNs = 1,
                    .fields = @constCast(&[_]Field{.{ .key = "b", .value = "right-1" }}),
                },
                .{
                    .timestampNs = 456,
                    .fields = @constCast(&[_]Field{.{ .key = "b", .value = "right-2" }}),
                },
            },
            .expected = &[_]Line{
                .{
                    .timestampNs = 1,
                    .fields = @constCast(&[_]Field{.{ .key = "b", .value = "right-1" }}),
                },
                .{
                    .timestampNs = 12,
                    .fields = @constCast(&[_]Field{.{ .key = "a", .value = "left-1" }}),
                },
                .{
                    .timestampNs = 456,
                    .fields = @constCast(&[_]Field{.{ .key = "b", .value = "right-2" }}),
                },
                .{
                    .timestampNs = 123456,
                    .fields = @constCast(&[_]Field{.{ .key = "a", .value = "left-2" }}),
                },
            },
        },
    };

    for (cases) |case| {
        var merged = try std.ArrayList(Line).initCapacity(alloc, case.left.len + case.right.len);
        defer merged.deinit(alloc);

        mergeLines(&merged, case.left, case.right);
        try std.testing.expectEqualDeep(case.expected, merged.items);
    }
}

test "mergeData keeps merged memtable buffers alive after source memtables deinit" {
    const alloc = std.testing.allocator;
    const io = std.testing.io;
    const sid = SID{ .tenantID = 1, .id = 42 };

    const expected = [_]struct {
        timestampNs: u64,
        value: []const u8,
    }{
        .{ .timestampNs = 1, .value = "left-1" },
        .{ .timestampNs = 2, .value = "right-1" },
        .{ .timestampNs = 3, .value = "left-2" },
        .{ .timestampNs = 4, .value = "right-2" },
    };

    const fieldKey = "key";

    const mergedMemTable = blk: {
        const leftMemTable = try MemTable.init(alloc);
        const leftTable = try Table.fromMem(alloc, leftMemTable);
        defer leftTable.close(io);
        const rightMemTable = try MemTable.init(alloc);
        const rightTable = try Table.fromMem(alloc, rightMemTable);
        defer rightTable.close(io);

        var leftFields1 = [_]Field{.{ .key = fieldKey, .value = "left-1" }};
        var leftFields2 = [_]Field{.{ .key = fieldKey, .value = "left-2" }};
        var rightFields1 = [_]Field{.{ .key = fieldKey, .value = "right-1" }};
        var rightFields2 = [_]Field{.{ .key = fieldKey, .value = "right-2" }};

        var leftLines = [_]Line{
            .{ .timestampNs = 1, .fields = leftFields1[0..] },
            .{ .timestampNs = 3, .fields = leftFields2[0..] },
        };
        var rightLines = [_]Line{
            .{ .timestampNs = 2, .fields = rightFields1[0..] },
            .{ .timestampNs = 4, .fields = rightFields2[0..] },
        };

        try leftMemTable.addLinesForSid(io, alloc, sid, leftLines[0..]);
        try rightMemTable.addLinesForSid(io, alloc, sid, rightLines[0..]);

        var readers = try std.ArrayList(*BlockReader).initCapacity(alloc, 2);
        defer readers.deinit(alloc);

        try readers.append(alloc, try BlockReader.initFromMemTable(alloc, leftTable));
        try readers.append(alloc, try BlockReader.initFromMemTable(alloc, rightTable));

        const dstMemTable = try MemTable.init(alloc);
        errdefer dstMemTable.deinit(alloc);
        const stopped: ?*std.atomic.Value(bool) = null;
        const streamWriter = try TableWriter.initMem(alloc, dstMemTable);
        defer streamWriter.deinit(alloc);
        dstMemTable.tableHeader = try mergeData(io, alloc, streamWriter, &readers, stopped);

        try std.testing.expect(dstMemTable.indexBuf.items.len > 0);
        try std.testing.expect(dstMemTable.metaIndexBuf.items.len > 0);
        try std.testing.expect(dstMemTable.columnsHeaderIndexBuf.items.len > 0);
        try std.testing.expect(dstMemTable.columnsHeaderBuf.items.len > 0);
        try std.testing.expect(dstMemTable.timestampsBuf.items.len > 0);

        break :blk dstMemTable;
    };
    const mergedTable = try Table.fromMem(alloc, mergedMemTable);
    defer mergedTable.close(io);

    var mergedReader = try BlockReader.initFromMemTable(alloc, mergedTable);
    defer mergedReader.deinit(alloc);

    const unpacker = try Unpacker.init(alloc);
    defer unpacker.deinit(alloc);
    const decoder = try ValuesDecoder.init(alloc);
    defer decoder.deinit();

    var expectedI: usize = 0;
    while (try mergedReader.nextBlock(io, alloc)) {
        const block = try Block.initFromData(io, alloc, &mergedReader.blockData, unpacker, decoder);
        defer block.deinit(alloc);

        var lines = std.ArrayList(Line).empty;
        defer {
            for (lines.items) |line| {
                alloc.free(line.fields);
            }
            lines.deinit(alloc);
        }
        try block.gatherLines(alloc, &lines);

        for (lines.items) |line| {
            try std.testing.expect(expectedI < expected.len);
            try std.testing.expectEqual(expected[expectedI].timestampNs, line.timestampNs);
            try std.testing.expectEqual(@as(usize, 1), line.fields.len);
            try std.testing.expectEqualStrings(fieldKey, line.fields[0].key);
            try std.testing.expectEqualStrings(expected[expectedI].value, line.fields[0].value);
            expectedI += 1;
        }
    }

    try std.testing.expectEqual(expected.len, expectedI);
}

test "mergeData flushes maxLines for one stream" {
    const alloc = std.testing.allocator;
    const io = std.testing.io;
    const sid = SID{ .tenantID = 1, .id = 42 };

    const srcMemTable = try MemTable.init(alloc);
    const srcTable = try Table.fromMem(alloc, srcMemTable);
    defer srcTable.close(io);

    var lines: [Block.maxLines]Line = undefined;
    for (0..Block.maxLines) |i| {
        lines[i] = .{
            .timestampNs = @intCast(i),
            .fields = @constCast(&[_]Field{.{ .key = "level", .value = "info" }}),
        };
    }

    try srcMemTable.addLinesForSid(io, alloc, sid, &lines);

    var readers = try std.ArrayList(*BlockReader).initCapacity(alloc, 1);
    defer readers.deinit(alloc);
    try readers.append(alloc, try BlockReader.initFromMemTable(alloc, srcTable));

    const dstMemTable = try MemTable.init(alloc);
    const dstTable = try Table.fromMem(alloc, dstMemTable);
    defer dstTable.close(io);

    const streamWriter = try TableWriter.initMem(alloc, dstMemTable);
    defer streamWriter.deinit(alloc);
    dstMemTable.tableHeader = try mergeData(io, alloc, streamWriter, &readers, null);

    var mergedReader = try BlockReader.initFromMemTable(alloc, dstTable);
    defer mergedReader.deinit(alloc);

    var actualRows: usize = 0;
    while (try mergedReader.nextBlock(io, alloc)) {
        actualRows += mergedReader.blockData.len;
    }

    try std.testing.expectEqual(Block.maxLines, actualRows);
}

test "mergeData multi tenant" {
    // TODO: make it testing alloc
    const alloc = std.heap.page_allocator;
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const rootPath = try tmp.dir.realPathFileAlloc(io, ".", alloc);
    defer alloc.free(rootPath);

    const tenantIDs = [_]u64{ 1, 2, 3, 4, 5, 6, 7, 8 };

    var readers = try std.ArrayList(*BlockReader).initCapacity(alloc, tenantIDs.len);
    var tables = try std.ArrayList(*Table).initCapacity(alloc, tenantIDs.len);
    defer {
        for (readers.items) |reader| {
            reader.deinit(alloc);
        }
        readers.deinit(alloc);
        for (tables.items) |table| {
            table.close(io);
        }
        tables.deinit(alloc);
    }

    for (tenantIDs, 0..) |tenantID, i| {
        const tablePath = try std.fmt.allocPrint(
            alloc,
            "{s}/table-{d}",
            .{ rootPath, i },
        );

        const memTable = try MemTable.init(alloc);
        defer memTable.deinit(alloc);

        var fields = [_]Field{
            .{ .key = "app", .value = "repro" },
            .{ .key = "level", .value = "info" },
        };
        var lines = [_]Line{.{
            .timestampNs = @intCast(i + 1),
            .fields = fields[0..],
        }};

        try memTable.addLinesForSid(io, alloc, .{ .tenantID = tenantID, .id = 1 }, lines[0..]);
        try memTable.storeToDisk(io, alloc, tablePath);

        const table = try Table.open(io, alloc, tablePath);
        try tables.append(alloc, table);
        const reader = try BlockReader.initFromDiskTable(io, alloc, table);
        try readers.append(alloc, reader);
    }

    const dstMemTable = try MemTable.init(alloc);
    defer dstMemTable.deinit(alloc);

    const stopped: ?*std.atomic.Value(bool) = null;
    const streamWriter = try TableWriter.initMem(alloc, dstMemTable);
    defer streamWriter.deinit(alloc);
    dstMemTable.tableHeader = try mergeData(io, alloc, streamWriter, &readers, stopped);

    try std.testing.expectEqual(@as(u32, tenantIDs.len), dstMemTable.tableHeader.len);
}
