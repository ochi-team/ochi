const std = @import("std");
const Allocator = std.mem.Allocator;

const Heap = @import("../../stds/heap.zig").Heap;

const sizing = @import("../inmem/sizing.zig");

const TableHeader = @import("../inmem/TableHeader.zig");
const SID = @import("../lines.zig").SID;
const Line = @import("../lines.zig").Line;
const Field = @import("../lines.zig").Field;

const StreamWriter = @import("../inmem/StreamWriter.zig");
const BlockWriter = @import("../inmem/BlockWriter.zig");
const MemTable = @import("../inmem/MemTable.zig");
const Block = @import("../inmem/Block.zig");
const BlockData = @import("../inmem/BlockData.zig").BlockData;
const BlockReader = @import("../inmem/reader.zig").BlockReader;
const Unpacker = @import("../inmem/Unpacker.zig");
const ValuesDecoder = @import("../inmem/ValuesDecoder.zig");

// TODO: rename this crap
pub fn mergeData(
    alloc: Allocator,
    writer: *StreamWriter,
    readers: *std.ArrayList(*BlockReader),
    stopped: ?*const std.atomic.Value(bool),
) !TableHeader {
    var merger = try StreamMerger.init(alloc, readers);
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
        try merger.writeBlock(alloc, blockWriter, writer, &reader.blockData);
        if (try reader.nextBlock(alloc)) {
            merger.heap.fix(0);
        } else {
            const exhaustedReader = merger.heap.pop();
            exhaustedReader.deinit(alloc);
        }
    }

    try merger.flushStream(alloc, blockWriter, writer);
    var tableHeader = TableHeader{};
    try blockWriter.finish(alloc, writer, &tableHeader);
    return tableHeader;
}

pub const StreamMerger = struct {
    heap: Heap(*BlockReader, BlockReader.blockReaderLessThan),

    // state

    // TODO: add block data as a merger state in order to do less decoding to lines,
    // reference to:
    // commit 0d6a3a45f7c4095101726ef1945c6988ea265fed
    // remove block content from data merger state

    sid: SID = .{ .tenantID = "", .id = 0 },
    totalKeys: usize = 0,
    size: usize = 0,
    lines: std.ArrayList(Line) = .empty,
    mergeBufferLines: std.ArrayList(Line) = .empty,

    unpacker: *Unpacker,
    decoder: *ValuesDecoder,

    /// init creates a StreamMerger instance from the readers
    /// be aware it mutates readers list inside
    pub fn init(alloc: Allocator, readers: *std.ArrayList(*BlockReader)) !StreamMerger {
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
            const hasNext = try reader.nextBlock(alloc);
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
        self.sid = .{ .tenantID = "", .id = 0 };

        // TODO: if Lines holds all the fields slice we can reuse the array capacity
        for (self.lines.items) |line| alloc.free(line.fields);
        self.lines.clearRetainingCapacity();
    }

    fn deinit(self: *StreamMerger, alloc: Allocator) void {
        for (self.lines.items) |line| alloc.free(line.fields);
        self.lines.deinit(alloc);
        self.mergeBufferLines.deinit(alloc);
        self.unpacker.deinit(alloc);
        self.decoder.deinit();
    }

    pub fn writeBlock(
        self: *StreamMerger,
        alloc: Allocator,
        blockWriter: *BlockWriter,
        writer: *StreamWriter,
        blockData: *BlockData,
    ) !void {
        // TODO: assert the data and merger state

        const totalKeys = blockData.columnsData.items.len + if (blockData.celledColumns) |celled| celled.len else 0;

        if (!blockData.sid.eql(&self.sid)) {
            // it means next stream begins, we have to flush the data
            try self.flushStream(alloc, blockWriter, writer);
            self.sid = blockData.sid;

            if (blockData.uncompressedSizeBytes >= MemTable.maxBlockSize) {
                try blockWriter.writeData(alloc, blockData, writer);
            } else {
                try self.decodeLines(alloc, blockData);
                self.totalKeys = totalKeys;
            }
        } else if (self.totalKeys + totalKeys > Block.maxColumns) {
            // we have to flush the data before we can add more
            try self.flushStream(alloc, blockWriter, writer);
            if (totalKeys > Block.maxColumns) {
                try blockWriter.writeData(alloc, blockData, writer);
            } else {
                try self.decodeLines(alloc, blockData);
                self.totalKeys = totalKeys;
            }
        } else if (self.size >= MemTable.maxBlockSize) {
            try self.flushStream(alloc, blockWriter, writer);
            try blockWriter.writeData(alloc, blockData, writer);
        } else {
            try self.merge(alloc, blockData, blockWriter, writer);
            self.totalKeys += totalKeys;
        }
    }

    // TODO: this and many more demonstartes obvious dependece of block and stream writers,
    // they always go together, I have to inject one into another probably
    fn flushStream(self: *StreamMerger, alloc: Allocator, writer: *BlockWriter, streamWriter: *StreamWriter) !void {
        if (self.lines.items.len > 0) {
            try writer.writeLines(alloc, self.sid, self.lines.items, streamWriter);
        }

        self.reset(alloc);
    }

    fn merge(
        self: *StreamMerger,
        alloc: Allocator,
        blockData: *BlockData,
        blockWriter: *BlockWriter,
        writer: *StreamWriter,
    ) !void {
        const len = self.lines.items.len;
        try self.decodeLines(alloc, blockData);
        std.debug.assert(self.lines.items.len > len);

        try self.mergeBufferLines.ensureTotalCapacity(alloc, self.lines.items.len);
        defer self.mergeBufferLines.clearRetainingCapacity();

        mergeLines(&self.mergeBufferLines, self.lines.items[0..len], self.lines.items[len..]);
        std.mem.swap(std.ArrayList(Line), &self.mergeBufferLines, &self.lines);

        if (self.size >= MemTable.maxBlockSize) {
            try self.flushStream(alloc, blockWriter, writer);
        }
    }

    fn decodeLines(self: *StreamMerger, alloc: Allocator, blockData: *BlockData) !void {
        const block = try Block.initFromData(alloc, blockData, self.unpacker, self.decoder);
        defer block.deinit(alloc);

        const offset = self.lines.items.len;
        try block.gatherLines(alloc, &self.lines);

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
                    .sid = .{ .tenantID = "", .id = 1 },
                    .fields = @constCast(&[_]Field{.{ .key = "a", .value = "left" }}),
                },
            },
            .right = &.{},
            .expected = &[_]Line{
                .{
                    .timestampNs = 123,
                    .sid = .{ .tenantID = "", .id = 1 },
                    .fields = @constCast(&[_]Field{.{ .key = "a", .value = "left" }}),
                },
            },
        },
        .{
            .left = &[_]Line{
                .{
                    .timestampNs = 123,
                    .sid = .{ .tenantID = "", .id = 1 },
                    .fields = @constCast(&[_]Field{.{ .key = "a", .value = "left" }}),
                },
            },
            .right = &[_]Line{
                .{
                    .timestampNs = 456,
                    .sid = .{ .tenantID = "", .id = 1 },
                    .fields = @constCast(&[_]Field{.{ .key = "b", .value = "right" }}),
                },
            },
            .expected = &[_]Line{
                .{
                    .timestampNs = 123,
                    .sid = .{ .tenantID = "", .id = 1 },
                    .fields = @constCast(&[_]Field{.{ .key = "a", .value = "left" }}),
                },
                .{
                    .timestampNs = 456,
                    .sid = .{ .tenantID = "", .id = 1 },
                    .fields = @constCast(&[_]Field{.{ .key = "b", .value = "right" }}),
                },
            },
        },
        .{
            .left = &[_]Line{
                .{
                    .timestampNs = 123,
                    .sid = .{ .tenantID = "", .id = 1 },
                    .fields = @constCast(&[_]Field{.{ .key = "a", .value = "left-1" }}),
                },
                .{
                    .timestampNs = 456,
                    .sid = .{ .tenantID = "", .id = 1 },
                    .fields = @constCast(&[_]Field{.{ .key = "a", .value = "left-2" }}),
                },
            },
            .right = &[_]Line{
                .{
                    .timestampNs = 123,
                    .sid = .{ .tenantID = "", .id = 1 },
                    .fields = @constCast(&[_]Field{.{ .key = "b", .value = "right-1" }}),
                },
                .{
                    .timestampNs = 456,
                    .sid = .{ .tenantID = "", .id = 1 },
                    .fields = @constCast(&[_]Field{.{ .key = "b", .value = "right-2" }}),
                },
            },
            .expected = &[_]Line{
                .{
                    .timestampNs = 123,
                    .sid = .{ .tenantID = "", .id = 1 },
                    .fields = @constCast(&[_]Field{.{ .key = "a", .value = "left-1" }}),
                },
                .{
                    .timestampNs = 123,
                    .sid = .{ .tenantID = "", .id = 1 },
                    .fields = @constCast(&[_]Field{.{ .key = "b", .value = "right-1" }}),
                },
                .{
                    .timestampNs = 456,
                    .sid = .{ .tenantID = "", .id = 1 },
                    .fields = @constCast(&[_]Field{.{ .key = "a", .value = "left-2" }}),
                },
                .{
                    .timestampNs = 456,
                    .sid = .{ .tenantID = "", .id = 1 },
                    .fields = @constCast(&[_]Field{.{ .key = "b", .value = "right-2" }}),
                },
            },
        },
        .{
            .left = &[_]Line{
                .{
                    .timestampNs = 12,
                    .sid = .{ .tenantID = "", .id = 1 },
                    .fields = @constCast(&[_]Field{.{ .key = "a", .value = "left-1" }}),
                },
                .{
                    .timestampNs = 123456,
                    .sid = .{ .tenantID = "", .id = 1 },
                    .fields = @constCast(&[_]Field{.{ .key = "a", .value = "left-2" }}),
                },
            },
            .right = &[_]Line{
                .{
                    .timestampNs = 1,
                    .sid = .{ .tenantID = "", .id = 1 },
                    .fields = @constCast(&[_]Field{.{ .key = "b", .value = "right-1" }}),
                },
                .{
                    .timestampNs = 456,
                    .sid = .{ .tenantID = "", .id = 1 },
                    .fields = @constCast(&[_]Field{.{ .key = "b", .value = "right-2" }}),
                },
            },
            .expected = &[_]Line{
                .{
                    .timestampNs = 1,
                    .sid = .{ .tenantID = "", .id = 1 },
                    .fields = @constCast(&[_]Field{.{ .key = "b", .value = "right-1" }}),
                },
                .{
                    .timestampNs = 12,
                    .sid = .{ .tenantID = "", .id = 1 },
                    .fields = @constCast(&[_]Field{.{ .key = "a", .value = "left-1" }}),
                },
                .{
                    .timestampNs = 456,
                    .sid = .{ .tenantID = "", .id = 1 },
                    .fields = @constCast(&[_]Field{.{ .key = "b", .value = "right-2" }}),
                },
                .{
                    .timestampNs = 123456,
                    .sid = .{ .tenantID = "", .id = 1 },
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
    const sid = SID{ .tenantID = "tenant-a", .id = 42 };

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
        defer leftMemTable.deinit(alloc);
        const rightMemTable = try MemTable.init(alloc);
        defer rightMemTable.deinit(alloc);

        var leftFields1 = [_]Field{.{ .key = fieldKey, .value = "left-1" }};
        var leftFields2 = [_]Field{.{ .key = fieldKey, .value = "left-2" }};
        var rightFields1 = [_]Field{.{ .key = fieldKey, .value = "right-1" }};
        var rightFields2 = [_]Field{.{ .key = fieldKey, .value = "right-2" }};

        var leftLines = [_]Line{
            .{ .timestampNs = 1, .sid = sid, .fields = leftFields1[0..] },
            .{ .timestampNs = 3, .sid = sid, .fields = leftFields2[0..] },
        };
        var rightLines = [_]Line{
            .{ .timestampNs = 2, .sid = sid, .fields = rightFields1[0..] },
            .{ .timestampNs = 4, .sid = sid, .fields = rightFields2[0..] },
        };

        try leftMemTable.addLines(alloc, leftLines[0..]);
        try rightMemTable.addLines(alloc, rightLines[0..]);

        var readers = try std.ArrayList(*BlockReader).initCapacity(alloc, 2);
        defer readers.deinit(alloc);

        try readers.append(alloc, try BlockReader.initFromMemTable(alloc, leftMemTable));
        try readers.append(alloc, try BlockReader.initFromMemTable(alloc, rightMemTable));

        const dstMemTable = try MemTable.init(alloc);
        errdefer dstMemTable.deinit(alloc);
        const stopped: ?*std.atomic.Value(bool) = null;
        dstMemTable.tableHeader = try mergeData(alloc, dstMemTable.streamWriter, &readers, stopped);

        try std.testing.expect(dstMemTable.streamWriter.indexDst.len() > 0);
        try std.testing.expect(dstMemTable.streamWriter.metaIndexDst.len() > 0);
        try std.testing.expect(dstMemTable.streamWriter.columnsHeaderIndexDst.len() > 0);
        try std.testing.expect(dstMemTable.streamWriter.columnsHeaderDst.len() > 0);
        try std.testing.expect(dstMemTable.streamWriter.timestampsDst.len() > 0);

        break :blk dstMemTable;
    };
    defer mergedMemTable.deinit(alloc);

    var mergedReader = try BlockReader.initFromMemTable(alloc, mergedMemTable);
    defer mergedReader.deinit(alloc);

    const unpacker = try Unpacker.init(alloc);
    defer unpacker.deinit(alloc);
    const decoder = try ValuesDecoder.init(alloc);
    defer decoder.deinit();

    var expectedI: usize = 0;
    while (try mergedReader.nextBlock(alloc)) {
        const block = try Block.initFromData(alloc, &mergedReader.blockData, unpacker, decoder);
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
