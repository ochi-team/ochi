const std = @import("std");
const Allocator = std.mem.Allocator;

const Heap = @import("../../stds/heap.zig").Heap;

const sizing = @import("../inmem/sizing.zig");

const TableHeader = @import("../inmem/TableHeader.zig");
const SID = @import("../lines.zig").SID;
const Line = @import("../lines.zig").Line2;

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
    tablePath: []const u8,
    writer: *StreamWriter,
    readers: *std.ArrayList(*BlockReader),
    stopped: ?*const std.atomic.Value(bool),
) !TableHeader {
    var merger = try StreamMerger.init(alloc, readers);
    const blockWriter = try BlockWriter.init(alloc);
    defer blockWriter.deinit(alloc);

    while (merger.heap.array.items.len > 0) {
        if (stopped) |stop| {
            if (stop.load(.acquire)) {
                // TODO: test whether simple break and merging what we have won't hurt
                return error.Stopped;
            }
        }

        const reader = merger.heap.peek().?;
        try merger.writeBlock(alloc, blockWriter, writer, &reader.blockData);
        // const block = reader.blockData;
    }

    _ = tablePath;
    unreachable;
}

pub const StreamMerger = struct {
    heap: Heap(*BlockReader, BlockReader.blockReaderLessThan),

    // state
    sid: SID = .{ .tenantID = "", .id = 0 },
    blockData: ?*BlockData = null,
    totalKeys: usize = 0,
    size: usize = 0,
    lines: std.ArrayList(Line) = .empty,

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
            self.flushStream();
            self.sid = blockData.sid;

            if (blockData.uncompressedSizeBytes >= MemTable.maxBlockSize) {
                try blockWriter.writeData(alloc, blockData, writer);
            } else {
                self.blockData = blockData;
                self.totalKeys = totalKeys;
            }
        } else if (self.totalKeys + totalKeys > Block.maxColumns) {
            // we have to flush the data before we can add more
            self.flushStream();
            if (totalKeys > Block.maxColumns) {
                try blockWriter.writeData(alloc, blockData, writer);
            } else {
                self.blockData = blockData;
                self.totalKeys = totalKeys;
            }
        } else if (self.size >= MemTable.maxBlockSize) {
            self.flushStream();
            try blockWriter.writeData(alloc, blockData, writer);
        } else {
            try self.merge(alloc, blockData);
            self.totalKeys += totalKeys;
        }
    }

    fn flushStream(self: *const StreamMerger) void {
        _ = self;
        unreachable;
    }

    fn merge(self: *StreamMerger, alloc: Allocator, blockData: *BlockData) !void {
        if (self.blockData) |current| {
            if (current.len > 0) {
                try self.decodeLines(alloc, current);
                current.reset(alloc);
            }
        }
        _ = blockData;
        unreachable;
    }

    fn decodeLines(self: *StreamMerger, alloc: Allocator, blockData: *BlockData) !void {
        const block = try Block.initFromData(alloc, blockData, self.unpacker, self.decoder);
        defer block.deinit(alloc);

        const offset = self.lines.items.len;
        block.gatherLines(&self.lines);

        // TODO: understand whether I can use sizing.blockJsonSize,
        // (test is implemented to confirm it, good to have it for merger),
        // then understand whether I can use blockData.uncompressedSizeBytes
        self.size += sizing.linesJsonSize(self.lines.items[offset..]);
    }
};
