const std = @import("std");
const Allocator = std.mem.Allocator;
const Io = std.Io;

const tracy = @import("tracy");

const Stop = @import("../../stds/Stop.zig");
const Heap = @import("../../stds/heap.zig").Heap;

const sizing = @import("../data/sizing.zig");

const ColumnData = @import("ColumnData.zig");
const Column = @import("Column.zig");
const TableHeader = @import("../data/TableHeader.zig");
const copyFields = @import("../lines.zig").copyFields;
const SID = @import("../lines.zig").SID;
const Line = @import("../lines.zig").Line;

const TableWriter = @import("../data/TableWriter.zig");
const BlockWriter = @import("../data/BlockWriter.zig");
const Block = @import("../data/Block.zig");
const BlockData = @import("../data/BlockData.zig").BlockData;
const BlockReader = @import("../data/BlockReader.zig");
const Unpacker = @import("../data/Unpacker.zig").Unpacker;
const ValuesDecoder = @import("../data/ValuesDecoder.zig");
const TimestampsEncoder = @import("../data/TimestampsEncoder.zig");
const DecompressionPool = @import("../compression/DecompressionPool.zig");

const Consts = @import("../../Consts.zig");

const maxBlockSize = Consts.maxBlockSize;

pub fn mergeBlocks(
    io: Io,
    alloc: Allocator,
    timestampsEncoders: *TimestampsEncoder.TimestampsEncoderPool,
    decompressionPool: *DecompressionPool,
    writer: *TableWriter,
    readers: *std.ArrayList(*BlockReader),
    stopped: ?*const Stop,
) !TableHeader {
    const z = tracy.Zone.begin(.{
        .src = @src(),
        .name = "DataRecorder.mergeBlocks",
    });
    defer z.end();

    defer writer.close(io);

    var merger = try StreamMerger.init(io, alloc, timestampsEncoders, decompressionPool, readers);
    defer merger.deinit();

    const blockWriter = try BlockWriter.init(alloc);
    defer blockWriter.deinit(alloc);

    while (merger.heap.array.items.len > 0) {
        if (stopped != null and stopped.?.isStopped()) {
            // TODO: test whether break cleans the resources
            return error.Stopped;
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

    sid: SID = .{ .tenantID = 0, .id = 0 },
    totalKeys: usize = 0,
    size: usize = 0,
    lines: std.ArrayList(Line) = .empty,
    mergeBufferLines: std.ArrayList(Line) = .empty,
    linesArena: std.heap.ArenaAllocator,

    // holds a current block copied until
    // either flushed as-is or merged with a second block for the same stream
    block: BlockData = BlockData.initEmpty(),

    // leaky unpacker since we use arena for it
    unpacker: Unpacker(true),
    decoder: ValuesDecoder = .{},
    timestampsEncoders: *TimestampsEncoder.TimestampsEncoderPool,

    /// init creates a StreamMerger instance from the readers
    /// be aware it mutates readers list inside
    pub fn init(
        io: Io,
        alloc: Allocator,
        timestampsEncoders: *TimestampsEncoder.TimestampsEncoderPool,
        decompressionPool: *DecompressionPool,
        readers: *std.ArrayList(*BlockReader),
    ) !StreamMerger {
        // TODO: experiment with Loser tree intead of heap:
        // https://grafana.com/blog/the-loser-tree-data-structure-how-to-optimize-merges-and-make-your-programs-run-faster/

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

        const linesArena: std.heap.ArenaAllocator = .init(alloc);
        errdefer linesArena.deinit();
        return .{
            .heap = heap,
            .unpacker = .init(decompressionPool),
            .linesArena = linesArena,
            .timestampsEncoders = timestampsEncoders,
        };
    }

    fn reset(self: *StreamMerger) void {
        self.totalKeys = 0;
        self.size = 0;
        self.sid = .{ .tenantID = 0, .id = 0 };

        // arena reset invalidates any capacity these held,
        // so drop it rather than clearRetainingCapacity
        self.lines = .empty;
        self.mergeBufferLines = .empty;
        self.decoder.resetArena();
        self.unpacker.resetArena();
        _ = self.linesArena.reset(.retain_capacity);

        self.resetBlock();
    }

    pub fn deinit(self: *StreamMerger) void {
        self.linesArena.deinit();
    }

    fn resetBlock(self: *StreamMerger) void {
        self.block = BlockData.initEmpty();
    }

    pub fn writeBlock(
        self: *StreamMerger,
        io: Io,
        alloc: Allocator,
        blockWriter: *BlockWriter,
        writer: *TableWriter,
        blockData: *BlockData,
    ) !void {
        const z = tracy.Zone.begin(.{
            .src = @src(),
            .name = "StreamMerger.writeBlock",
        });
        defer z.end();

        self.assertState(blockData);

        const totalKeys = blockData.columnsData.items.len +
            if (blockData.invariantColumns) |invariantCol| invariantCol.len else 0;

        if (!blockData.sid.eql(self.sid)) {
            // it means next stream begins, we have to flush the data
            try self.flushStream(io, alloc, blockWriter, writer);
            self.sid = blockData.sid;

            if (blockData.uncompressedSizeBytes >= maxBlockSize) {
                // max block size, flush immediately
                try blockWriter.writeData(io, alloc, blockData, writer);
            } else {
                // copy block to the merger, wait for the next block
                try self.setBlock(blockData);
                self.totalKeys = totalKeys;
            }
        } else if (self.totalKeys + totalKeys > Block.maxColumns) {
            // we have to flush the data before we can add more keys
            try self.flushStream(io, alloc, blockWriter, writer);
            if (totalKeys >= Block.maxColumns) {
                try blockWriter.writeData(io, alloc, blockData, writer);
            } else {
                try self.setBlock(blockData);
                self.totalKeys = totalKeys;
            }
        } else if (blockData.uncompressedSizeBytes >= maxBlockSize) {
            try self.flushStream(io, alloc, blockWriter, writer);
            try blockWriter.writeData(io, alloc, blockData, writer);
        } else {
            try self.merge(io, alloc, blockData, blockWriter, writer);
            self.totalKeys += totalKeys;
        }
    }

    // TODO: this and many more demonstartes obvious dependece of block and stream writers,
    // they always go together, I have to inject one into another probably
    fn flushStream(
        self: *StreamMerger,
        io: Io,
        alloc: Allocator,
        writer: *BlockWriter,
        streamWriter: *TableWriter,
    ) !void {
        if (self.lines.items.len > 0) {
            try writer.writeLines(io, alloc, self.sid, self.lines.items, streamWriter);
        } else if (self.block.len > 0) {
            // never merged with a second block for this stream, write the copy as-is.
            try writer.writeData(io, alloc, &self.block, streamWriter);
        }

        self.reset();
    }

    fn merge(
        self: *StreamMerger,
        io: Io,
        alloc: Allocator,
        blockData: *BlockData,
        blockWriter: *BlockWriter,
        writer: *TableWriter,
    ) !void {
        const z = tracy.Zone.begin(.{
            .src = @src(),
            .name = "StreamMerger.merge",
        });
        defer z.end();

        if (self.block.len > 0) {
            // a single block was held for this stream, unpack it into lines now
            // that a second block forces an actual merge
            try self.decodeLines(io, &self.block);
            self.resetBlock();
        }

        const len = self.lines.items.len;
        try self.decodeLines(io, blockData);
        std.debug.assert(self.lines.items.len > len);

        const linesArena = self.linesArena.allocator();
        try self.mergeBufferLines.ensureTotalCapacity(linesArena, self.lines.items.len);
        defer self.mergeBufferLines.clearRetainingCapacity();

        mergeLines(&self.mergeBufferLines, self.lines.items[0..len], self.lines.items[len..]);
        std.mem.swap(std.ArrayList(Line), &self.mergeBufferLines, &self.lines);

        if (self.size >= maxBlockSize) {
            try self.flushStream(io, alloc, blockWriter, writer);
        }
    }

    // setBlock copies block into self.block so it can be written out later
    // without ever decoding it into lines. columnsHeaderBuf/columnsHeader are not copied.
    // the copy is allocated from linesArena, since self.block shares its lifetime with lines.
    fn setBlock(self: *StreamMerger, block: *const BlockData) !void {
        const alloc = self.linesArena.allocator();

        const timestampsBuf = try alloc.alloc(u8, block.timestampsData.data.len);
        const timestampsData = block.timestampsData.copy(timestampsBuf);

        // self.block.columnsData is reused from a previous setBlock call
        std.debug.assert(self.block.columnsData.items.len == 0);
        try self.block.columnsData.ensureTotalCapacity(alloc, block.columnsData.items.len);
        errdefer self.block.columnsData.clearRetainingCapacity();

        for (block.columnsData.items) |*src| {
            var dst: ColumnData = undefined;
            try src.copy(alloc, &dst);
            self.block.columnsData.appendAssumeCapacity(dst);
        }

        var invariantColumns: ?[]Column = null;
        if (block.invariantColumns) |cols| {
            const dup = try alloc.alloc(Column, cols.len);
            for (cols, 0..) |col, i| {
                const key = try alloc.dupe(u8, col.key);
                const values = try alloc.alloc([]const u8, col.values.len);
                for (col.values, 0..) |v, j| {
                    const value = try alloc.dupe(u8, v);
                    values[j] = value;
                }

                dup[i] = .{ .key = key, .values = values };
            }
            invariantColumns = dup;
        }

        self.block = .{
            .sid = block.sid,
            .uncompressedSizeBytes = block.uncompressedSizeBytes,
            .len = block.len,
            .timestampsData = timestampsData,
            .invariantColumns = invariantColumns,
            .columnsData = self.block.columnsData,
        };
    }

    pub fn decodeLines(self: *StreamMerger, io: Io, blockData: *BlockData) !void {
        const z = tracy.Zone.begin(.{
            .src = @src(),
            .name = "StreamMerger.decodeLines",
        });
        defer z.end();

        const alloc = self.linesArena.allocator();
        const block = try Block.initFromData(
            io,
            alloc,
            self.timestampsEncoders,
            blockData,
            true,
            &self.unpacker,
            &self.decoder,
        );

        const offset = self.lines.items.len;
        try block.gatherLines(alloc, &self.lines);

        for (offset..self.lines.items.len) |lineI| {
            const fields = self.lines.items[lineI].fields;
            // data is short living, so we need to copy key values buffers,
            // TODO: we may move field array instead of copying it, do it for every copyFields usage
            const copiedFields = try copyFields(alloc, fields);
            self.lines.items[lineI].fields = copiedFields;
        }

        // TODO: understand whether I can use sizing.blockJsonSize,
        // (test is implemented to confirm it, good to have it for merger),
        // then understand whether I can use blockData.uncompressedSizeBytes
        self.size += sizing.linesJsonSize(self.lines.items[offset..]);
    }

    fn assertState(self: *const StreamMerger, data: *const BlockData) void {
        // expected empty block if the lines are not processed yet
        if (self.lines.items.len > 0) std.debug.assert(self.block.len == 0);
        std.debug.assert(!data.sid.lessThan(self.sid));

        if (!data.sid.eql(self.sid)) return;
        if (data.len == 0) return;

        if (self.lines.items.len == 0) {
            if (self.block.len == 0) return;

            std.debug.assert(data.timestampsData.minTimestamp >= self.block.timestampsData.minTimestamp);
            return;
        }

        std.debug.assert(data.timestampsData.minTimestamp >= self.lines.items[0].timestampNs);
    }
};

/// expects dst as a preallocated array,
/// merges left and right into dst.
/// It uses intermediate buffer dst in order to reduce merge complexity
/// TODO: make it in-place no copy?
pub fn mergeLines(dst: *std.ArrayList(Line), left: []const Line, right: []const Line) void {
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
