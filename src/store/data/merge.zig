const std = @import("std");
const Allocator = std.mem.Allocator;

const Heap = @import("../../stds/heap.zig").Heap;

const TableHeader = @import("../inmem/TableHeader.zig");

const StreamWriter = @import("../inmem/StreamWriter.zig");
const BlockData = @import("../inmem/BlockData.zig").BlockData;
const BlockReader = @import("../inmem/reader.zig").BlockReader;

// TODO: rename this crap
pub fn mergeData(
    alloc: Allocator,
    tablePath: []const u8,
    writer: *StreamWriter,
    readers: *std.ArrayList(*BlockReader),
    stopped: ?*const std.atomic.Value(bool),
) !TableHeader {
    var merger = try StreamMerger.init(alloc, readers);

    while (merger.heap.array.items.len > 0) {
        if (stopped) |stop| {
            if (stop.load(.acquire)) {
                // TODO: test whether simple break and merging what we have won't hurt
                return error.Stopped;
            }
        }

        const reader = merger.heap.peek().?;
        try merger.writeBlock(alloc, writer, reader.blockData);
        // const block = reader.blockData;
    }

    _ = tablePath;
    unreachable;
}

pub const StreamMerger = struct {
    heap: Heap(*BlockReader, BlockReader.blockReaderLessThan),

    /// init creates a StreamMerger instance from the readers
    /// be aware it mutates readers list inside
    pub fn init(alloc: Allocator, readers: *std.ArrayList(*BlockReader)) !StreamMerger {
        // TODO: collect metrics and experiment with flat array on 1-3 elements

        // TODO: experiment with Loser tree intead of heap:
        // https://grafana.com/blog/the-loser-tree-data-structure-how-to-optimize-merges-and-make-your-programs-run-faster/

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
        };
    }

    pub fn writeBlock(
        self: *const StreamMerger,
        alloc: Allocator,
        writer: *StreamWriter,
        blockData: BlockData,
    ) !void {
        _ = self;
        _ = alloc;
        _ = writer;
        _ = blockData;
        unreachable;
    }
};
