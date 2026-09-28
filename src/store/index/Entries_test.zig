const std = @import("std");

const MemBlock = @import("MemBlock.zig");

const Entries = @import("Entries.zig");
const EntriesShard = Entries.EntriesShard;
const maxBlocksPerShard = Entries.maxBlocksPerShard;

// no point to index larger entries
pub const maxEntrySize = 1024;

const testing = std.testing;

test "Entries.shardIdxOverflow" {
    const alloc = testing.allocator;

    const e = try Entries.init(alloc, 4);
    defer e.deinit(alloc);
    e.shardIdx = .init(std.math.maxInt(usize));
    try testing.expectEqual(e.shardIdx.load(.monotonic), std.math.maxInt(usize));

    _ = e.next();
    try testing.expectEqual(e.shardIdx.load(.monotonic), 0);

    // it fetches the value first, then increments,
    // therefore on it returns zero's shard and has value 1
    const shard = e.next();
    const firstShard = &e.shards[0];
    try testing.expectEqual(e.shardIdx.load(.monotonic), 1);
    try testing.expectEqual(shard, firstShard);
}

test "EntriesShard.add" {
    const maxIndexMemBlockSize = 1024;
    const alloc = testing.allocator;
    const io = testing.io;
    const tooLarge = "x" ** (maxIndexMemBlockSize + 1);
    const theLargest = "x" ** (maxIndexMemBlockSize - 1);

    const Case = struct {
        fill_block_after_setup: bool = false,
        setup_blocks_count: usize = 0,
        test_entries: []const []const u8,
        expected_flush: bool = false,
        expected_block_count: usize,
        expected_last_block_entries_count: usize,
        expected_gathered_entries_count: usize = 0,
    };

    const cases = [_]Case{
        //normal entries added successfully
        .{
            .test_entries = &.{ "first_normal", "second_normal" },
            .expected_flush = false,
            .expected_block_count = 1,
            .expected_last_block_entries_count = 2,
        },
        // only too large entry creates two empty blocks
        .{
            .test_entries = &.{tooLarge},
            .expected_flush = false,
            .expected_block_count = 0,
            .expected_last_block_entries_count = 0,
        },
        // no flush when at threshold-1 with small entry that fits
        .{
            .setup_blocks_count = maxBlocksPerShard - 1,
            .fill_block_after_setup = true,
            .test_entries = &.{"fits_in_remaining_space"},
            .expected_flush = false,
            .expected_block_count = maxBlocksPerShard - 1,
            .expected_last_block_entries_count = 2, // filled + 1 new entry
        },
        // flush when at threshold-1 with large entry that doesn't fit
        .{
            .setup_blocks_count = maxBlocksPerShard - 1,
            .fill_block_after_setup = true,
            .test_entries = &.{ theLargest, theLargest },
            .expected_flush = true,
            .expected_block_count = 0,
            .expected_last_block_entries_count = 0,
            .expected_gathered_entries_count = 1,
        },
        // flush when already at threshold (entry goes into flushed blocks)
        .{
            .setup_blocks_count = maxBlocksPerShard,
            .test_entries = &.{"trigger_immediate_flush"},
            .expected_flush = true,
            .expected_block_count = 0,
            .expected_last_block_entries_count = 0,
            .expected_gathered_entries_count = 1,
        },
        // no flush when below threshold (entry fits in last block)
        .{
            .setup_blocks_count = maxBlocksPerShard - 2,
            .test_entries = &.{"no_flush"},
            .expected_flush = false,
            .expected_block_count = maxBlocksPerShard - 2,
            .expected_last_block_entries_count = 1,
        },
        // flush and check gathered_entries_count always = maxBlocksPerShard
        .{
            .setup_blocks_count = 0,
            .test_entries = &([_][]const u8{theLargest} ** (maxBlocksPerShard + 1)),
            .expected_flush = true,
            .expected_block_count = 0,
            .expected_last_block_entries_count = 0,
            .expected_gathered_entries_count = maxBlocksPerShard,
        },
    };

    for (cases) |case| {
        var shard = EntriesShard{
            .mx = .init,
            .blocks = std.ArrayList(*MemBlock).empty,
            .flushAtUs = 0,
        };
        defer {
            for (shard.blocks.items) |b| b.deinit(alloc);
            shard.blocks.deinit(alloc);
        }

        // Setup blocks if specified
        if (case.setup_blocks_count > 0) {
            for (0..case.setup_blocks_count) |_| {
                const b = try MemBlock.init(alloc, .{
                    .maxMemBlockSize = maxIndexMemBlockSize,
                    .blocksCountHint = 2,
                });
                try shard.blocks.append(alloc, b);
            }
        }

        if (case.fill_block_after_setup and shard.blocks.items.len > 0) {
            const block = shard.blocks.items[shard.blocks.items.len - 1];
            const filler = "y" ** (maxIndexMemBlockSize - 100);
            while (block.buf.items.len + filler.len <= maxIndexMemBlockSize) {
                _ = block.add(filler);
            }
        }

        const result = try shard.add(io, alloc, case.test_entries, maxIndexMemBlockSize);

        // Check if flush happened as expected
        if (case.expected_flush) {
            try testing.expect(result != null);
            var flushed = result.?;
            for (flushed.blocksToFlush.items) |b| b.deinit(alloc);
            flushed.blocksToFlush.deinit(alloc);

            try testing.expectEqual(case.expected_gathered_entries_count, flushed.gatheredEntriesCount);
        } else {
            try testing.expect(result == null);
        }

        try testing.expectEqual(case.expected_block_count, shard.blocks.items.len);
        if (shard.blocks.items.len > 0) {
            const lastBlock = shard.blocks.items[shard.blocks.items.len - 1];
            try testing.expectEqual(case.expected_last_block_entries_count, lastBlock.memEntries.items.len);
        }
    }
}
