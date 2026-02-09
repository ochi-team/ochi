const std = @import("std");
const Allocator = std.mem.Allocator;

const builtin = @import("builtin");

const Conf = @import("../../Conf.zig");

const MemBlock = @import("MemBlock.zig");

const EntriesShard = struct {
    mx: std.Thread.Mutex,
    blocks: std.ArrayList(*MemBlock),
    flushAtUs: i64,

    // TODO: init EntriesShard and its blocks with maxBlocks capacity

    pub fn add(
        self: *EntriesShard,
        alloc: Allocator,
        entries: [][]const u8,
        maxMemBlockSize: u32,
    ) !?std.ArrayList(*MemBlock) {
        self.mx.lock();
        defer self.mx.unlock();

        if (self.blocks.items.len == 0) {
            const b = try MemBlock.init(alloc, maxMemBlockSize);
            try self.blocks.append(alloc, b);
            self.flushAtUs = std.time.microTimestamp() + std.time.us_per_s;
        }

        var block = self.blocks.items[self.blocks.items.len - 1];

        for (entries) |entry| {
            if (block.add(entry)) continue;

            // Skip too long item
            if (entry.len > maxMemBlockSize) {
                var logPrefix = entry;
                if (logPrefix.len > 32) {
                    logPrefix = logPrefix[0..32];
                }
                std.debug.print(
                    "skip adding item to index, must not exceed {d} bytes, given={d}, value={s}\n",
                    .{ maxMemBlockSize, entry.len, logPrefix },
                );
                continue;
            }

            // TODO: throttle max blocks per entries shard,
            // instead of creating new block it has to return the unprocessed entries

            // if it didn't skip the block means the previous one has not enough space
            block = try MemBlock.init(alloc, maxMemBlockSize);
            try self.blocks.append(alloc, block);

            const ok = block.add(entry);
            if (builtin.is_test) {
                std.debug.assert(ok);
            }
        }

        if (self.blocks.items.len >= maxBlocksPerShard) {
            // TODO: test if its worth returning the origin array instead of the copy
            // so the caller could clear its capacity having no need to allocate one more same array
            // OR preallocate a pool of such arrays in a single segment
            const blocksToFlush = self.blocks;
            const freshBlocks = try std.ArrayList(*MemBlock).initCapacity(alloc, maxBlocksPerShard);

            self.blocks = freshBlocks;
            return blocksToFlush;
        }

        return null;
    }
};

pub const maxBlocksPerShard = 256;

const Entries = @This();

shardIdx: std.atomic.Value(usize) = std.atomic.Value(usize).init(0),
shards: []EntriesShard,

pub fn init(alloc: Allocator) !*Entries {
    const conf = Conf.getConf().server.pools;
    std.debug.assert(conf.cpus != 0);

    const shards = try alloc.alloc(EntriesShard, conf.cpus);
    errdefer alloc.free(shards);

    const e = try alloc.create(Entries);
    e.* = .{
        .shards = shards,
    };
    return e;
}

pub fn next(self: *Entries) *EntriesShard {
    const i = self.shardIdx.fetchAdd(1, .acquire) % self.shards.len;
    return &self.shards[i];
}

pub fn deinit(self: *Entries, alloc: Allocator) void {
    alloc.free(self.shards);
    alloc.destroy(self);
}

const testing = std.testing;

test "Entries.shardIdxOverflow" {
    const alloc = testing.allocator;
    _ = try Conf.default();

    const e = try Entries.init(alloc);
    defer e.deinit(alloc);
    e.shardIdx = .init(std.math.maxInt(usize));
    try std.testing.expectEqual(e.shardIdx.load(.acquire), std.math.maxInt(usize));

    _ = e.next();
    try std.testing.expectEqual(e.shardIdx.load(.acquire), 0);

    // it fetches the value first, then increments,
    // therefore on it returns zero's shard and has value 1
    const shard = e.next();
    const firstShard = &e.shards[0];
    try std.testing.expectEqual(e.shardIdx.load(.acquire), 1);
    try std.testing.expectEqual(shard, firstShard);
}

test "EntriesShard.flushesAllBlocksIncludingExtras" {
    const alloc = testing.allocator;
    const maxIndexMemBlockSize = 1024;

    var shard = EntriesShard{
        .mx = .{},
        .blocks = std.ArrayList(*MemBlock).empty,
        .flushAtUs = 0,
    };
    defer {
        for (shard.blocks.items) |b| b.deinit(alloc);
        shard.blocks.deinit(alloc);
    }

    // Setup: create maxBlocksPerShard blocks with last one full
    for (0..maxBlocksPerShard) |_| {
        const b = try MemBlock.init(alloc, maxIndexMemBlockSize);
        try shard.blocks.append(alloc, b);
    }
    const lastBlock = shard.blocks.items[shard.blocks.items.len - 1];
    const filler = "y" ** 100;
    while (lastBlock.size + filler.len <= maxIndexMemBlockSize) {
        _ = lastBlock.add(filler);
    }
    try testing.expectEqual(maxBlocksPerShard, shard.blocks.items.len);
    const remainingSpace = maxIndexMemBlockSize - lastBlock.size;

    // Add entry that doesn't fit - will create block #257
    const entry_buf = try alloc.alloc(u8, remainingSpace + 1);
    defer alloc.free(entry_buf);
    @memset(entry_buf, 'x');
    var entries = [_][]const u8{entry_buf};

    // add() processes ALL entries even if it temporarily exceeds maxBlocksPerShard,
    // then flushes everything including the extra blocks
    const result = try shard.add(alloc, &entries, maxIndexMemBlockSize);

    try testing.expect(result != null);
    var flushed = result.?;

    // Should flush all blocks including the one created during processing
    try testing.expectEqual(maxBlocksPerShard + 1, flushed.items.len);

    // Shard should have fresh empty array
    try testing.expectEqual(0, shard.blocks.items.len);

    // cleanup
    for (flushed.items) |b| b.deinit(alloc);
    flushed.deinit(alloc);
}

test "EntriesShard.add" {
    const maxIndexMemBlockSize = 1024;
    const alloc = testing.allocator;
    const tooLarge = "x" ** (maxIndexMemBlockSize + 1);
    const theLargest = "x" ** (maxIndexMemBlockSize - 1);

    const Case = struct {
        fill_block_after_setup: bool = false,
        setup_blocks_count: usize = 0,
        test_entries: []const []const u8,
        expected_flush: bool = false,
        expected_block_count: usize,
        expected_last_block_entries: usize,
    };

    const cases = [_]Case{
        // normal entries added successfully
        .{
            .test_entries = &.{ "first_normal", "second_normal" },
            .expected_flush = false,
            .expected_block_count = 1,
            .expected_last_block_entries = 2,
        },
        // only too large entry creates two empty blocks
        .{
            .test_entries = &.{tooLarge},
            .expected_flush = false,
            .expected_block_count = 1,
            .expected_last_block_entries = 0,
        },
        // no flush when at threshold-1 with small entry that fits
        .{
            .setup_blocks_count = maxBlocksPerShard - 1,
            .fill_block_after_setup = true,
            .test_entries = &.{"fits_in_remaining_space"},
            .expected_flush = false,
            .expected_block_count = maxBlocksPerShard - 1,
            .expected_last_block_entries = 2, // filled + 1 new entry
        },
        // flush when at threshold-1 with large entry that doesn't fit
        .{
            .setup_blocks_count = maxBlocksPerShard - 1,
            .fill_block_after_setup = true,
            .test_entries = &.{theLargest},
            .expected_flush = true,
            .expected_block_count = 0,
            .expected_last_block_entries = 0,
        },
        // flush when already at threshold (entry goes into flushed blocks)
        .{
            .setup_blocks_count = maxBlocksPerShard,
            .test_entries = &.{"trigger_immediate_flush"},
            .expected_flush = true,
            .expected_block_count = 0,
            .expected_last_block_entries = 0,
        },
        // no flush when below threshold (entry fits in last block)
        .{
            .setup_blocks_count = maxBlocksPerShard - 2,
            .test_entries = &.{"no_flush"},
            .expected_flush = false,
            .expected_block_count = maxBlocksPerShard - 2,
            .expected_last_block_entries = 1,
        },
    };

    for (cases) |case| {
        var shard = EntriesShard{
            .mx = .{},
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
                const b = try MemBlock.init(alloc, maxIndexMemBlockSize);
                try shard.blocks.append(alloc, b);
            }
        }

        if (case.fill_block_after_setup and shard.blocks.items.len > 0) {
            const block = shard.blocks.items[shard.blocks.items.len - 1];
            const filler = "y" ** (maxIndexMemBlockSize - 100);
            while (block.size + filler.len <= maxIndexMemBlockSize) {
                _ = block.add(filler);
            }
        }

        const result = try shard.add(alloc, @constCast(case.test_entries), maxIndexMemBlockSize);

        // Check if flush happened as expected
        if (case.expected_flush) {
            try testing.expect(result != null);
            var flushed = result.?;
            defer {
                for (flushed.items) |b| b.deinit(alloc);
                flushed.deinit(alloc);
            }
        } else {
            try testing.expect(result == null);
        }

        try testing.expectEqual(case.expected_block_count, shard.blocks.items.len);
        if (shard.blocks.items.len > 0) {
            const lastBlock = shard.blocks.items[shard.blocks.items.len - 1];
            try testing.expectEqual(case.expected_last_block_entries, lastBlock.items.items.len);
        }
    }
}
