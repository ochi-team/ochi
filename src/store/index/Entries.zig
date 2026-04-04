const std = @import("std");
const Allocator = std.mem.Allocator;

const builtin = @import("builtin");

const MemBlock = @import("MemBlock.zig");

const EntriesShardAddResult = struct {
    blocksToFlush: std.ArrayList(*MemBlock),
    gatheredEntriesCount: usize,
};

const EntriesShard = struct {
    mx: std.Thread.Mutex = .{},
    blocks: std.ArrayList(*MemBlock),
    // TODO: perhaps worth making it atomic instead of accessable under a mutex lock
    flushAtUs: i64 = std.math.maxInt(i64),

    pub fn init(alloc: Allocator, blocksCap: usize) !EntriesShard {
        return .{
            .blocks = try .initCapacity(alloc, blocksCap),
        };
    }

    pub fn deinit(self: *EntriesShard, alloc: Allocator) void {
        for (self.blocks.items) |block| {
            // TODO: find a way to assert there are no items, this is critical,
            // we must assert on shutdown path it's empty
            block.deinit(alloc);
        }

        self.blocks.deinit(alloc);
    }

    pub fn add(
        self: *EntriesShard,
        alloc: Allocator,
        entries: [][]const u8,
        maxMemBlockSize: u32,
    ) !?EntriesShardAddResult {
        self.mx.lock();
        defer self.mx.unlock();
        if (self.blocks.items.len == 0) {
            const b = try MemBlock.init(alloc, maxMemBlockSize);
            try self.blocks.append(alloc, b);
            self.flushAtUs = std.time.microTimestamp() + std.time.us_per_s;
        }
        var block = self.blocks.items[self.blocks.items.len - 1];
        var gatheredEntriesCount: usize = 0;

        for (entries) |entry| {
            if (block.add(entry)) {
                gatheredEntriesCount += 1;
                continue;
            }

            if (self.blocks.items.len >= maxBlocksPerShard) {
                break;
            }

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

            // if it didn't skip the block means the previous one has not enough space
            block = try MemBlock.init(alloc, maxMemBlockSize);
            try self.blocks.append(alloc, block);

            gatheredEntriesCount += 1;

            const ok = block.add(entry);
            if (builtin.is_test) {
                std.debug.assert(ok);
            }
        }

        if (self.blocks.items.len >= maxBlocksPerShard) {
            // TODO: test if its worth returning the origin array instead of the copy
            // so the caller could clear its capacity having no need to allocate one more same array
            // OR preallocate a pool of such arrays in a single segment
            const result: EntriesShardAddResult = .{
                .blocksToFlush = self.blocks,
                .gatheredEntriesCount = gatheredEntriesCount,
            };

            self.blocks = try std.ArrayList(*MemBlock).initCapacity(alloc, maxBlocksPerShard);

            return result;
        }

        return null;
    }

    pub fn collectBlocks(
        self: *EntriesShard,
        alloc: Allocator,
        destination: *std.ArrayList(*MemBlock),
        nowUs: i64,
        force: bool,
    ) !void {
        self.mx.lock();
        defer self.mx.unlock();

        if (!force and nowUs < self.flushAtUs) {
            return;
        }

        try destination.appendSlice(alloc, self.blocks.items);
        self.blocks.clearRetainingCapacity();
    }
};

pub const maxBlocksPerShard = 256;

const Entries = @This();

shardIdx: std.atomic.Value(usize) = std.atomic.Value(usize).init(0),
shards: []EntriesShard,

pub fn init(alloc: Allocator, concurrency: u16) !*Entries {
    std.debug.assert(concurrency != 0);

    const shards = try alloc.alloc(EntriesShard, concurrency);
    errdefer alloc.free(shards);

    var initialized: usize = 0;
    errdefer for (shards[0..initialized]) |*shard| shard.deinit(alloc);

    for (shards) |*shard| {
        shard.* = try .init(alloc, maxBlocksPerShard);
        initialized += 1;
    }

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
    for (self.shards) |*shard| {
        shard.deinit(alloc);
    }
    alloc.free(self.shards);
    alloc.destroy(self);
}

const testing = std.testing;

test "Entries.shardIdxOverflow" {
    const alloc = testing.allocator;

    const e = try Entries.init(alloc, 4);
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
            .expected_block_count = 1,
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
            for (flushed.blocksToFlush.items) |b| b.deinit(alloc);
            flushed.blocksToFlush.deinit(alloc);

            try testing.expectEqual(case.expected_gathered_entries_count, flushed.gatheredEntriesCount);
        } else {
            try testing.expect(result == null);
        }

        try testing.expectEqual(case.expected_block_count, shard.blocks.items.len);
        if (shard.blocks.items.len > 0) {
            const lastBlock = shard.blocks.items[shard.blocks.items.len - 1];
            try testing.expectEqual(case.expected_last_block_entries_count, lastBlock.items.items.len);
        }
    }
}
