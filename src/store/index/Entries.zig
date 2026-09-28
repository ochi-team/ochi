const std = @import("std");
const Allocator = std.mem.Allocator;
const Io = std.Io;

const tracy = @import("tracy");

const MemBlock = @import("MemBlock.zig");
const Logger = @import("logging");

// no point to index larger entries
pub const maxEntrySize = 1024;

const EntriesShardAddResult = struct {
    blocksToFlush: std.ArrayList(*MemBlock),
    gatheredEntriesCount: usize,
};

pub const EntriesShard = struct {
    mx: Io.Mutex = .init,
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
            block.deinit(alloc);
        }

        self.blocks.deinit(alloc);
    }

    pub fn add(
        self: *EntriesShard,
        io: Io,
        alloc: Allocator,
        entries: []const []const u8,
        maxMemBlockSize: u32,
    ) !?EntriesShardAddResult {
        const z = tracy.Zone.begin(.{
            .src = @src(),
            .name = "EntriesShard.add",
        });
        defer z.end();

        // an entry allowed through must still fit a freshly created block,
        // whose capacity is maxMemBlockSize
        const effectiveMaxEntrySize = @min(maxEntrySize, maxMemBlockSize);

        self.mx.lockUncancelable(io);
        defer self.mx.unlock(io);

        var validBlockI: usize = 0;
        if (self.blocks.items.len == 0) {
            var firstValid: usize = entries.len;
            for (0..entries.len) |i| {
                if (entries[i].len <= effectiveMaxEntrySize) {
                    firstValid = i;
                    break;
                }
            }

            if (firstValid == entries.len) {
                Logger.log(.warn, "skip adding items to index block", .{
                    .items = entries.len,
                    .maxSize = effectiveMaxEntrySize,
                });
                return null;
            }

            // we identify whether we need to create a new block for the upcoming entry,
            // during the flush we must end up either with no blocks,
            // or with blocks having a content,
            // passing the data without validation makes us create an empty block in advance,
            // but it's a dangling block does nothing
            validBlockI = firstValid;
            try self.blocks.ensureUnusedCapacity(alloc, 1);
            const b = try MemBlock.init(alloc, .{ .maxMemBlockSize = maxMemBlockSize });
            self.blocks.appendAssumeCapacity(b);
            self.flushAtUs = Io.Timestamp.now(io, .real).toMicroseconds() + std.time.us_per_s;
        }

        var block = self.blocks.items[self.blocks.items.len - 1];
        var gatheredEntriesCount: usize = 0;

        for (entries[validBlockI..]) |entry| {
            // Skip too long item
            if (entry.len > effectiveMaxEntrySize) {
                var logPrefix = entry;
                if (logPrefix.len > 32) {
                    logPrefix = logPrefix[0..32];
                }
                Logger.log(.warn, "skip adding item to index, item is too large", .{
                    .maxSize = effectiveMaxEntrySize,
                    .given = entry.len,
                    .value = logPrefix,
                });
                continue;
            }

            if (block.add(entry)) {
                gatheredEntriesCount += 1;
                continue;
            }

            if (self.blocks.items.len >= maxBlocksPerShard) {
                break;
            }

            try self.blocks.ensureUnusedCapacity(alloc, 1);

            // if it didn't skip the block means the previous one has not enough space
            block = try MemBlock.init(alloc, .{ .maxMemBlockSize = maxMemBlockSize });
            self.blocks.appendAssumeCapacity(block);

            gatheredEntriesCount += 1;

            const ok = block.add(entry);
            // fresh block must have enough space
            std.debug.assert(ok);
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
        io: Io,
        alloc: Allocator,
        destination: *std.ArrayList(*MemBlock),
        nowUs: i64,
        force: bool,
    ) !void {
        self.mx.lockUncancelable(io);
        defer self.mx.unlock(io);

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
    const i = self.shardIdx.fetchAdd(1, .monotonic) % self.shards.len;
    return &self.shards[i];
}

pub fn deinit(self: *Entries, alloc: Allocator) void {
    for (self.shards) |*shard| {
        shard.deinit(alloc);
    }
    alloc.free(self.shards);
    alloc.destroy(self);
}

test {
    _ = @import("Entries_test.zig");
}
