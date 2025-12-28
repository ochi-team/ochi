const std = @import("std");
const Allocator = std.mem.Allocator;

const getConf = @import("conf.zig").getConf;

// TODO: make it configurable,
// depending on used CPU model must be changed according its L1 cache size
const maxMemBlockSize = 32 * 1024;

const MemBlock = struct {
    data: std.ArrayList(u8),
    ends: std.ArrayList(u32),

    pub fn add(self: *MemBlock, alloc: Allocator, entry: []const u8) !bool {
        if ((entry.len + self.data.items.len) > maxMemBlockSize) return false;

        try self.data.appendSlice(alloc, entry);
        try self.ends.append(alloc, @intCast(self.data.items.len));
        return true;
    }
};

const EntriesShard = struct {
    mx: std.Thread.Mutex,
    blocks: std.ArrayList(*MemBlock),
    flushAtUs: i64,

    pub fn add(self: *EntriesShard, alloc: Allocator, entries: [][]const u8) ![]*MemBlock {
        self.mx.lock();
        defer self.mx.unlock();

        // TODO: throttle max block per entries shard

        if (self.blocks.items.len == 0) {
            const b = try alloc.create(MemBlock);
            try self.blocks.append(alloc, b);
            self.flushAtUs = std.time.microTimestamp() + std.time.us_per_s;
        }

        var block = self.blocks.items[self.blocks.items.len - 1];

        for (entries) |entry| {
            if (try block.add(alloc, entry)) continue;

            block = try alloc.create(MemBlock);
            if (try block.add(alloc, entry)) {
                try self.blocks.append(alloc, block);
            }

            // TODO: handle too large entries, log an error
        }

        return self.blocks.items;
    }
};

const Entries = struct {
    shardIdx: std.atomic.Value(usize) = std.atomic.Value(usize).init(0),
    shards: []EntriesShard,

    pub fn init(alloc: Allocator) !*Entries {
        const conf = getConf().server.pools;
        std.debug.assert(conf.cpus != 0);

        const shards = try alloc.alloc(EntriesShard, conf.cpus);
        errdefer alloc.free(shards);

        const e = try alloc.create(Entries);
        e.* = .{
            .shards = shards,
        };
        return e;
    }

    pub fn deinit(self: *Entries, alloc: Allocator) void {
        alloc.free(self.shards);
        alloc.destroy(self);
    }
};

const Self = @This();

flushInterval: u64,
entries: *Entries,

pub fn init(alloc: Allocator, flushInterval: u64) !*Self {
    const entries = try Entries.init(alloc);
    errdefer entries.deinit(alloc);

    const t = try alloc.create(Self);
    t.* = .{
        .flushInterval = flushInterval,
        .entries = entries,
    };
    return t;
}

pub fn add(self: *Self, alloc: Allocator, entries: [][]const u8) !void {
    const i = self.entries.shardIdx.fetchAdd(1, .acquire) % self.entries.shards.len;
    var shard = &self.entries.shards[i];

    const blocks = try shard.add(alloc, entries);
    if (blocks.len == 0) return;
    self.flushBlocks(blocks);
}

fn flushBlocks(self: *Self, blocks: []*MemBlock) void {
    _ = self;
    _ = blocks;
    unreachable;
}
