const std = @import("std");
const Allocator = std.mem.Allocator;

const getConf = @import("conf.zig").getConf;

// TODO: make it configurable,
// depending on used CPU model must be changed according its L1 cache size
const maxMemBlockSize = 32 * 1024;

const maxBlocksPerShard = 256;

// TODO: worth tuning on practice
const blocksInMemTable = 15;

const MemBlock = struct {
    data: std.ArrayList([]const u8),
    size: u32,

    pub fn add(self: *MemBlock, alloc: Allocator, entry: []const u8) !bool {
        if ((self.size + entry.len) > maxMemBlockSize) return false;

        try self.data.append(alloc, entry);
        self.size += @intCast(entry.len);
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

    pub fn next(self: *Entries) *EntriesShard {
        const i = self.shardIdx.fetchAdd(1, .acquire) % self.shards.len;
        return &self.shards[i];
    }

    pub fn deinit(self: *Entries, alloc: Allocator) void {
        alloc.free(self.shards);
        alloc.destroy(self);
    }
};

const Self = @This();

flushInterval: u64,
entries: *Entries,

blocks: std.ArrayList(*MemBlock) = .empty,
mxBlocks: std.Thread.Mutex = .{},
flushAtUs: ?i64 = null,

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
    const shard = self.entries.next();
    const blocks = try shard.add(alloc, entries);
    if (blocks.len == 0) return;
    try self.flushBlocks(alloc, blocks);
}

fn flushBlocks(self: *Self, alloc: Allocator, blocks: []*MemBlock) !void {
    self.mxBlocks.lock();
    defer self.mxBlocks.unlock();

    if (self.blocks.items.len == 0) {
        self.flushAtUs = std.time.microTimestamp() + std.time.us_per_s;
    }

    try self.blocks.appendSlice(alloc, blocks);
    if (self.blocks.items.len >= maxBlocksPerShard * self.entries.shards.len) {
        try self.flush(alloc, self.blocks.items, false);
        self.blocks.clearRetainingCapacity();
    }
}

fn flush(self: *Self, alloc: Allocator, blocks: []*MemBlock, force: bool) !void {
    const tablesSize = (blocks.len + blocksInMemTable - 1) / blocksInMemTable;
    var res = try std.ArrayList(*MemTable).initCapacity(alloc, tablesSize);
    errdefer {
        for (res.items) |memTable| memTable.deinit(alloc);
        res.deinit(alloc);
    }

    var tail = blocks[0..];
    // TODO: benchmark parallel mem table creation
    while (tail.len > 0) {
        const offset = @min(blocksInMemTable, tail.len);
        const head = tail[0..offset];
        tail = tail[offset..];

        const memTable = try MemTable.init(alloc, head);
        res.appendAssumeCapacity(memTable);
    }

    _ = self;
    _ = force;
}

const MemTable = struct {
    pub fn init(alloc: Allocator, blocks: []*MemBlock) !*MemTable {
        const readers = try alloc.alloc(*BlockReader, blocks.len);
        var lastInit: usize = 0;
        errdefer {
            for (readers[0..lastInit]) |reader| reader.deinit(alloc);
            alloc.free(readers);
        }

        for (0..blocks.len) |i| {
            const reader = try BlockReader.init(alloc, blocks[i]);
            readers[i] = reader;
            lastInit = i + 1;
        }

        const t = try alloc.create(MemTable);
        t.* = .{};
        return t;
    }

    pub fn deinit(self: *MemTable, alloc: Allocator) void {
        alloc.destroy(self);
    }
};

const BlockReader = struct {
    pub fn init(alloc: Allocator, block: *MemBlock) !*BlockReader {
        _ = block;
        const r = try alloc.create(BlockReader);
        r.* = .{};
        return r;
    }

    pub fn deinit(self: *BlockReader, alloc: Allocator) void {
        alloc.destroy(self);
    }
};
