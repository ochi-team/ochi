const std = @import("std");
const Allocator = std.mem.Allocator;

const Entries = @import("Entries.zig");
const MemBlock = @import("MemBlock.zig");
const MemTable = @import("MemTable.zig");

const Conf = @import("../../Conf.zig");

const maxBlocksPerShard = 256;

// TODO: worth tuning on practice
const blocksInMemTable = 15;

const Self = @This();

flushInterval: u64,
entries: *Entries,

blocksToFlush: std.ArrayList(*MemBlock),
mxBlocks: std.Thread.Mutex = .{},
flushAtUs: ?i64 = null,
blocksThresholdToFlush: u32,

// config fields
maxIndexBlockSize: u32,

pub fn init(alloc: Allocator, flushInterval: u64) !*Self {
    const entries = try Entries.init(alloc);
    errdefer entries.deinit(alloc);

    const blocksThresholdToFlush: u32 = @intCast(entries.shards.len * maxBlocksPerShard);

    // TODO: try using list of lists instead in order not to copy data from blocks to blocksToFlush
    var blocksToFlush = try std.ArrayList(*MemBlock).initCapacity(alloc, blocksThresholdToFlush);
    errdefer blocksToFlush.deinit(alloc);

    const t = try alloc.create(Self);
    t.* = .{
        .flushInterval = flushInterval,
        .entries = entries,
        .blocksThresholdToFlush = @intCast(entries.shards.len * maxBlocksPerShard),
        .blocksToFlush = blocksToFlush,
        .maxIndexBlockSize = Conf.getConf().app.maxIndexMemBlockSize,
    };
    return t;
}

pub fn add(self: *Self, alloc: Allocator, entries: [][]const u8) !void {
    const shard = self.entries.next();
    const blocksList = try shard.add(alloc, entries, self.maxIndexBlockSize);
    if (blocksList == null) return;

    var blocks = blocksList.?;
    defer blocks.deinit(alloc);
    try self.flushBlocks(alloc, blocks.items);
}

fn flushBlocks(self: *Self, alloc: Allocator, blocks: []*MemBlock) !void {
    if (blocks.len == 0) return;

    self.mxBlocks.lock();
    defer self.mxBlocks.unlock();

    if (self.blocksToFlush.items.len == 0) {
        self.flushAtUs = std.time.microTimestamp() + std.time.us_per_s;
    }

    try self.blocksToFlush.appendSlice(alloc, blocks);
    if (self.blocksToFlush.items.len >= self.blocksThresholdToFlush) {
        try self.flushBlocksToMemTables(alloc, self.blocksToFlush.items, false);
        self.blocksToFlush.clearRetainingCapacity();
    }
}

fn flushBlocksToMemTables(self: *Self, alloc: Allocator, blocks: []*MemBlock, force: bool) !void {
    const tablesSize = (blocks.len + blocksInMemTable - 1) / blocksInMemTable;
    var memTables = try std.ArrayList(*MemTable).initCapacity(alloc, tablesSize);
    errdefer {
        for (memTables.items) |memTable| memTable.deinit(alloc);
        memTables.deinit(alloc);
    }

    var tail = blocks[0..];
    // TODO: benchmark parallel mem table creation
    while (tail.len > 0) {
        const offset = @min(blocksInMemTable, tail.len);
        const head = tail[0..offset];
        tail = tail[offset..];

        const memTable = try MemTable.init(alloc, head);
        memTables.appendAssumeCapacity(memTable);
    }

    _ = self;
    _ = force;
    unreachable;
    // TODO: merge tables
    // var n: usize = 0;
    // var memTableSlice = memTables.items[n..];
    // while (memTableSlice.len > 1) {
    //     n = self.mergeMemTables(alloc, memTableSlice);
    //     memTableSlice = memTableSlice[n..];
    // }
    // if (memTableSlice.len == 1) {
    //     self.addToMemTables(alloc, memTableSlice[0], force);
    // }

    // for (memTables.items) |memTable| {
    //     self.addToMemTables(alloc, memTable, force);
    // }
}
