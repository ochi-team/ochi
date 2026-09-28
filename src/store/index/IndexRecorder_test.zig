const std = @import("std");
const Allocator = std.mem.Allocator;
const Io = std.Io;
const xev = @import("xev");
const tracy = @import("tracy");

const fs = @import("../../fs.zig");

const timedWait = @import("../../stds/sem.zig").timedWait;

const cap = @import("../table/cap.zig");

const Cache = @import("../../stds/Cache.zig").Cache;
const Entries = @import("Entries.zig");
const maxEntrySize = Entries.maxEntrySize;
const MemBlock = @import("MemBlock.zig");
const Table = @import("Table.zig");
const MemTable = @import("MemTable.zig");
const BlockWriter = @import("BlockWriter.zig");
const BlockReader = @import("BlockReader.zig");
const LookupTable = @import("lookup/LookupTable.zig");
const CompressionPool = @import("../compression/CompressionPool.zig");
const DecompressionPool = @import("../compression/DecompressionPool.zig");

const merge = @import("../table/merge.zig");
const TableKind = merge.TableKind;
const swap = @import("../table/swap.zig");

const Conf = @import("../../Conf.zig");
const Stop = @import("../../stds/Stop.zig");
const TimerLoop = @import("../../stds/xev/TimerLoop.zig");
const Runtime = @import("../../Runtime.zig");
const Logger = @import("logging");
const DebugIo = @import("../../stds/Io/DebugIo.zig");

const Consts = @import("../../Consts.zig");

const amountOfTablesToMerge = @import("../../Consts.zig").amountOfTablesToMerge;

const blocksInMemTable = 16;

const maxMemTables = 16;
comptime {
    // it claims we can use a buffer size of amountOfTablesToMerge to handle merging any kind of tables
    std.debug.assert(maxMemTables <= amountOfTablesToMerge);
}

const merger = merge.Merger(*Table, maxMemTables, amountOfTablesToMerge);
const swapper = swap.Swapper(IndexRecorder, Table);

const IndexRecorder = @import("IndexRecorder.zig");

const testing = std.testing;

fn createMemTableFromItems(io: Io, alloc: Allocator, items: []const []const u8) !*Table {
    const compressionPool = try CompressionPool.init(alloc, 1);
    defer compressionPool.deinit(alloc);
    const decompressionPool = try DecompressionPool.init(alloc, 1);
    defer decompressionPool.deinit(alloc);

    var total: u32 = 0;
    for (items) |item| total += @intCast(item.len);
    var block = try MemBlock.init(alloc, .{
        .maxMemBlockSize = total + 16,
        .blocksCountHint = items.len,
    });
    for (items) |item| {
        const ok = block.add(item);
        try testing.expect(ok);
    }
    var blocks = [_]*MemBlock{block};
    const memTable = try MemTable.init(io, alloc, &blocks, compressionPool, decompressionPool);
    return Table.fromMem(io, alloc, memTable, decompressionPool);
}

fn createDiskTableFromItems(
    io: Io,
    alloc: Allocator,
    rootPath: []const u8,
    tableName: []const u8,
    decompressionPool: *DecompressionPool,
    items: []const []const u8,
) !*Table {
    const tablePath = try std.fmt.allocPrint(alloc, "{s}/{s}", .{ rootPath, tableName });
    errdefer alloc.free(tablePath);

    const memTable = try createMemTableFromItems(io, alloc, items);
    defer memTable.close(io);
    try memTable.inner.mem.storeToDisk(io, tablePath);
    return Table.open(io, alloc, tablePath, decompressionPool);
}

fn countMemItemsInRecorder(recorder: *IndexRecorder) u64 {
    var count: u64 = 0;
    for (recorder.memTables.items) |table| {
        count += table.tableHeader().entriesCount;
    }
    return count;
}

fn countDiskItemsInRecorder(recorder: *IndexRecorder) u64 {
    var count: u64 = 0;
    for (recorder.diskTables.items) |table| {
        count += table.tableHeader().entriesCount;
    }
    return count;
}

const stableItems = [_][]const u8{
    "item-a", "item-b", "item-c", "item-d", "item-e", "item-f", "item-g", "item-h",
};

test "flushMemEntries non-force respects flush deadline" {
    const alloc = testing.allocator;
    const io = testing.io;

    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    const rootPath = try tmp.dir.realPathFileAlloc(io, ".", alloc);
    defer alloc.free(rootPath);

    const runtime = try Runtime.init(io, alloc, rootPath, 0.5);
    defer runtime.deinit(alloc);

    const compressionPool = try CompressionPool.init(alloc, 1);
    defer compressionPool.deinit(alloc);
    const decompressionPool = try DecompressionPool.init(alloc, 1);
    defer decompressionPool.deinit(alloc);

    var mergePool = xev.ThreadPool.init(.{ .max_threads = runtime.cpus });
    defer {
        mergePool.shutdown();
        mergePool.deinit();
    }

    const timerLoop = try TimerLoop.init(alloc);
    const recorder = try IndexRecorder.init(io, alloc, rootPath, runtime, compressionPool, decompressionPool, &mergePool, timerLoop);
    defer recorder.deinit(io, alloc);
    defer {
        timerLoop.stop();
        timerLoop.join();
        timerLoop.deinit();
    }

    var block = try MemBlock.init(alloc, .{
        .maxMemBlockSize = 64,
        .blocksCountHint = 1,
    });
    errdefer block.deinit(alloc);
    const ok = block.add("alpha");
    try testing.expect(ok);
    try recorder.blocksToFlush.append(alloc, block);

    var dst = try std.ArrayList(*MemBlock).initCapacity(alloc, 4);
    defer dst.deinit(alloc);

    recorder.flushEntriesAtUs = Io.Timestamp.now(io, .real).toMicroseconds() + std.time.us_per_s;
    try recorder.flushMemEntries(io, alloc, &dst, false);
    try testing.expectEqual(1, recorder.blocksToFlush.items.len);
    try testing.expectEqual(0, recorder.memTables.items.len);

    recorder.flushEntriesAtUs = Io.Timestamp.now(io, .real).toMicroseconds() - std.time.us_per_s;
    try recorder.flushMemEntries(io, alloc, &dst, false);
    try testing.expectEqual(0, recorder.blocksToFlush.items.len);
    try testing.expect(recorder.memTables.items.len > 0);

    try recorder.stop(io, alloc);
}

test "mergeTables force single mem table creates disk table" {
    const alloc = testing.allocator;
    const io = testing.io;

    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    const rootPath = try tmp.dir.realPathFileAlloc(io, ".", alloc);
    defer alloc.free(rootPath);

    const runtime = try Runtime.init(io, alloc, rootPath, 0.5);
    defer runtime.deinit(alloc);

    const compressionPool = try CompressionPool.init(alloc, 1);
    defer compressionPool.deinit(alloc);
    const decompressionPool = try DecompressionPool.init(alloc, 1);
    defer decompressionPool.deinit(alloc);

    var mergePool = xev.ThreadPool.init(.{ .max_threads = runtime.cpus });
    defer {
        mergePool.shutdown();
        mergePool.deinit();
    }

    const timerLoop = try TimerLoop.init(alloc);
    const recorder = try IndexRecorder.init(io, alloc, rootPath, runtime, compressionPool, decompressionPool, &mergePool, timerLoop);
    defer recorder.deinit(io, alloc);
    defer {
        timerLoop.stop();
        timerLoop.join();
        timerLoop.deinit();
    }

    const table = try createMemTableFromItems(io, alloc, &.{ "k1", "k2", "k3" });
    try recorder.memTables.append(alloc, table);
    table.inMerge = true;

    var single = [_]*Table{table};
    try recorder.mergeTables(io, alloc, single[0..], true, null);
    try testing.expectEqual(@as(usize, 0), recorder.memTables.items.len);
    try testing.expectEqual(@as(usize, 1), recorder.diskTables.items.len);
    try testing.expect(recorder.diskTables.items[0].inner == .disk);

    try recorder.stop(io, alloc);
}

test "IndexRecorder add and reopen preserves item count" {
    const alloc = testing.allocator;
    const io = testing.io;

    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    const rootPath = try tmp.dir.realPathFileAlloc(io, ".", alloc);
    defer alloc.free(rootPath);

    const inserted: usize = 128;
    {
        const runtime = try Runtime.init(io, alloc, rootPath, 0.5);
        defer runtime.deinit(alloc);

        const compressionPool = try CompressionPool.init(alloc, 1);
        defer compressionPool.deinit(alloc);
        const decompressionPool = try DecompressionPool.init(alloc, 1);
        defer decompressionPool.deinit(alloc);

        var mergePool = xev.ThreadPool.init(.{ .max_threads = runtime.cpus });
        defer {
            mergePool.shutdown();
            mergePool.deinit();
        }

        const timerLoop = try TimerLoop.init(alloc);
        const recorder = try IndexRecorder.init(io, alloc, rootPath, runtime, compressionPool, decompressionPool, &mergePool, timerLoop);
        defer recorder.deinit(io, alloc);
        defer {
            timerLoop.stop();
            timerLoop.join();
            timerLoop.deinit();
        }

        for (0..inserted) |i| {
            const item = stableItems[i % stableItems.len];
            var batch = [_][]const u8{item};
            try recorder.add(io, alloc, &batch);
        }

        try recorder.flushForce(io, alloc);
        try testing.expectEqual(@as(usize, 0), recorder.memTables.items.len);
        try testing.expect(recorder.diskTables.items.len > 0);
        try testing.expectEqual(@as(u64, 0), countMemItemsInRecorder(recorder));
        try testing.expectEqual(@as(u64, inserted), countDiskItemsInRecorder(recorder));

        try recorder.stop(io, alloc);
    }

    {
        const runtime = try Runtime.init(io, alloc, rootPath, 0.5);
        defer runtime.deinit(alloc);

        const compressionPool = try CompressionPool.init(alloc, 1);
        defer compressionPool.deinit(alloc);
        const decompressionPool = try DecompressionPool.init(alloc, 1);
        defer decompressionPool.deinit(alloc);

        var mergePool = xev.ThreadPool.init(.{ .max_threads = runtime.cpus });
        defer {
            mergePool.shutdown();
            mergePool.deinit();
        }

        const timerLoop = try TimerLoop.init(alloc);
        const reopened = try IndexRecorder.init(io, alloc, rootPath, runtime, compressionPool, decompressionPool, &mergePool, timerLoop);
        defer reopened.deinit(io, alloc);
        defer {
            timerLoop.stop();
            timerLoop.join();
            timerLoop.deinit();
        }
        try testing.expect(reopened.diskTables.items.len > 0);
        try testing.expectEqual(@as(u64, 0), countMemItemsInRecorder(reopened));
        try testing.expectEqual(@as(u64, inserted), countDiskItemsInRecorder(reopened));
    }
}

const AddWorkerCtx = struct {
    io: Io,
    alloc: Allocator,
    recorder: *IndexRecorder,
    workerID: usize,
    rounds: usize,
};

const testWorkerBatchSize = 60;
fn addWorker(ctx: *AddWorkerCtx) void {
    var round: usize = 0;
    while (round < ctx.rounds) : (round += 1) {
        var batch: [testWorkerBatchSize][]const u8 = undefined;
        for (0..testWorkerBatchSize) |i| {
            batch[i] = stableItems[(ctx.workerID + round + i) % stableItems.len];
        }

        ctx.recorder.add(ctx.io, ctx.alloc, batch[0..]) catch |err| {
            Logger.log(.err, "failed to add batch in worker", .{ .workerID = ctx.workerID, .err = err });
            return;
        };
    }
}

const WorkerCtxWithItems = struct {
    io: Io,
    alloc: Allocator,
    recorder: *IndexRecorder,
    workerID: usize,
    rounds: usize,
    items: []const []const u8,
};

fn allocCtxItem(alloc: Allocator, id: usize, len: usize) ![]u8 {
    const buf = try alloc.alloc(u8, len);
    errdefer alloc.free(buf);

    const head = try std.fmt.bufPrint(buf, "tenant-42-{d:0>4}-", .{id});
    if (head.len < len) {
        for (head.len..len) |i| {
            buf[i] = @intCast('a' + ((id + i) % 26));
        }
    }
    return buf;
}

fn addWorkerWithItems(ctx: *WorkerCtxWithItems) void {
    var round: usize = 0;
    while (round < ctx.rounds) : (round += 1) {
        var batch: [testWorkerBatchSize][]const u8 = undefined;
        for (0..testWorkerBatchSize) |i| {
            const idx = (ctx.workerID * ctx.rounds + round + i) % ctx.items.len;
            batch[i] = ctx.items[idx];
        }

        ctx.recorder.add(ctx.io, ctx.alloc, batch[0..]) catch |err| {
            Logger.log(.err, "failed to add batch in worker", .{ .workerID = ctx.workerID, .err = err });
            return;
        };
    }
}

test "IndexRecorder background flusher survives load" {
    const alloc = testing.allocator;
    const io = testing.io;

    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    const rootPath = try tmp.dir.realPathFileAlloc(io, ".", alloc);
    defer alloc.free(rootPath);

    const runtime = try Runtime.init(io, alloc, rootPath, 0.5);
    defer runtime.deinit(alloc);

    const compressionPool = try CompressionPool.init(alloc, 1);
    defer compressionPool.deinit(alloc);
    const decompressionPool = try DecompressionPool.init(alloc, 1);
    defer decompressionPool.deinit(alloc);
    runtime.cpus = 4;

    var mergePool = xev.ThreadPool.init(.{ .max_threads = runtime.cpus });
    defer {
        mergePool.shutdown();
        mergePool.deinit();
    }

    const timerLoop = try TimerLoop.init(alloc);
    const recorder = try IndexRecorder.init(io, alloc, rootPath, runtime, compressionPool, decompressionPool, &mergePool, timerLoop);
    defer recorder.deinit(io, alloc);
    defer {
        timerLoop.stop();
        timerLoop.join();
        timerLoop.deinit();
    }
    recorder.maxMemBlockSize = 256;
    try recorder.startTasks(io, alloc);

    var g: std.Io.Group = .init;
    errdefer g.cancel(io);

    const workers = 4;
    const rounds = 100;
    var ctxs: [workers]AddWorkerCtx = undefined;

    for (0..workers) |i| {
        ctxs[i] = .{
            .io = io,
            .alloc = alloc,
            .recorder = recorder,
            .workerID = i,
            .rounds = rounds,
        };
        try g.concurrent(io, addWorker, .{&ctxs[i]});
    }

    try g.await(io);
    try recorder.stop(io, alloc);

    try testing.expectEqual(0, countMemItemsInRecorder(recorder));
    try testing.expectEqual(workers * rounds * testWorkerBatchSize, countDiskItemsInRecorder(recorder));
}

test "IndexRecorder disk table merger survives large load" {
    const alloc = testing.allocator;
    const io = testing.io;

    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    const rootPath = try tmp.dir.realPathFileAlloc(io, ".", alloc);
    defer alloc.free(rootPath);

    const runtime = try Runtime.init(io, alloc, rootPath, 0.5);
    defer runtime.deinit(alloc);

    const compressionPool = try CompressionPool.init(alloc, 1);
    defer compressionPool.deinit(alloc);
    const decompressionPool = try DecompressionPool.init(alloc, 1);
    defer decompressionPool.deinit(alloc);
    runtime.cpus = 4;

    var mergePool = xev.ThreadPool.init(.{ .max_threads = runtime.cpus });
    defer {
        mergePool.shutdown();
        mergePool.deinit();
    }

    const timerLoop = try TimerLoop.init(alloc);
    const recorder = try IndexRecorder.init(io, alloc, rootPath, runtime, compressionPool, decompressionPool, &mergePool, timerLoop);
    recorder.maxMemBlockSize = 4 * 1024;
    defer recorder.deinit(io, alloc);
    defer {
        timerLoop.stop();
        timerLoop.join();
        timerLoop.deinit();
    }
    try recorder.startTasks(io, alloc);

    const minItemLen = 512;
    const maxItemLen = maxEntrySize;
    const itemPoolSize = 32;
    const lenSpan = maxItemLen - minItemLen + 1;

    var itemPool = try std.ArrayList([]u8).initCapacity(alloc, itemPoolSize);
    defer {
        for (itemPool.items) |item| alloc.free(item);
        itemPool.deinit(alloc);
    }
    for (0..itemPoolSize) |i| {
        const itemLen = minItemLen + ((i * 977) % lenSpan);
        const item = try allocCtxItem(alloc, i, itemLen);
        try itemPool.append(alloc, item);
    }

    var g: std.Io.Group = .init;
    errdefer g.cancel(io);

    const workers = 3;
    const rounds = 8;
    var ctxs: [workers]WorkerCtxWithItems = undefined;

    for (0..workers) |i| {
        ctxs[i] = .{
            .io = io,
            .alloc = alloc,
            .recorder = recorder,
            .workerID = i,
            .rounds = rounds,
            .items = itemPool.items,
        };
        try g.concurrent(io, addWorkerWithItems, .{&ctxs[i]});
    }

    try g.await(io);
    try recorder.stop(io, alloc);

    try testing.expectEqual(0, countMemItemsInRecorder(recorder));
    try testing.expectEqual(workers * rounds * testWorkerBatchSize, countDiskItemsInRecorder(recorder));
}

test "IndexRecorder flushForce skips oversized-only input without crash" {
    const alloc = testing.allocator;
    const io = testing.io;

    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    const rootPath = try tmp.dir.realPathFileAlloc(io, ".", alloc);
    defer alloc.free(rootPath);

    const runtime = try Runtime.init(io, alloc, rootPath, 0.5);
    defer runtime.deinit(alloc);

    const compressionPool = try CompressionPool.init(alloc, 1);
    defer compressionPool.deinit(alloc);
    const decompressionPool = try DecompressionPool.init(alloc, 1);
    defer decompressionPool.deinit(alloc);
    runtime.cpus = 1;

    var mergePool = xev.ThreadPool.init(.{ .max_threads = runtime.cpus });
    defer {
        mergePool.shutdown();
        mergePool.deinit();
    }

    const timerLoop = try TimerLoop.init(alloc);
    const recorder = try IndexRecorder.init(io, alloc, rootPath, runtime, compressionPool, decompressionPool, &mergePool, timerLoop);
    defer recorder.deinit(io, alloc);
    defer {
        timerLoop.stop();
        timerLoop.join();
        timerLoop.deinit();
    }
    recorder.maxMemBlockSize = 64;

    const tooLarge = "x" ** 512;
    try recorder.add(io, alloc, &.{tooLarge});

    // Must not crash: oversized entries are skipped before any mem block is created.
    try recorder.flushForce(io, alloc);

    try testing.expectEqual(@as(usize, 0), recorder.blocksToFlush.items.len);
    var blocksInShards: usize = 0;
    for (recorder.entries.shards) |shard| {
        blocksInShards += shard.blocks.items.len;
    }
    try testing.expectEqual(@as(usize, 0), blocksInShards);
    try testing.expectEqual(@as(u64, 0), countMemItemsInRecorder(recorder));
    try testing.expectEqual(@as(u64, 0), countDiskItemsInRecorder(recorder));
}

test "IndexRecorder reads free disk space from runtime" {
    const alloc = testing.allocator;
    const io = testing.io;

    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    const rootPath = try tmp.dir.realPathFileAlloc(io, "./", alloc);

    defer alloc.free(rootPath);

    const runtime = try Runtime.init(io, alloc, rootPath, 0.5);
    defer runtime.deinit(alloc);

    const compressionPool = try CompressionPool.init(alloc, 1);
    defer compressionPool.deinit(alloc);
    const decompressionPool = try DecompressionPool.init(alloc, 1);
    defer decompressionPool.deinit(alloc);

    var mergePool = xev.ThreadPool.init(.{ .max_threads = runtime.cpus });
    defer {
        mergePool.shutdown();
        mergePool.deinit();
    }

    const timerLoop = try TimerLoop.init(alloc);
    const recorder = try IndexRecorder.init(io, alloc, rootPath, runtime, compressionPool, decompressionPool, &mergePool, timerLoop);
    var batch = [_][]const u8{stableItems[1]};

    try recorder.add(io, alloc, &batch);
    // startMemTablesMerge is necessary to call before we stop the recorder
    try recorder.startMemTablesMerge(io, alloc);

    const firstSpace = runtime.getFreeDiskSpace(io);
    const secondSpace = runtime.getFreeDiskSpace(io);
    try testing.expect(firstSpace > 0);
    try testing.expectEqual(firstSpace, secondSpace);

    recorder.stopped.stop(io);
    defer recorder.deinit(io, alloc);
    defer {
        timerLoop.stop();
        timerLoop.join();
        timerLoop.deinit();
    }
    recorder.waitForMergesToDrain(io);
}

test "IndexRecorder large entries write to 3 shards sequentially" {
    const alloc = testing.allocator;
    const io = testing.io;

    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    const rootPath = try tmp.dir.realPathFileAlloc(io, ".", alloc);
    defer alloc.free(rootPath);
    const maxIndexMemBlockSize = 256;
    const countAdditionalEntries = Entries.maxBlocksPerShard - 1;
    const theLargest = "x" ** (maxIndexMemBlockSize);

    const runtime = try Runtime.init(io, alloc, rootPath, 0.5);
    defer runtime.deinit(alloc);

    const compressionPool = try CompressionPool.init(alloc, 1);
    defer compressionPool.deinit(alloc);
    const decompressionPool = try DecompressionPool.init(alloc, 1);
    defer decompressionPool.deinit(alloc);
    runtime.cpus = 3;

    var mergePool = xev.ThreadPool.init(.{ .max_threads = runtime.cpus });
    defer {
        mergePool.shutdown();
        mergePool.deinit();
    }

    const timerLoop = try TimerLoop.init(alloc);
    const recorder = try IndexRecorder.init(io, alloc, rootPath, runtime, compressionPool, decompressionPool, &mergePool, timerLoop);
    defer recorder.deinit(io, alloc);
    defer {
        timerLoop.stop();
        timerLoop.join();
        timerLoop.deinit();
    }
    recorder.maxMemBlockSize = maxIndexMemBlockSize;

    const firstShardEntries = try alloc.alloc([]const u8, Entries.maxBlocksPerShard);
    defer alloc.free(firstShardEntries);
    const secondShardEntries = try alloc.alloc([]const u8, Entries.maxBlocksPerShard);
    defer alloc.free(secondShardEntries);
    const thirdShardEntries = try alloc.alloc([]const u8, countAdditionalEntries);
    defer alloc.free(thirdShardEntries);

    for (firstShardEntries) |*entry| entry.* = theLargest;
    for (secondShardEntries) |*entry| entry.* = theLargest;
    for (thirdShardEntries) |*entry| entry.* = theLargest;

    try recorder.add(io, alloc, firstShardEntries);
    try recorder.add(io, alloc, secondShardEntries);
    try recorder.add(io, alloc, thirdShardEntries);

    try testing.expectEqual(2 * Entries.maxBlocksPerShard, recorder.blocksToFlush.items.len);

    var blocksInShards: usize = 0;
    for (recorder.entries.shards) |shard| {
        blocksInShards += shard.blocks.items.len;
    }
    try testing.expectEqual(countAdditionalEntries, blocksInShards);
    try recorder.stop(io, alloc);
}

test "IndexRecorder 3 shards addings small entries doesn't flush them" {
    const alloc = testing.allocator;
    const io = testing.io;

    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    const rootPath = try tmp.dir.realPathFileAlloc(io, ".", alloc);
    defer alloc.free(rootPath);
    const shortValue = "short";

    const runtime = try Runtime.init(io, alloc, rootPath, 0.5);
    defer runtime.deinit(alloc);

    const compressionPool = try CompressionPool.init(alloc, 1);
    defer compressionPool.deinit(alloc);
    const decompressionPool = try DecompressionPool.init(alloc, 1);
    defer decompressionPool.deinit(alloc);
    runtime.cpus = 3;

    var mergePool = xev.ThreadPool.init(.{ .max_threads = runtime.cpus });
    defer {
        mergePool.shutdown();
        mergePool.deinit();
    }

    const timerLoop = try TimerLoop.init(alloc);
    const recorder = try IndexRecorder.init(io, alloc, rootPath, runtime, compressionPool, decompressionPool, &mergePool, timerLoop);
    defer recorder.deinit(io, alloc);
    defer {
        timerLoop.stop();
        timerLoop.join();
        timerLoop.deinit();
    }
    try testing.expectEqual(recorder.entries.shards.len, runtime.cpus);

    for (0..runtime.cpus) |_| {
        try recorder.add(io, alloc, &.{shortValue});
    }

    try testing.expectEqual(0, recorder.blocksToFlush.items.len);
    for (recorder.entries.shards) |*shard| {
        shard.mx.lockUncancelable(io);
        defer shard.mx.unlock(io);

        try testing.expectEqual(1, shard.blocks.items.len);
        try testing.expectEqual(1, shard.blocks.items[0].memEntries.items.len);
        try testing.expectEqualStrings(shortValue, shard.blocks.items[0].get(0));
    }

    try recorder.stop(io, alloc);
    var tables = try Table.openAll(io, alloc, rootPath, decompressionPool);
    try testing.expectEqual(tables.items.len, 1);
    defer {
        for (tables.items) |table| table.release(io);
        tables.deinit(alloc);
    }
    const flushedTable = tables.items[0];

    const cache = try Cache(*MemBlock).init(io, alloc, .{ .meter = .{ .name = "" } });
    defer cache.deinit();
    var lookup = LookupTable.init(alloc, flushedTable, Conf.getConf().app.maxIndexMemBlockSize, cache, decompressionPool);
    defer lookup.deinit(alloc);

    try lookup.seek(io, alloc, shortValue);
    var readItems: usize = 0;
    while (try lookup.next(io, alloc)) {
        try testing.expectEqualStrings(shortValue, lookup.current);
        readItems += 1;
    }
    try testing.expectEqual(runtime.cpus, readItems);
}

test "IndexRecorder large entries write to 3 shards" {
    const alloc = testing.allocator;
    const io = testing.io;

    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    const rootPath = try tmp.dir.realPathFileAlloc(io, ".", alloc);
    defer alloc.free(rootPath);
    const maxIndexMemBlockSize = 256;
    //countAdditionalEntries < Entries.maxBlocksPerShard
    const countAdditionalEntries = Entries.maxBlocksPerShard - 1;
    //2 shards full-filled and third shard is not completely filled
    const totalEntries = (2 * Entries.maxBlocksPerShard) + countAdditionalEntries;
    const theLargest = "x" ** maxIndexMemBlockSize;
    var testEntries: [][]const u8 = try alloc.alloc([]const u8, totalEntries);
    defer alloc.free(testEntries);

    for (0..totalEntries) |i| {
        testEntries[i] = theLargest;
    }

    const runtime = try Runtime.init(io, alloc, rootPath, 0.5);
    defer runtime.deinit(alloc);

    const compressionPool = try CompressionPool.init(alloc, 1);
    defer compressionPool.deinit(alloc);
    const decompressionPool = try DecompressionPool.init(alloc, 1);
    defer decompressionPool.deinit(alloc);
    runtime.cpus = 3;

    var mergePool = xev.ThreadPool.init(.{ .max_threads = runtime.cpus });
    defer {
        mergePool.shutdown();
        mergePool.deinit();
    }

    const timerLoop = try TimerLoop.init(alloc);
    const recorder = try IndexRecorder.init(io, alloc, rootPath, runtime, compressionPool, decompressionPool, &mergePool, timerLoop);
    defer recorder.deinit(io, alloc);
    defer {
        timerLoop.stop();
        timerLoop.join();
        timerLoop.deinit();
    }
    recorder.maxMemBlockSize = maxIndexMemBlockSize;

    try recorder.add(io, alloc, testEntries);

    try testing.expectEqual(totalEntries - countAdditionalEntries, recorder.blocksToFlush.items.len);

    try recorder.stop(io, alloc);

    try testing.expectEqual(@as(usize, 0), recorder.memTables.items.len);
    try testing.expect(recorder.diskTables.items.len > 0);
    try testing.expectEqual(@as(u64, 0), countMemItemsInRecorder(recorder));
    try testing.expectEqual(@as(u64, totalEntries), countDiskItemsInRecorder(recorder));
}

test "addToMemTables overflows memTables past maxMemTables when the semaphore wait times out" {
    const alloc = testing.allocator;
    const io = testing.io;

    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    const rootPath = try tmp.dir.realPathFileAlloc(io, ".", alloc);
    defer alloc.free(rootPath);

    const runtime = try Runtime.init(io, alloc, rootPath, 0.5);
    defer runtime.deinit(alloc);

    const compressionPool = try CompressionPool.init(alloc, 1);
    defer compressionPool.deinit(alloc);
    const decompressionPool = try DecompressionPool.init(alloc, 1);
    defer decompressionPool.deinit(alloc);

    var mergePool = xev.ThreadPool.init(.{ .max_threads = runtime.cpus });
    defer {
        mergePool.shutdown();
        mergePool.deinit();
    }

    const timerLoop = try TimerLoop.init(alloc);
    const recorder = try IndexRecorder.init(io, alloc, rootPath, runtime, compressionPool, decompressionPool, &mergePool, timerLoop);
    defer recorder.deinit(io, alloc);
    defer {
        timerLoop.stop();
        timerLoop.join();
        timerLoop.deinit();
    }

    // Fill memTables to its cap with tables that are still "in merge" (simulating a
    // slow concurrent merger), so addToMemTables's forced flush can't reclaim a slot.
    for (0..maxMemTables) |i| {
        const table = try createMemTableFromItems(io, alloc, &.{stableItems[i % stableItems.len]});
        table.inMerge = true;
        try recorder.memTables.append(alloc, table);
    }

    // Exhaust the semaphore so addToMemTables's timedWait must fail with error.Timeout.
    recorder.memTablesSem.permits = 0;

    const extraTable = try createMemTableFromItems(io, alloc, &.{"extra"});

    // since all mem tables are 'in merge' and never gonna merge, we expect addToMemTables
    // to time out and flush the extra table directly to disk instead of overflowing memTables.
    try recorder.addToMemTables(io, alloc, extraTable, false);

    try testing.expectEqual(@as(usize, maxMemTables), recorder.memTables.items.len);
    try testing.expect(recorder.diskTables.items.len > 0);

    // deinit test data
    for (recorder.memTables.items) |t| t.inMerge = false;
    try recorder.flushForce(io, alloc);
}
