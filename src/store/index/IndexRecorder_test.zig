const std = @import("std");
const checkAllAllocationFailures = @import("../../stds/testing.zig").checkAllAllocationFailures;
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
const TimerLoop = @import("../../stds/xev/TimerLoop.zig");
const shutdownTesting = @import("../../stds/testing/Shutdown.zig");
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
const TableTimerSlot = IndexRecorder.TableTimerSlot;
const tableTimerCallback = IndexRecorder.tableTimerCallback;

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

fn startTasks(self: *IndexRecorder, io: Io, alloc: Allocator) !void {
    if (self.timerLoop.stopping.load(.acquire)) return;
    try self.startTasks(io, alloc);
    const registrations = self.taskRegistrations();
    try self.timerLoop.register(&registrations);
}

const stableItems = [_][]const u8{
    "item-a", "item-b", "item-c", "item-d", "item-e", "item-f", "item-g", "item-h",
};

/// Real one-item tables with synthetic selection sizes; no background tasks run.
const SkewedMergeFixture = struct {
    alloc: Allocator,
    tmp: testing.TmpDir,
    rootPath: [:0]const u8,
    runtime: *Runtime,
    compressionPool: *CompressionPool,
    decompressionPool: *DecompressionPool,
    mergePool: *xev.ThreadPool,
    timerLoop: *TimerLoop,
    recorder: *IndexRecorder,

    const rows = [_][]const u8{
        "pressure-00", "pressure-01", "pressure-02", "pressure-03", "pressure-04", "pressure-05", "pressure-06", "pressure-07",
        "pressure-08", "pressure-09", "pressure-10", "pressure-11", "pressure-12", "pressure-13", "pressure-14", "pressure-15",
    };
    const State = struct {
        memTables: usize = 0,
        diskTables: usize = 0,
        memRows: u64 = 0,
        diskRows: u64 = 0,
        permits: usize,
        mergingTables: usize = 0,
    };

    fn init(alloc: Allocator, tableCount: usize) !SkewedMergeFixture {
        std.debug.assert(tableCount <= maxMemTables);
        const io = testing.io;
        var tmp = testing.tmpDir(.{});
        errdefer tmp.cleanup();
        const rootPath = try tmp.dir.realPathFileAlloc(io, ".", alloc);
        errdefer alloc.free(rootPath);
        const runtime = try Runtime.init(io, alloc, rootPath, 0.5);
        errdefer runtime.deinit(alloc);
        const compressionPool = try CompressionPool.init(alloc, 1);
        errdefer compressionPool.deinit(alloc);
        const decompressionPool = try DecompressionPool.init(alloc, 1);
        errdefer decompressionPool.deinit(alloc);
        // The recorder retains this pointer even when the fixture moves.
        const mergePool = try alloc.create(xev.ThreadPool);
        errdefer alloc.destroy(mergePool);
        mergePool.* = xev.ThreadPool.init(.{ .max_threads = 1 });
        errdefer {
            mergePool.shutdown();
            mergePool.deinit();
        }
        const timerLoop = try TimerLoop.init(alloc);
        errdefer timerLoop.deinit();
        const recorder = try IndexRecorder.init(io, alloc, rootPath, runtime, compressionPool, decompressionPool, mergePool, timerLoop);
        errdefer {
            // Rollback must not allocate to flush partial input.
            for (recorder.memTables.items) |table| table.release(io);
            recorder.memTables.clearRetainingCapacity();
            recorder.shutdown(io, alloc) catch unreachable;
            recorder.deinit(io, alloc);
        }

        var selectionSize: u64 = 1;
        for (rows[0..tableCount]) |row| {
            const table = try createMemTableFromItems(io, alloc, &.{row});
            // Persisted items are real; only compaction selection sees these sizes.
            table.size = selectionSize;
            selectionSize *= 2;
            recorder.memTablesSem.waitUncancelable(io);
            recorder.memTables.appendAssumeCapacity(table);
        }
        return .{
            .alloc = alloc,
            .tmp = tmp,
            .rootPath = rootPath,
            .runtime = runtime,
            .compressionPool = compressionPool,
            .decompressionPool = decompressionPool,
            .mergePool = mergePool,
            .timerLoop = timerLoop,
            .recorder = recorder,
        };
    }

    fn deinit(self: *SkewedMergeFixture) void {
        const alloc = self.alloc;
        const io = testing.io;
        // Recover stranded flags after assertions so rollback failures do not abort cleanup.
        for (self.recorder.memTables.items) |table| table.inMerge = false;
        self.recorder.shutdown(io, alloc) catch unreachable;
        self.timerLoop.stop();
        self.timerLoop.join();
        self.recorder.deinit(io, alloc);
        self.timerLoop.deinit();
        self.mergePool.shutdown();
        self.mergePool.deinit();
        alloc.destroy(self.mergePool);
        self.decompressionPool.deinit(alloc);
        self.compressionPool.deinit(alloc);
        self.runtime.deinit(alloc);
        alloc.free(self.rootPath);
        self.tmp.cleanup();
    }

    fn openRecorder(self: *SkewedMergeFixture) !*IndexRecorder {
        return IndexRecorder.init(testing.io, self.alloc, self.rootPath, self.runtime, self.compressionPool, self.decompressionPool, self.mergePool, self.timerLoop);
    }

    fn state(self: *SkewedMergeFixture) State {
        const recorder = self.recorder;
        recorder.mxTables.lockUncancelable(testing.io);
        defer recorder.mxTables.unlock(testing.io);
        var mergingTables: usize = 0;
        for (recorder.memTables.items) |table| {
            if (table.inMerge) mergingTables += 1;
        }
        for (recorder.diskTables.items) |table| {
            if (table.inMerge) mergingTables += 1;
        }
        return .{
            .memTables = recorder.memTables.items.len,
            .diskTables = recorder.diskTables.items.len,
            .memRows = countMemItemsInRecorder(recorder),
            .diskRows = countDiskItemsInRecorder(recorder),
            .permits = recorder.memTablesSem.permits,
            .mergingTables = mergingTables,
        };
    }
};

test "flushForce clears index merge flags after a chunk fails" {
    const a = testing.allocator;
    const f = struct {
        fn f(alloc: Allocator, count: usize) !void {
            const backingAlloc = testing.allocator;
            var fixture = try SkewedMergeFixture.init(backingAlloc, count);
            defer fixture.deinit();
            const recorder = fixture.recorder;

            // flushForce swaps this buffer into a local list freed with the failing allocator.
            // TODO: make the allocator ownership sinmpler and remove this statement
            try testing.expectEqual(0, recorder.blocksToFlush.items.len);
            recorder.blocksToFlush.deinit(backingAlloc);
            recorder.blocksToFlush = .empty;

            // Keep list storage on the allocator used by fixture cleanup.
            try recorder.diskTables.ensureTotalCapacity(backingAlloc, count);
            recorder.halt(testing.io);

            // Two balanced groups require separate flush chunks.
            const largeGroupSize = 10000;
            for (recorder.memTables.items, 0..) |table, i| table.size = if (i < maxMemTables / 2) 1 else largeGroupSize;
            recorder.flushForce(testing.io, alloc) catch |err| {
                try testing.expectEqual(error.OutOfMemory, err);
                const after = fixture.state();
                try testing.expectEqualDeep(SkewedMergeFixture.State{
                    .memTables = after.memTables,
                    .diskTables = after.diskTables,
                    .memRows = after.memTables,
                    .diskRows = count - after.memTables,
                    .permits = maxMemTables - after.memTables,
                }, after);
                return err;
            };
            try testing.expectEqualDeep(SkewedMergeFixture.State{
                .diskTables = recorder.diskTables.items.len,
                .diskRows = count,
                .permits = maxMemTables,
            }, fixture.state());
        }
    }.f;
    inline for ([_]usize{ maxMemTables / 2, maxMemTables }) |count| {
        try checkAllAllocationFailures(a, f, .{count});
    }
}

test "tablesMerger clears index pressure merge flags after allocation failure at capacity" {
    const a = testing.allocator;
    const f = struct {
        fn f(alloc: Allocator) !void {
            const backingAlloc = testing.allocator;
            var fixture = try SkewedMergeFixture.init(backingAlloc, maxMemTables);
            defer fixture.deinit();
            const before = fixture.state();
            fixture.recorder.tablesMerger(testing.io, alloc, .mem) catch |err| {
                try testing.expectEqual(error.OutOfMemory, err);
                try testing.expectEqualDeep(before, fixture.state());
                return err;
            };
            try testing.expectEqualDeep(SkewedMergeFixture.State{
                .memTables = 1,
                .memRows = maxMemTables,
                .permits = maxMemTables - 1,
            }, fixture.state());
        }
    }.f;
    try checkAllAllocationFailures(a, f, .{});
}

test "tablesMerger leaves skewed index memory tables below capacity" {
    const alloc = testing.allocator;
    for ([_]usize{ maxMemTables - 2, maxMemTables - 1 }) |count| {
        var fixture = try SkewedMergeFixture.init(alloc, count);
        defer fixture.deinit();
        var windowBuf: [amountOfTablesToMerge]*Table = undefined;
        try testing.expect(merger.filterTablesToMerge(fixture.recorder.memTables.items, &windowBuf, std.math.maxInt(u64), .leveled) == null);
        try fixture.recorder.tablesMerger(testing.io, alloc, .mem);
        try testing.expectEqualDeep(SkewedMergeFixture.State{
            .memTables = count,
            .memRows = count,
            .permits = maxMemTables - count,
        }, fixture.state());
    }
}

test "tablesMerger merges skewed index memory tables at capacity without forcing disk output" {
    const alloc = testing.allocator;
    var fixture = try SkewedMergeFixture.init(alloc, maxMemTables);
    defer fixture.deinit();
    var windowBuf: [amountOfTablesToMerge]*Table = undefined;
    try testing.expect(merger.filterTablesToMerge(fixture.recorder.memTables.items, &windowBuf, std.math.maxInt(u64), .leveled) == null);
    try fixture.recorder.tablesMerger(testing.io, alloc, .mem);
    try testing.expectEqualDeep(SkewedMergeFixture.State{
        .memTables = 1,
        .memRows = maxMemTables,
        .permits = maxMemTables - 1,
    }, fixture.state());
}

test "tablesMerger on index disk tables does not flush memory tables" {
    const alloc = testing.allocator;
    for ([_]usize{ maxMemTables - 1, maxMemTables }) |count| {
        var fixture = try SkewedMergeFixture.init(alloc, count);
        defer fixture.deinit();
        const before = fixture.state();

        try fixture.recorder.tablesMerger(testing.io, alloc, .disk);
        try testing.expectEqualDeep(before, fixture.state());
    }
}

test "shutdown index recorder closes the countdown to stop an executing merge" {
    const f = struct {
        fn run(skipAllocations: usize) !void {
            const alloc = testing.allocator;
            var fixture = try SkewedMergeFixture.init(alloc, maxMemTables);
            defer fixture.deinit();
            const before = fixture.state();
            try shutdownTesting.expectRunningMergeStops(fixture.recorder, skipAllocations);
            try testing.expectEqualDeep(before, fixture.state());
        }
    }.run;
    try f(1);
    try f(2);
}

// drain must join timer publishers and release timer owned references,
// the remaining memory row checks ownership, not a ban on already running flush
test "shutdown index recorder joins timer publication before releasing retained tables" {
    const f = struct {
        fn publish(recorder: *IndexRecorder, table: *Table) anyerror!void {
            const alloc = testing.allocator;
            try recorder.addToMemTables(testing.io, alloc, table, true);
        }
        fn run(failAllocation: bool) !void {
            const alloc = testing.allocator;
            const io = testing.io;
            var fixture = try SkewedMergeFixture.init(alloc, 0);
            defer fixture.deinit();
            const table = try createMemTableFromItems(io, alloc, &.{"timer-publication"});
            try shutdownTesting.expectTimerPublicationReserved(fixture.recorder, publish, .{ fixture.recorder, table }, failAllocation);
            try testing.expectEqual(1, countMemItemsInRecorder(fixture.recorder));
        }
    }.run;
    try f(false);
    try f(true);
}

test "shutdown index recorder drains queued merge tasks" {
    const f = struct {
        fn run(count: usize) !void {
            const alloc = testing.allocator;
            var fixture = try SkewedMergeFixture.init(alloc, 0);
            defer fixture.deinit();
            try shutdownTesting.expectQueuedWorkDrained(fixture.recorder, count);
        }
    }.run;
    try f(1);
    try f(3);
}

// submission must reserve a job before allocation so shutdown cannot miss an admitted but unscheduled task,
// both successful allocation and OOM must release that reservation; this checks admission accounting, not whether the merge runs.
test "shutdown index recorder reserves submissions before allocation" {
    const f = struct {
        fn run(alloc: Allocator) !void {
            const backingAlloc = testing.allocator;
            var fixture = try SkewedMergeFixture.init(backingAlloc, 0);
            defer fixture.deinit();
            try shutdownTesting.expectSubmissionReserved(fixture.recorder, alloc);
        }
    }.run;
    const alloc = testing.allocator;
    try testing.checkAllAllocationFailures(alloc, f, .{});
}

test "index capacity pressure merge survives shutdown and reopening" {
    const alloc = testing.allocator;
    const io = testing.io;
    var fixture = try SkewedMergeFixture.init(alloc, maxMemTables);
    defer fixture.deinit();
    try fixture.recorder.tablesMerger(io, alloc, .mem);
    try testing.expectEqualDeep(SkewedMergeFixture.State{
        .memTables = 1,
        .memRows = maxMemTables,
        .permits = maxMemTables - 1,
    }, fixture.state());
    try fixture.recorder.shutdown(io, alloc);
    const reopened = try fixture.openRecorder();
    defer reopened.deinit(io, alloc);
    defer reopened.shutdown(io, alloc) catch unreachable;
    try testing.expectEqual(1, reopened.diskTables.items.len);
    const table = reopened.diskTables.items[0];
    try testing.expectEqualDeep(.{ false, maxMemTables }, .{ table.inMerge, table.tableHeader().entriesCount });
    const cache = try Cache(*MemBlock).init(io, alloc, .{ .meter = .{ .name = "" } });
    defer cache.deinit();
    var lookup = LookupTable.init(alloc, table, Conf.getConf().app.maxIndexMemBlockSize, cache, fixture.decompressionPool);
    defer lookup.deinit(alloc);
    try lookup.seek(io, alloc, SkewedMergeFixture.rows[0]);
    for (SkewedMergeFixture.rows) |row| {
        try testing.expect(try lookup.next(io, alloc));
        try testing.expectEqualStrings(row, lookup.current);
    }
    try testing.expect(!try lookup.next(io, alloc));
}

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
    defer {
        recorder.deinit(io, alloc);
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

    try recorder.shutdown(io, alloc);
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
    defer {
        recorder.deinit(io, alloc);
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

    try recorder.shutdown(io, alloc);
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
        defer {
            recorder.deinit(io, alloc);
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

        try recorder.shutdown(io, alloc);
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
        defer {
            reopened.deinit(io, alloc);
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
    defer {
        recorder.deinit(io, alloc);
        timerLoop.stop();
        timerLoop.join();
        timerLoop.deinit();
    }
    recorder.maxMemBlockSize = 256;
    try startTasks(recorder, io, alloc);

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
    try recorder.shutdown(io, alloc);

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
    defer {
        recorder.deinit(io, alloc);
        timerLoop.stop();
        timerLoop.join();
        timerLoop.deinit();
    }
    try startTasks(recorder, io, alloc);

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
    try recorder.shutdown(io, alloc);

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
    defer {
        recorder.deinit(io, alloc);
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

    recorder.halt(io);
    defer {
        recorder.deinit(io, alloc);
        timerLoop.stop();
        timerLoop.join();
        timerLoop.deinit();
    }
    recorder.jobs.wait(io);
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
    defer {
        recorder.deinit(io, alloc);
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
    try recorder.shutdown(io, alloc);
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
    defer {
        recorder.deinit(io, alloc);
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

    try recorder.shutdown(io, alloc);
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
    defer {
        recorder.deinit(io, alloc);
        timerLoop.stop();
        timerLoop.join();
        timerLoop.deinit();
    }
    recorder.maxMemBlockSize = maxIndexMemBlockSize;

    try recorder.add(io, alloc, testEntries);

    try testing.expectEqual(totalEntries - countAdditionalEntries, recorder.blocksToFlush.items.len);

    try recorder.shutdown(io, alloc);

    try testing.expectEqual(@as(usize, 0), recorder.memTables.items.len);
    try testing.expect(recorder.diskTables.items.len > 0);
    try testing.expectEqual(@as(u64, 0), countMemItemsInRecorder(recorder));
    try testing.expectEqual(@as(u64, totalEntries), countDiskItemsInRecorder(recorder));
}

test "addToMemTables publishes incoming rows when forced flush makes the second wait succeed" {
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
    defer {
        recorder.deinit(io, alloc);
        timerLoop.stop();
        timerLoop.join();
        timerLoop.deinit();
    }
    defer recorder.shutdown(io, alloc) catch unreachable;
    recorder.halt(io);

    for (0..maxMemTables) |i| {
        const table = try createMemTableFromItems(io, alloc, &.{stableItems[i % stableItems.len]});
        recorder.memTablesSem.waitUncancelable(io);
        try recorder.memTables.append(alloc, table);
    }
    try testing.expectEqual(0, recorder.memTablesSem.permits);

    const incomingRows = [_][]const u8{ "incoming-a", "incoming-b" };
    const incoming = try createMemTableFromItems(io, alloc, &incomingRows);
    try recorder.addToMemTables(io, alloc, incoming, true);

    const State = struct {
        memTables: usize,
        memRows: u64,
        diskRows: u64,
        permits: usize,
        timerArms: usize,
        pendingMerges: usize,
        cacheVersion: u32,
        needInvalidate: bool,
    };
    try testing.expectEqualDeep(State{
        .memTables = 1,
        .memRows = incomingRows.len,
        .diskRows = maxMemTables,
        // all the tables are flushed, but 1 the last is occupied via addToMemTables
        .permits = maxMemTables - 1,
        .timerArms = 0,
        .pendingMerges = 0,
        .cacheVersion = 1,
        .needInvalidate = false,
    }, State{
        .memTables = recorder.memTables.items.len,
        .memRows = countMemItemsInRecorder(recorder),
        .diskRows = countDiskItemsInRecorder(recorder),
        .permits = recorder.memTablesSem.permits,
        .timerArms = recorder.pendingTableArms.items.len,
        .pendingMerges = recorder.jobs.pending,
        .cacheVersion = recorder.indexCacheKeyVersion.load(.acquire),
        .needInvalidate = recorder.needInvalidate.load(.acquire),
    });
    try testing.expectEqual(incoming, recorder.memTables.items[0]);
    try testing.expectEqual(1, incoming.refCounter.load(.acquire));
    incoming.retain();
    defer incoming.release(io);

    try recorder.flushForce(io, alloc);
    try testing.expectEqual(0, recorder.memTables.items.len);
    try testing.expectEqual(maxMemTables, recorder.memTablesSem.permits);
    try testing.expectEqual(maxMemTables + incomingRows.len, countDiskItemsInRecorder(recorder));
    try testing.expect(incoming.toRemove.load(.acquire));
    try testing.expectEqual(1, incoming.refCounter.load(.acquire));

    var reopened = try Table.openAll(io, alloc, rootPath, decompressionPool);
    defer {
        for (reopened.items) |table| table.release(io);
        reopened.deinit(alloc);
    }
    const cache = try Cache(*MemBlock).init(io, alloc, .{ .meter = .{ .name = "" } });
    defer cache.deinit();

    var reopenedRows: u64 = 0;
    var foundIncoming = false;
    for (reopened.items) |table| {
        reopenedRows += table.tableHeader().entriesCount;
        if (!std.mem.eql(u8, table.tableHeader().firstEntry, incomingRows[0])) continue;
        foundIncoming = true;

        var lookup = LookupTable.init(alloc, table, Conf.getConf().app.maxIndexMemBlockSize, cache, decompressionPool);
        defer lookup.deinit(alloc);
        try lookup.seek(io, alloc, incomingRows[0]);
        for (incomingRows) |row| {
            try testing.expect(try lookup.next(io, alloc));
            try testing.expectEqualStrings(row, lookup.current);
        }
        try testing.expect(!try lookup.next(io, alloc));
    }
    try testing.expectEqual(maxMemTables + incomingRows.len, reopenedRows);
    try testing.expect(foundIncoming);
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
    defer {
        recorder.deinit(io, alloc);
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

test "data table timer retains and rearms an early callback" {
    const State = struct {
        retainsTable: bool,
        references: u32,
        completion: xev.CompletionState,
        inMerge: bool,
    };

    // keep deadlines ahead of execution so scheduler rearms
    for ([_]i64{ 10, 60 }) |remainingSeconds| {
        var loop = try xev.Loop.init(.{});
        defer loop.deinit();
        var recorder: IndexRecorder = undefined;
        recorder.taskCtx = .{ .recorder = &recorder, .io = testing.io, .alloc = testing.allocator };
        recorder.jobs = .{};
        recorder.mxTables = .init;
        var mem: MemTable = undefined;
        mem.flushAtUs = Io.Timestamp.now(testing.io, .real).toMicroseconds() + remainingSeconds * std.time.us_per_s;
        var table: Table = undefined;
        table.inner = .{ .mem = &mem };
        table.inMerge = false;
        // one reference belongs to the table owner, the other to the timer slot.
        table.refCounter = .init(2);
        var slot: TableTimerSlot = .{ .recorder = &recorder, .xevTimer = try xev.Timer.init(), .table = &table };
        defer slot.xevTimer.deinit();

        try testing.expectEqual(.dead, slot.completion.state());
        try testing.expectEqual(.disarm, tableTimerCallback(&slot, &loop, &slot.completion, {}));
        try testing.expectEqualDeep(State{
            .retainsTable = true,
            .references = 2,
            .completion = .active,
            .inMerge = false,
        }, State{
            .retainsTable = slot.table == &table,
            .references = table.refCounter.load(.acquire),
            .completion = slot.completion.state(),
            .inMerge = table.inMerge,
        });
    }
}
