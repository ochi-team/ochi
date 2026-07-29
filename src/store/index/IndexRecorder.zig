const std = @import("std");
const Allocator = std.mem.Allocator;
const Io = std.Io;
const xev = @import("xev");

const fs = @import("../../fs.zig");

const cap = @import("../table/cap.zig");

const Cache = @import("../../stds/Cache.zig").Cache;
const Entries = @import("Entries.zig");
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

const IndexRecorder = @This();

const TaskCtx = struct {
    recorder: *IndexRecorder,
    io: Io,
    alloc: Allocator,
};

const MergeTask = struct {
    task: xev.ThreadPool.Task,
    ctx: TaskCtx,
    run: *const fn (*IndexRecorder, Io, Allocator) void,

    fn callback(t: *xev.ThreadPool.Task) void {
        const self: *MergeTask = @fieldParentPtr("task", t);
        self.run(self.ctx.recorder, self.ctx.io, self.ctx.alloc);

        const recorder = self.ctx.recorder;
        const alloc = self.ctx.alloc;
        alloc.destroy(self);
        _ = recorder.pendingMerges.fetchSub(1, .release);
    }
};

// fixed pool of one-shot deadline timers for mem tables: memTablesSem bounds the
// number of live mem tables to maxMemTables, so a slot is always available.
// The slot's address is stable for the process lifetime.
const TableTimerSlot = struct {
    recorder: *IndexRecorder = undefined,
    xevTimer: xev.Timer,
    completion: xev.Completion = .{},
    cancelCompletion: xev.Completion = .{},
    table: ?*Table = null,
};

entries: *Entries,

// accumulated memblocks prepared to flush
blocksToFlush: std.ArrayList(*MemBlock),
// reused buffer for the periodic mem block flusher
flushBlocksDestination: std.ArrayList(*MemBlock),
// mxBlocks lock is used to provide access to blocksToFlush and flushBlocksDestination
mxBlocks: Io.Mutex = .init,
// TODO: make it as atomic instead of locking to access this value,
// we still need mutex to access blocksToFlush (mxBlocks)
flushEntriesAtUs: i64 = std.math.maxInt(i64),
blocksThresholdToFlush: u32,

// config fields
// TODO: make it as a config access instead of a field
maxMemBlockSize: u32,

mxTables: Io.Mutex = .init,
diskTables: std.ArrayList(*Table),
memTables: std.ArrayList(*Table),

concurrency: u16,
diskMergeSem: Io.Semaphore,
memMergeSem: Io.Semaphore,

stopped: Stop = .{},
timerLoop: *TimerLoop,
taskCtx: TaskCtx = undefined,
mergePool: *xev.ThreadPool,
pendingMerges: std.atomic.Value(usize) = .init(0),

// per-object deadline scheduling: arm requests come from arbitrary threads
// (addToMemTables, merge workers) and must be applied to timerLoop.loop only from
// its own thread, so they're queued here and drained via timerLoop's own
// wake handler instead of standing up a second xev.Async.
pendingDeadlineMx: std.atomic.Mutex = .unlocked,
pendingTableArms: std.ArrayList(*Table) = .empty,
tableTimerSlots: [maxMemTables]TableTimerSlot = undefined,
// limits amount of mem tables in order to handle too high ingestion rate,
// when mem tables are not merged fast enough
// TODO: find an optimal way to handle ingestion rate higher than merge rate
// 1. throttle ingestion: sub optimal
// 2. extend limit of inmemory tables
// 3. find a way to make flushing / merging more optimal
// 4. more aggresive memory merging
memTablesSem: Io.Semaphore = .{
    .permits = maxMemTables,
},

needInvalidate: std.atomic.Value(bool) = .init(false),
indexCacheKeyVersion: std.atomic.Value(u64) = .init(0),

mergeIdx: std.atomic.Value(u64),
path: []const u8,
runtime: *Runtime,
compressionPool: *CompressionPool,
decompressionPool: *DecompressionPool,

pub fn init(
    io: Io,
    alloc: Allocator,
    path: []const u8,
    runtime: *Runtime,
    compressionPool: *CompressionPool,
    decompressionPool: *DecompressionPool,
    mergePool: *xev.ThreadPool,
    timerLoop: *TimerLoop,
) !*IndexRecorder {
    std.debug.assert(std.fs.path.isAbsolute(path));
    std.debug.assert(path[path.len - 1] != std.fs.path.sep);

    const concurrency = runtime.cpus;

    const entries = try Entries.init(alloc, concurrency);
    errdefer entries.deinit(alloc);

    const blocksThresholdToFlush: u32 = @intCast(entries.shards.len * Entries.maxBlocksPerShard);

    // TODO: try using list of lists instead in order not to copy data from blocks to blocksToFlush
    var blocksToFlush = try std.ArrayList(*MemBlock).initCapacity(alloc, blocksThresholdToFlush);
    errdefer blocksToFlush.deinit(alloc);

    var memTables = try std.ArrayList(*Table).initCapacity(alloc, maxMemTables);
    errdefer memTables.deinit(alloc);

    var tables = try Table.openAll(io, alloc, path, decompressionPool);
    errdefer {
        for (tables.items) |table| table.close(io);
        tables.deinit(alloc);
    }

    var flushBlocksDestination = try std.ArrayList(*MemBlock).initCapacity(alloc, blocksThresholdToFlush);
    errdefer flushBlocksDestination.deinit(alloc);

    var tableTimerSlots: [maxMemTables]TableTimerSlot = undefined;
    for (&tableTimerSlots) |*slot| slot.* = .{ .xevTimer = try xev.Timer.init() };

    const t = try alloc.create(IndexRecorder);
    errdefer alloc.destroy(t);
    t.* = .{
        .entries = entries,
        .blocksThresholdToFlush = blocksThresholdToFlush,
        .blocksToFlush = blocksToFlush,
        .flushBlocksDestination = flushBlocksDestination,
        .maxMemBlockSize = Conf.getConf().app.maxIndexMemBlockSize,
        .diskTables = tables,
        .memTables = memTables,
        .mergeIdx = .init(@intCast(Io.Timestamp.now(io, .real).nanoseconds)),
        .path = path,
        .runtime = runtime,
        .compressionPool = compressionPool,
        .decompressionPool = decompressionPool,
        .timerLoop = timerLoop,
        .mergePool = mergePool,
        .concurrency = concurrency,
        .diskMergeSem = .{
            .permits = @max(4, concurrency),
        },
        .memMergeSem = .{
            .permits = @max(4, concurrency),
        },
        .tableTimerSlots = tableTimerSlots,
    };
    t.taskCtx = .{ .recorder = t, .io = io, .alloc = alloc };
    for (&t.tableTimerSlots) |*slot| slot.recorder = t;

    return t;
}

pub fn createDir(io: Io, path: []const u8) !void {
    try fs.createDirAssert(io, path);
    try fs.syncPathAndParentDir(io, path);
}

pub fn startTasks(self: *IndexRecorder, io: Io, alloc: Allocator) !void {
    // disk tables merge task is different,
    // it doesn't run infinitely, but runs a few merge cycles to process left overs
    // from the previous launches
    for (0..self.concurrency) |_| {
        try self.startDiskTablesMerge(io, alloc);
    }

    try self.timerLoop.addWakeHandler(self, deadlineWakeHandler);

    try self.timerLoop.addTimer(std.time.ns_per_s, &self.taskCtx, memBlockFlusherTick);
    try self.timerLoop.start();
}

// TODO: find an approach to make it never fail,
// the only option it fails is OOM, so cleaning more memory in advance might be more reliable
// another problem it's hard to test it via checkAllAllocationFailures
pub fn stop(self: *IndexRecorder, io: Io, alloc: Allocator) !void {
    self.stopped.stop(io);
    self.waitForMergesToDrain(io);

    try self.flushForce(io, alloc);
}

pub fn flushForce(self: *IndexRecorder, io: Io, alloc: Allocator) !void {
    // pass empty destination because we don't plan to push more data in
    var blocksDestination = std.ArrayList(*MemBlock).empty;
    defer blocksDestination.deinit(alloc);

    try self.flushMemEntries(io, alloc, &blocksDestination, true);
    try self.flushMemTables(io, alloc, true);
}

// TODO: this must assert there is no data inmemory or it flushes it immediately
// entires, blocks, memtables
pub fn deinit(self: *IndexRecorder, io: Io, alloc: Allocator) void {
    self.waitForMergesToDrain(io);

    std.debug.assert(self.blocksToFlush.items.len == 0);
    std.debug.assert(self.memTables.items.len == 0);

    // requestTableTimer retains its table for the timer's lifetime; if the timer
    // never got to fire (queued but never drained, e.g. startTasks was never
    // called, or timerLoop's thread already stopped/joined before deinit), that
    // extra ref would otherwise leak the table forever.
    for (self.pendingTableArms.items) |table| table.release(io);
    for (&self.tableTimerSlots) |*slot| {
        if (slot.table) |table| table.release(io);
    }
    self.pendingTableArms.deinit(alloc);

    for (self.blocksToFlush.items) |block| {
        block.deinit(alloc);
    }

    for (self.diskTables.items) |table| {
        table.release(io);
    }
    for (self.memTables.items) |table| {
        table.release(io);
    }

    self.entries.deinit(alloc);
    self.blocksToFlush.deinit(alloc);
    self.flushBlocksDestination.deinit(alloc);
    self.diskTables.deinit(alloc);
    self.memTables.deinit(alloc);
    self.* = undefined;
    alloc.destroy(self);
}

pub fn nextMergeIdx(self: *IndexRecorder) u64 {
    return self.mergeIdx.fetchAdd(1, .monotonic);
}

pub fn add(self: *IndexRecorder, io: Io, alloc: Allocator, entries: []const []const u8) !void {
    var entryIndex: usize = 0;

    while (entryIndex < entries.len) {
        const shard = self.entries.next();
        const blocksListResult = try shard.add(io, alloc, entries[entryIndex..], self.maxMemBlockSize);

        var blocksList = blocksListResult orelse return;
        defer blocksList.blocksToFlush.deinit(alloc);

        try self.flushBlocks(io, alloc, blocksList.blocksToFlush.items);
        entryIndex += blocksList.gatheredEntriesCount;
    }
}

pub fn collectTables(self: *IndexRecorder, io: Io, alloc: Allocator, dst: *std.ArrayList(*Table)) !void {
    self.mxTables.lockUncancelable(io);
    defer self.mxTables.unlock(io);

    const tablesLen = self.memTables.items.len + self.diskTables.items.len;
    try dst.ensureUnusedCapacity(alloc, tablesLen);

    for (self.memTables.items) |table| {
        table.retain();
        dst.appendAssumeCapacity(table);
    }
    for (self.diskTables.items) |table| {
        table.retain();
        dst.appendAssumeCapacity(table);
    }
}

fn flushBlocks(self: *IndexRecorder, io: Io, alloc: Allocator, blocks: []*MemBlock) !void {
    if (blocks.len == 0) return;

    // TODO: make a more narrow locking, ideally before we make flushEntriesAt field as atomic
    self.mxBlocks.lockUncancelable(io);
    defer self.mxBlocks.unlock(io);

    if (self.blocksToFlush.items.len == 0) {
        self.flushEntriesAtUs = Io.Timestamp.now(io, .real).toMicroseconds() + std.time.us_per_s;
    }

    try self.blocksToFlush.appendSlice(alloc, blocks);
    if (self.blocksToFlush.items.len >= self.blocksThresholdToFlush) {
        // TODO: metric how much capacity is actual capacity of it comparing to expected
        // TODO: this slice could have come out of a mem pool
        // and pops on demand
        var blocksToFlush = try std.ArrayList(*MemBlock).initCapacity(alloc, self.blocksToFlush.items.len);
        std.mem.swap(std.ArrayList(*MemBlock), &blocksToFlush, &self.blocksToFlush);
        defer blocksToFlush.deinit(alloc);

        try self.flushBlocksToMemTables(io, alloc, blocksToFlush.items, false);
    }
}

fn flushBlocksToMemTables(self: *IndexRecorder, io: Io, alloc: Allocator, blocks: []*MemBlock, force: bool) !void {
    std.debug.assert(blocks.len > 0);
    var tail = blocks[0..];
    errdefer for (tail) |block| block.deinit(alloc);

    // enough for 256 tables, which a way beyond the expected amount
    var fba = std.heap.stackFallback(2048, alloc);
    const fbaAlloc = fba.get();

    const tablesSize = (blocks.len + blocksInMemTable - 1) / blocksInMemTable;
    var memTables = try std.ArrayList(*Table).initCapacity(fbaAlloc, tablesSize);
    defer memTables.deinit(fbaAlloc);
    errdefer {
        for (memTables.items) |memTable| memTable.close(io);
    }

    // TODO: benchmark parallel mem table creation
    while (tail.len > 0) {
        const offset = @min(blocksInMemTable, tail.len);
        const head = tail[0..offset];
        tail = tail[offset..];

        const memTable = try MemTable.init(io, alloc, head, self.compressionPool, self.decompressionPool);
        const t = try Table.fromMem(io, alloc, memTable, self.decompressionPool);
        memTables.appendAssumeCapacity(t);
    }

    const maxSize = merger.getMaxInmemoryTableSize(self.runtime.cacheSize);

    var left = try std.ArrayList(*Table).initCapacity(fbaAlloc, memTables.items.len);
    defer left.deinit(fbaAlloc);

    // TODO: consider skipping this step and directly append tables to its collection,
    // it requires another way to handle mem tables semaphore,
    // but might reduce the load on merging small tables
    while (memTables.items.len > 1) {
        try self.mergeMemTables(io, alloc, &memTables);

        for (memTables.items) |table| {
            if (table.size >= maxSize) {
                try self.addToMemTables(io, alloc, table, force);
            } else {
                left.appendAssumeCapacity(table);
            }
        }

        memTables.clearRetainingCapacity();
        std.mem.swap(std.ArrayList(*Table), &memTables, &left);
    }

    if (memTables.items.len == 1) {
        try self.addToMemTables(io, alloc, memTables.items[0], force);
    }
}

/// merges mem tables to a bigger size ones
/// requires same Allocator that's used to create them,
/// because it deinits the merged ones
fn mergeMemTables(self: *IndexRecorder, io: Io, alloc: Allocator, memTables: *std.ArrayList(*Table)) !void {
    // TODO: run merging job in parallel and benchmark whether it doesn't hurt general throughput

    var fba = std.heap.stackFallback(512, alloc);
    const fbaAlloc = fba.get();
    var mergedTables = try std.ArrayList(*Table).initCapacity(fbaAlloc, 8);
    defer mergedTables.deinit(fbaAlloc);

    var memToMerge = try std.ArrayList(*Table).initCapacity(fbaAlloc, 8);
    defer memToMerge.deinit(fbaAlloc);

    std.debug.assert(memTables.items.len != 0);
    if (memTables.items.len == 1) return;

    var left = memTables.items[0..];
    while (left.len > 0) {
        const n = merger.selectTablesToMerge(left);
        const toMerge = left[0..n];
        left = left[n..];

        try memToMerge.ensureUnusedCapacity(fbaAlloc, toMerge.len);
        for (toMerge) |table| {
            std.debug.assert(table.inner == .mem);
            memToMerge.appendAssumeCapacity(table);
        }

        // TODO: I don't need it, we already have merging from []*Table, replace it
        // or document the reasoning
        const res = try MemTable.mergeMemTables(io, alloc, memToMerge.items, self.compressionPool, self.decompressionPool);
        memToMerge.clearRetainingCapacity();

        for (toMerge) |t| t.close(io);
        const t = try Table.fromMem(io, alloc, res, self.decompressionPool);
        try mergedTables.append(fbaAlloc, t);
    }

    // TODO: make it in place overwriting instead of holding a copy on stack
    memTables.clearRetainingCapacity();
    memTables.appendSliceAssumeCapacity(mergedTables.items);
}

// TODO: replace to std Semaphore.waitTimout:
// https://codeberg.org/ziglang/zig/commit/21980c82f48f239e50239d5af264706d15829268
pub fn timedWait(sem: *Io.Semaphore, io: Io, timeout_ns: u64) !void {
    sem.mutex.lockUncancelable(io);
    defer sem.mutex.unlock(io);

    while (sem.permits == 0) {
        const elapsed = std.Io.Timestamp.now(io, .real).nanoseconds;
        if (elapsed > timeout_ns)
            return error.Timeout;

        sem.cond.waitUncancelable(io, &sem.mutex);
    }

    sem.permits -= 1;
    if (sem.permits > 0)
        sem.cond.signal(io);
}

fn addToMemTables(self: *IndexRecorder, io: Io, alloc: Allocator, memTable: *Table, force: bool) !void {
    timedWait(&self.memTablesSem, io, std.time.ns_per_s / 5) catch |err| {
        errdefer memTable.release(io);

        switch (err) {
            error.Timeout => {
                if (self.stopped.isStopped() and !force) {
                    return error.Stopped;
                }

                try self.flushMemTables(io, alloc, true);

                // if the first sem wait couldn't free the space it times out
                // and must flush to disk as is,
                timedWait(&self.memTablesSem, io, std.time.ns_per_s * 3) catch |e| {
                    switch (e) {
                        error.Timeout => {
                            Logger.log(.warn, "index: mem tables buffer is full, flush mem table", .{});

                            const destinationTablePath = try self.diskTablePath(alloc, .disk);
                            errdefer if (destinationTablePath.len > 0) alloc.free(destinationTablePath);

                            // pass empty list tables because we have nothing to merge/replace,
                            // it must only flush to disk a passed mem table and not remove existing tables,
                            // but perform semaphore
                            try self.flushMemTable(io, alloc, memTable.inner.mem, &[_]*Table{}, destinationTablePath, .disk);
                            memTable.release(io);
                        },
                    }
                };

                return;
            },
        }
    };

    {
        self.mxTables.lockUncancelable(io);
        defer self.mxTables.unlock(io);

        errdefer self.memTablesSem.post(io);
        errdefer memTable.release(io);

        try self.memTables.append(alloc, memTable);
    }

    self.requestTableTimer(memTable);
    try self.startMemTablesMerge(io, alloc);

    if (force) {
        self.invalidateStreamFilterCache();
    } else {
        _ = self.needInvalidate.cmpxchgStrong(false, true, .acq_rel, .acquire);
    }
}

// merge-flush
// the functions below describe merge/flush jobs
// the naming is grouped on the following levels
// 1. startX - starts an infinite (or limited) cycle of a task
// 2. runX - runs a given task that MUST be able to complete without stopped signal,
// it has a specific error handling and stopped signal

fn submitMergeTask(
    self: *IndexRecorder,
    io: Io,
    alloc: Allocator,
    run: *const fn (*IndexRecorder, Io, Allocator) void,
) !void {
    if (self.stopped.isStopped()) return;

    const t = try alloc.create(MergeTask);
    errdefer alloc.destroy(t);

    t.* = .{
        .task = .{ .callback = MergeTask.callback },
        .ctx = .{
            .recorder = self,
            .io = io,
            .alloc = alloc,
        },
        .run = run,
    };

    _ = self.pendingMerges.fetchAdd(1, .monotonic);
    self.mergePool.schedule(.from(&t.task));
}

fn waitForMergesToDrain(self: *IndexRecorder, io: Io) void {
    while (self.pendingMerges.load(.acquire) != 0) {
        Io.sleep(io, .fromMilliseconds(1), .real) catch {};
    }
}

fn deltaMs(deadlineUs: i64, nowUs: i64) u64 {
    if (deadlineUs <= nowUs) return 0;
    const deltaUs: u64 = @intCast(deadlineUs - nowUs);
    return deltaUs / std.time.us_per_ms;
}

// queues an arm request for the loop thread; safe to call from any thread.
// retains the table for the timer's lifetime so a merge that frees it before
// the deadline fires can't leave the timer pointing at freed memory.
fn requestTableTimer(self: *IndexRecorder, table: *Table) void {
    table.retain();

    TimerLoop.spinLock(&self.pendingDeadlineMx);
    self.pendingTableArms.append(self.taskCtx.alloc, table) catch |err| {
        self.pendingDeadlineMx.unlock();
        Logger.log(.err, "IndexRecorder: failed to queue table timer arm", .{ .err = err });
        table.release(self.taskCtx.io);
        return;
    };
    self.pendingDeadlineMx.unlock();

    self.timerLoop.notify();
}

fn deadlineWakeHandler(ctx: *anyopaque, loop: *xev.Loop) void {
    const self: *IndexRecorder = @ptrCast(@alignCast(ctx));
    if (self.stopped.isStopped()) return;

    var tableArms: std.ArrayList(*Table) = undefined;
    {
        TimerLoop.spinLock(&self.pendingDeadlineMx);
        defer self.pendingDeadlineMx.unlock();
        tableArms = self.pendingTableArms;
        self.pendingTableArms = .empty;
    }
    defer tableArms.deinit(self.taskCtx.alloc);

    for (tableArms.items) |table| self.armTableTimer(loop, table);
}

fn armTableTimer(self: *IndexRecorder, loop: *xev.Loop, table: *Table) void {
    const io = self.taskCtx.io;

    self.mxTables.lockUncancelable(io);
    const found = for (&self.tableTimerSlots) |*s| {
        if (s.table == null or s.table.?.inMerge) break s;
    } else null;
    self.mxTables.unlock(io);

    const slot = found orelse {
        Logger.log(.err, "IndexRecorder: no free table timer slot, dropping scheduled flush", .{});
        table.release(io);
        return;
    };

    if (slot.table) |stale| stale.release(io);
    slot.table = table;

    const nowUs = Io.Timestamp.now(self.taskCtx.io, .real).toMicroseconds();
    const delayMs = deltaMs(table.inner.mem.flushAtUs, nowUs);
    slot.xevTimer.reset(loop, &slot.completion, &slot.cancelCompletion, delayMs, TableTimerSlot, slot, tableTimerCallback);
}

fn tableTimerCallback(
    ud: ?*TableTimerSlot,
    loop: *xev.Loop,
    c: *xev.Completion,
    r: xev.Timer.RunError!void,
) xev.CallbackAction {
    _ = loop;
    _ = c;
    _ = r catch {};
    const slot = ud.?;
    const self = slot.recorder;
    const table = slot.table.?;
    const io = self.taskCtx.io;

    defer {
        table.release(io);
        slot.table = null;
    }

    if (self.stopped.isStopped()) return .disarm;

    const nowUs = Io.Timestamp.now(io, .real).toMicroseconds();

    self.mxTables.lockUncancelable(io);
    const shouldFlush = !table.inMerge and table.inner.mem.flushAtUs <= nowUs;
    if (shouldFlush) table.inMerge = true;
    self.mxTables.unlock(io);

    if (shouldFlush) {
        var tables = [_]*Table{table};
        self.mergeTables(io, self.taskCtx.alloc, tables[0..], true, null) catch |err| {
            self.stopped.stop(io);
            Logger.log(.err, "failed to run scheduled mem table flush", .{ .err = err });
        };
    }

    return .disarm;
}

pub fn startDiskTablesMerge(self: *IndexRecorder, io: Io, alloc: Allocator) !void {
    try self.submitMergeTask(io, alloc, runDiskTablesMerger);
}

fn runDiskTablesMerger(self: *IndexRecorder, io: Io, alloc: Allocator) void {
    self.tablesMerger(io, alloc, &self.diskTables, &self.diskMergeSem) catch |err| {
        if (err == error.Stopped) return;

        self.stopped.stop(io);
        Logger.log(.err, "failed to run disk tables merger", .{ .err = err });
    };
}

fn memBlockFlusherTick(ctx: *anyopaque) void {
    const tickCtx: *TaskCtx = @ptrCast(@alignCast(ctx));
    const self = tickCtx.recorder;

    if (self.stopped.isStopped()) return;

    self.flushMemEntries(tickCtx.io, tickCtx.alloc, &self.flushBlocksDestination, false) catch |err| {
        if (err == error.Stopped) return;

        self.stopped.stop(tickCtx.io);
        Logger.log(.err, "unexpected error on running mem blocks flusher", .{ .err = err });
        return;
    };
    self.flushBlocksDestination.clearRetainingCapacity();
}

/// it's not supposed to run at the beginning in backrgound,
/// we run it only on demand
pub fn startMemTablesMerge(self: *IndexRecorder, io: Io, alloc: Allocator) !void {
    try self.submitMergeTask(io, alloc, runMemTablesMerge);
}

fn runMemTablesMerge(self: *IndexRecorder, io: Io, alloc: Allocator) void {
    self.tablesMerger(io, alloc, &self.memTables, &self.memMergeSem) catch |err| {
        if (err == error.Stopped) return;

        self.stopped.stop(io);
        Logger.log(.err, "failed to merge mem tables", .{ .err = err });
    };
}

fn flushMemTables(self: *IndexRecorder, io: Io, alloc: Allocator, force: bool) !void {
    const nowUs = Io.Timestamp.now(io, .real).toMicroseconds();

    var toFlushBuffer: [maxMemTables]*Table = undefined;
    var toFlush = std.ArrayList(*Table).initBuffer(&toFlushBuffer);

    self.mxTables.lockUncancelable(io);
    for (self.memTables.items) |memTable| {
        if (!memTable.inMerge and (force or memTable.inner.mem.flushAtUs < nowUs)) {
            memTable.inMerge = true;
            toFlush.appendAssumeCapacity(memTable);
        }
    }
    self.mxTables.unlock(io);

    try self.flushMemTablesInChunks(io, alloc, toFlush);
}

fn flushMemEntries(
    self: *IndexRecorder,
    io: Io,
    alloc: Allocator,
    blocksDestination: *std.ArrayList(*MemBlock),
    force: bool,
) !void {
    const nowUs = Io.Timestamp.now(io, .real).toMicroseconds();

    self.mxBlocks.lockUncancelable(io);
    if (force or nowUs >= self.flushEntriesAtUs) {
        std.mem.swap(std.ArrayList(*MemBlock), blocksDestination, &self.blocksToFlush);
    }
    self.mxBlocks.unlock(io);

    for (self.entries.shards) |*shard| {
        try shard.collectBlocks(io, alloc, blocksDestination, nowUs, force);
    }

    if (blocksDestination.items.len > 0) try self.flushBlocksToMemTables(io, alloc, blocksDestination.items, force);
}

fn flushMemTablesInChunks(self: *IndexRecorder, io: Io, alloc: Allocator, toFlush: std.ArrayList(*Table)) !void {
    if (toFlush.items.len == 0) return;

    // TODO: consider running chunks merging in parallel
    var left = toFlush.items[0..];
    while (left.len > 0) {
        const n = merger.selectTablesToMerge(left);
        std.debug.assert(n > 0);

        // pass stopped as null since we must be able to flush data to disk
        try self.mergeTables(io, alloc, left[0..n], true, null);
        left = left[n..];
    }
}

fn tablesMerger(
    self: *IndexRecorder,
    io: Io,
    alloc: Allocator,
    tables: *std.ArrayList(*Table),
    sem: *Io.Semaphore,
) anyerror!void {
    var tablesToMergeBuf: [amountOfTablesToMerge]*Table = undefined;

    while (!self.stopped.isStopped()) {
        const maxDiskTableSize = cap.getMaxTableSize(self.runtime.getFreeDiskSpace(io));

        self.mxTables.lockUncancelable(io);
        const window = merger.filterTablesToMerge(
            tables.items,
            &tablesToMergeBuf,
            maxDiskTableSize,
        );
        self.mxTables.unlock(io);

        const filteredTablesToMerge = window orelse return;
        if (filteredTablesToMerge.len == 0) return;

        // TODO: make sure error.Stopped is handled on the upper level
        // TODO: audit all waitUncancelable, on read path it must be only cancelable
        sem.waitUncancelable(io);
        defer sem.post(io);
        try self.mergeTables(io, alloc, filteredTablesToMerge, false, &self.stopped);
    }
}

// TODO: make it used in the partition cache
fn invalidateStreamFilterCache(self: *IndexRecorder) void {
    _ = self.indexCacheKeyVersion.fetchAdd(1, .monotonic);
}

pub fn mergeTables(
    self: *IndexRecorder,
    io: Io,
    alloc: Allocator,
    tables: []*Table,
    force: bool,
    stopped: ?*const Stop,
) !void {
    std.debug.assert(tables.len > 0);
    for (tables) |table| std.debug.assert(table.inMerge);

    var swapped = false;
    defer {
        if (!swapped) {
            self.mxTables.lockUncancelable(io);
            for (tables) |table| table.inMerge = false;
            self.mxTables.unlock(io);
        }
    }

    const maxInmemoryTableSize = merger.getMaxInmemoryTableSize(self.runtime.cacheSize);
    const tableKind = merger.getDestinationTableKind(tables, force, maxInmemoryTableSize);

    const destinationTablePath = try self.diskTablePath(alloc, tableKind);
    errdefer if (destinationTablePath.len > 0) alloc.free(destinationTablePath);

    if (force and tables.len == 1 and tables[0].inner == .mem) {
        const table = tables[0].inner.mem;
        try self.flushMemTable(io, alloc, table, tables, destinationTablePath, tableKind);
        swapped = true;
        return;
    }

    var readersBuf: [amountOfTablesToMerge]*BlockReader = undefined;
    var readers = std.ArrayList(*BlockReader).initBuffer(&readersBuf);
    defer for (readers.items) |reader| reader.deinit(alloc);

    try openTableReaders(io, alloc, &readers, tables, self.decompressionPool);

    // TODO: block writer deinit might be called before it's actually created,
    // if we get rid of all the undefined's it's solved
    var newMemTable: ?*MemTable = null;
    var blockWriter: BlockWriter = blk: {
        if (tableKind == .mem) {
            newMemTable = try MemTable.empty(alloc);
            break :blk BlockWriter.initFromMemTable(newMemTable.?, self.compressionPool);
        } else {
            var sourceItemsCount: u64 = 0;
            for (tables) |table| {
                sourceItemsCount += table.tableHeader().entriesCount;
            }
            // TODO: test if we can record compressed size and make caching more reliable
            const fitsInCache = sourceItemsCount <= maxItemsPerCachedTable(self.runtime.maxMem, self.runtime.cacheSize);
            break :blk try BlockWriter.initFromDiskTable(io, destinationTablePath, fitsInCache, self.compressionPool);
        }
    };
    defer blockWriter.deinit(alloc);

    const tableHeader = MemTable.mergeBlocks(
        io,
        alloc,
        &blockWriter,
        &readers,
        stopped,
    ) catch |err| {
        switch (err) {
            // TODO: replace Stopped to Io.Cancelable.Canceled
            error.Stopped => {
                if (destinationTablePath.len > 0) {
                    fs.deleteTreeAbsolute(io, destinationTablePath) catch |deleteErr| {
                        Logger.log(.err, "failed to delete half way merged index table after stopped", .{ .err = deleteErr });
                    };
                }
                return err;
            },
            else => {
                Logger.log(.err, "failed to merge tables", .{ .err = err });
                return err;
            },
        }
    };
    if (newMemTable) |memTable| {
        memTable.tableHeader = tableHeader;
    } else {
        std.debug.assert(destinationTablePath.len > 0);
        var fbaFallback = std.heap.stackFallback(256, alloc);
        // TODO: pass table header to openining a table and use it instead of reading from a file,
        // write a test in advance to confirm it's exact same header
        defer tableHeader.deinit(alloc);
        try tableHeader.writeFile(io, fbaFallback.get(), destinationTablePath);

        try fs.syncPathAndParentDir(io, destinationTablePath);
    }

    const openTable = try openCreatedTable(io, alloc, destinationTablePath, newMemTable, self.decompressionPool);
    errdefer openTable.release(io);

    try swapper.swapTables(self, io, alloc, tables, openTable, tableKind);
    swapped = true;

    if (tableKind == .mem) self.requestTableTimer(openTable);
}

// TODO: move it to config instead of computed property
fn maxItemsPerCachedTable(maxMem: u64, cacheSize: u64) u64 {
    const restMem = maxMem - cacheSize;
    // we anticipate 6 bytes per index item in compressed form
    return @max(restMem / (6 * blocksInMemTable), merge.minMemTableSize);
}

pub fn diskTablePath(self: *IndexRecorder, alloc: Allocator, kind: TableKind) ![]const u8 {
    const destinationTablePath: []u8 =
        if (kind == .disk) blk: {
            // 1 for / and 16 for 16 bytes of idx representation,
            // we can't bitcast it to [8]u8 because we need human readlable file names
            const mergeIdx = self.nextMergeIdx();

            const path = try alloc.alloc(u8, self.path.len + 1 + 16);
            errdefer alloc.free(path);

            _ = try std.fmt.bufPrint(path, "{s}/{X:0>16}", .{ self.path, mergeIdx });

            break :blk path;
        } else "";

    return destinationTablePath;
}

pub fn flushMemTable(
    self: *IndexRecorder,
    io: Io,
    alloc: Allocator,
    memTable: *MemTable,
    tables: []*Table,
    destinationTablePath: []const u8,
    tableKind: TableKind,
) !void {
    try memTable.storeToDisk(io, alloc, destinationTablePath);

    const newTable = try openCreatedTable(io, alloc, destinationTablePath, null, self.decompressionPool);
    errdefer newTable.release(io);

    try swapper.swapTables(self, io, alloc, tables, newTable, tableKind);
}

fn openTableReaders(
    io: Io,
    alloc: Allocator,
    readers: *std.ArrayList(*BlockReader),
    tables: []*Table,
    decompressionPool: *DecompressionPool,
) !void {
    for (tables) |table| {
        const reader = switch (table.inner) {
            .mem => try BlockReader.initFromMemTable(io, alloc, table, decompressionPool),
            .disk => try BlockReader.initFromDiskTable(io, alloc, table, decompressionPool),
        };
        readers.appendAssumeCapacity(reader);
    }
}

fn openCreatedTable(
    io: Io,
    alloc: Allocator,
    tablePath: []const u8,
    maybeMemTable: ?*MemTable,
    decompressionPool: *DecompressionPool,
) !*Table {
    if (maybeMemTable) |memTable| {
        memTable.flushAtUs = Consts.indexFlushIntervalUs + Io.Timestamp.now(io, .real).toMicroseconds();
        return Table.fromMem(io, alloc, memTable, decompressionPool);
    }

    return Table.open(io, alloc, tablePath, decompressionPool);
}

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
    try memTable.inner.mem.storeToDisk(io, alloc, tablePath);
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
    const maxItemLen = 1536;
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
