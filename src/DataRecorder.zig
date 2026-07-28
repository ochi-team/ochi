// TODO: data and index recorders are both hold a lot in common,
// we must desine a single component to manage both
const std = @import("std");
const Allocator = std.mem.Allocator;
const FixedBufferAllocator = std.heap.FixedBufferAllocator;
const Io = std.Io;

const fs = @import("fs.zig");

const Line = @import("store/lines.zig").Line;
const Field = @import("store/lines.zig").Field;
const defaultMaxFieldValueSize = @import("store/lines.zig").defaultMaxFieldValueSize;
const validate = @import("store/lines.zig").validate;
const deinitLinesFull = @import("store/lines.zig").deinitLinesFull;
const maxColumns = @import("store/data/Block.zig").maxColumns;
const maxLines = @import("store/data/Block.zig").maxLines;
const Query = @import("query/Query.zig");
const SID = @import("store/lines.zig").SID;

const MemTable = @import("store/data/MemTable.zig");
const BlockWriter = @import("store/data/BlockWriter.zig");
const TableWriter = @import("store/data/TableWriter.zig");
const TimestampsEncoder = @import("store/data/TimestampsEncoder.zig");
const CompressionPool = @import("store/compression/CompressionPool.zig");
const DecompressionPool = @import("store/compression/DecompressionPool.zig");
const TableHeader = @import("store/data/TableHeader.zig");
const Table = @import("store/data/Table.zig");
const BlockReader = @import("store/data/BlockReader.zig");
const mergeData = @import("store/data/merge.zig").mergeData;
const Runtime = @import("Runtime.zig");
const Logger = @import("logging");
const xev = @import("xev");

const Stop = @import("stds/Stop.zig");
const TimerLoop = @import("stds/xev/TimerLoop.zig");

const merge = @import("store/table/merge.zig");
const TableKind = merge.TableKind;
const cap = @import("store/table/cap.zig");
const swap = @import("store/table/swap.zig");

const Consts = @import("Consts.zig");

const flushSizeThreshold = Consts.flushSizeThreshold;
const amountOfTablesToMerge = Consts.amountOfTablesToMerge;
const maxBlockSize = Consts.maxBlockSize;

const merger = merge.Merger(*Table, maxMemTables, amountOfTablesToMerge);
const swapper = swap.Swapper(DataRecorder, Table);

const TaskCtx = struct {
    recorder: *DataRecorder,
    io: Io,
    alloc: Allocator,
    pool: *std.heap.MemoryPool(MergeTask),
    mx: *std.Io.Mutex,
};

const MergeTask = struct {
    task: xev.ThreadPool.Task,
    ctx: TaskCtx,
    run: *const fn (*DataRecorder, Io, Allocator) void,

    fn callback(t: *xev.ThreadPool.Task) void {
        const self: *MergeTask = @fieldParentPtr("task", t);
        self.run(self.ctx.recorder, self.ctx.io, self.ctx.alloc);

        const pool = self.ctx.pool;
        const recorder = self.ctx.recorder;
        const mx = self.ctx.mx;
        const io = self.ctx.io;
        mx.lockUncancelable(io);
        pool.destroy(self);
        mx.unlock(io);
        _ = recorder.pendingMerges.fetchSub(1, .release);
    }
};

// fixed pool of one-shot deadline timers for mem tables: memTablesSem bounds the
// number of live mem tables to maxMemTables, so a slot is always available.
// The slot's address is stable for the process lifetime.
const TableTimerSlot = struct {
    recorder: *DataRecorder = undefined,
    xevTimer: xev.Timer,
    completion: xev.Completion = .{},
    cancelCompletion: xev.Completion = .{},
    table: ?*Table = null,
};

const maxMemTables = 16;
comptime {
    // it claims we can use a buffer size of amountOfTablesToMerge to handle merging any kind of tables
    std.debug.assert(maxMemTables <= amountOfTablesToMerge);
}

fn getFlushTime(io: Io) i64 {
    return Io.Timestamp.now(io, .real).toMicroseconds() + Consts.dataFlushIntervalUs;
}

pub const DataRecorder = @This();

const SidCheckpoint = struct {
    sid: SID,
    // it's safe to use u16, the flush limit is 1/4 of max u16,
    // so even trippling the amount won't reach it
    // TODO: implement a tail return from addLines in order to hard limit the lines,
    // it allows us to double the limit and be in u16 range
    i: u16,

    comptime {
        // verifies u16 fits enough to have max max lines index
        std.debug.assert(std.math.maxInt(u16) >= maxLines);
    }
};

// TODO: move datashard to its file
pub const DataShard = struct {
    // state

    mx: Io.Mutex = .init,
    lines: std.ArrayList(Line) = .empty,
    // TODO: take a meter to understand if we should increase checkpoints array size
    checkpoints: [maxCheckpoints]SidCheckpoint = undefined,
    checkpointsLen: u16 = 0,
    buffer: FixedBufferAllocator,

    flushAtUs: ?i64 = null,

    // per-shard deadline timer, armed at the exact flushAtUs instant instead of
    // being discovered by a periodic scan; shards live for the process lifetime
    // in DataRecorder.shards, so the timer's userdata (the shard itself) is always valid
    parent: *DataRecorder = undefined,
    xevTimer: xev.Timer,
    timerC: xev.Completion = .{},
    timerCancelC: xev.Completion = .{},

    pub const maxCheckpoints = 16;

    fn reset(self: *DataShard) void {
        self.lines.clearRetainingCapacity();
        self.buffer.reset();
        self.checkpointsLen = 0;
        self.flushAtUs = null;
    }
    fn deinit(self: *DataShard, alloc: Allocator) void {
        self.lines.deinit(alloc);
        alloc.free(self.buffer.buffer);
        self.* = undefined;
    }

    fn appendLines(shard: *DataShard, alloc: Allocator, lines: []const Line, sid: SID) !void {
        const bufferAlloc = shard.buffer.allocator();
        for (lines) |line| {
            validate(line.fields) catch |err| {
                switch (err) {
                    error.MaxFieldsPerLineExceeded => {
                        Logger.log(.warn, "DataShard: max fields per line exceeded", .{});
                        continue;
                    },
                    error.MaxFieldKeySizeExceeded => {
                        Logger.log(.warn, "DataShard: max field key size exceeded", .{});
                        continue;
                    },
                    error.MaxFieldValueSizeExceeded => {
                        Logger.log(.warn, "DataShard: max field value size exceeded", .{});
                        continue;
                    },
                    error.MaxLineSizeExceeded => {
                        Logger.log(.warn, "DataShard: max line size exceeded", .{});
                        continue;
                    },
                }
            };

            const prevLine: ?Line = if (shard.lines.items.len > 0)
                shard.lines.items[shard.lines.items.len - 1]
            else
                null;
            var prevFields: ?[]const Field = if (prevLine) |pl| pl.fields else null;

            const fieldsCopy = try bufferAlloc.alloc(Field, line.fields.len);
            for (line.fields, 0..) |field, fieldIndex| {
                const prevField: ?Field = if (prevFields) |pfs|
                    if (fieldIndex < pfs.len) pfs[fieldIndex] else null
                else
                    null;

                const key: []const u8 = k: {
                    if (prevField) |pf| {
                        if (std.mem.eql(u8, pf.key, field.key)) break :k pf.key;
                    }
                    prevFields = null;
                    break :k try bufferAlloc.dupe(u8, field.key);
                };
                const value: []const u8 = v: {
                    if (prevField) |pf| {
                        if (std.mem.eql(u8, pf.value, field.value)) break :v pf.value;
                    }
                    break :v try bufferAlloc.dupe(u8, field.value);
                };
                fieldsCopy[fieldIndex] = .{ .key = key, .value = value };
            }

            try shard.lines.append(alloc, .{
                .timestampNs = line.timestampNs,
                .fields = fieldsCopy,
            });

            // update the checkpoint after every line so a partial append (e.g. interrupted
            // by an OOM from the fixed buffer) still leaves it consistent with shard.lines
            if (shard.checkpointsLen == 0 or !shard.checkpoints[shard.checkpointsLen - 1].sid.eql(sid)) {
                shard.checkpoints[shard.checkpointsLen] = .{
                    .sid = sid,
                    .i = @intCast(shard.lines.items.len),
                };
                shard.checkpointsLen += 1;
            } else {
                shard.checkpoints[shard.checkpointsLen - 1].i = @intCast(shard.lines.items.len);
            }
        }
    }

    fn mustFlush(self: *const DataShard) bool {
        return self.buffer.end_index >= flushSizeThreshold or
            self.checkpointsLen == maxCheckpoints or
            self.lines.items.len >= maxLines;
    }

    // flush sends all the data to a mem Table,
    // is not a thread safe, assumes the shard is locked
    fn flush(
        self: *DataShard,
        io: Io,
        alloc: Allocator,
        timestampsEncoders: *TimestampsEncoder.TimestampsEncoderPool,
        compressionPool: *CompressionPool,
        decompressionPool: *DecompressionPool,
        sem: *Io.Semaphore,
    ) !?*Table {
        if (self.lines.items.len == 0) {
            return null;
        }

        const memTable = try MemTable.init(alloc);
        errdefer memTable.deinit(alloc);

        sem.waitUncancelable(io);

        var linesByCheckpoint: [maxCheckpoints][]Line = undefined;
        var sids: [maxCheckpoints]SID = undefined;

        var since: usize = 0;
        for (0..self.checkpointsLen) |i| {
            const checkpoint = self.checkpoints[i];
            linesByCheckpoint[i] = self.lines.items[since..checkpoint.i];
            since = checkpoint.i;
            sids[i] = checkpoint.sid;
        }

        memTable.addLines(
            io,
            alloc,
            timestampsEncoders,
            compressionPool,
            sids[0..self.checkpointsLen],
            linesByCheckpoint[0..self.checkpointsLen],
        ) catch |err| {
            sem.post(io);
            return err;
        };
        self.reset();

        sem.post(io);

        memTable.flushAtUs = getFlushTime(io);
        return Table.fromMem(io, alloc, memTable, decompressionPool);
    }
};

shards: []DataShard,
nextShard: std.atomic.Value(usize),

mxTables: Io.Mutex,
memTables: std.ArrayList(*Table),
diskTables: std.ArrayList(*Table),

concurrency: u16,
diskMergeSem: Io.Semaphore,
memMergeSem: Io.Semaphore,

// TODO: implement its usage, limit the amount of mem tables similar to index
// in order to let the mem merger handle it
memTablesSem: Io.Semaphore = .{
    .permits = maxMemTables,
},
timerLoop: *TimerLoop,
taskCtx: TaskCtx,
mergePool: *xev.ThreadPool,
pendingMerges: std.atomic.Value(usize) = .init(0),

// per-object deadline scheduling: arm requests come from arbitrary threads
// (addLines, merge workers) and must be applied to timerLoop.loop only from
// its own thread, so they're queued here and drained via timerLoop's own
// wake handler instead of standing up a second xev.Async.
pendingDeadlineMx: std.atomic.Mutex = .unlocked,
pendingShardArms: std.ArrayList(*DataShard) = .empty,
pendingTableArms: std.ArrayList(*Table) = .empty,
tableTimerSlots: [maxMemTables]TableTimerSlot,
// TODO: migrate to io cancelation
// TODO: implement atomic value that change it's value depending on how many times it's read,
// the idea is to test every break on stop.load() similar to check all allocations failure
stopped: Stop = .{},
mergeIdx: std.atomic.Value(usize),
path: []const u8,
runtime: *Runtime,
timestampsEncoders: *TimestampsEncoder.TimestampsEncoderPool,
compressionPool: *CompressionPool,
decompressionPool: *DecompressionPool,

taskPool: *std.heap.MemoryPool(MergeTask),
mxPool: std.Io.Mutex = .init,

pub fn init(
    io: Io,
    alloc: Allocator,
    path: []const u8,
    runtime: *Runtime,
    timestampsEncoders: *TimestampsEncoder.TimestampsEncoderPool,
    compressionPool: *CompressionPool,
    decompressionPool: *DecompressionPool,
    mergePool: *xev.ThreadPool,
    timerLoop: *TimerLoop,
) !*DataRecorder {
    std.debug.assert(std.fs.path.isAbsolute(path));
    std.debug.assert(path[path.len - 1] != std.fs.path.sep);

    const concurrency = runtime.cpus;
    std.debug.assert(concurrency != 0);

    const shards = try alloc.alloc(DataShard, concurrency);
    var shardsInited: u16 = 0;
    errdefer {
        for (shards[0..shardsInited]) |*shard| shard.deinit(alloc);
        alloc.free(shards);
    }

    for (shards) |*shard| {
        const buf = try alloc.alloc(u8, maxBlockSize);
        errdefer alloc.free(buf);

        shard.* = .{
            .buffer = FixedBufferAllocator.init(buf),
            .xevTimer = try xev.Timer.init(),
        };
        shardsInited += 1;
    }

    var tableTimerSlots: [maxMemTables]TableTimerSlot = undefined;
    for (&tableTimerSlots) |*slot| slot.* = .{ .xevTimer = try xev.Timer.init() };

    var memTables = try std.ArrayList(*Table).initCapacity(alloc, maxMemTables);
    errdefer memTables.deinit(alloc);

    var tables = try Table.openAll(io, alloc, path, decompressionPool);
    errdefer {
        for (tables.items) |table| table.close(io);
        tables.deinit(alloc);
    }

    const taskPool = try alloc.create(std.heap.MemoryPool(MergeTask));
    errdefer alloc.destroy(taskPool);
    taskPool.* = try .initCapacity(alloc, 32);
    errdefer taskPool.deinit(alloc);

    const t = try alloc.create(DataRecorder);
    errdefer alloc.destroy(t);

    t.* = DataRecorder{
        .shards = shards,
        .nextShard = std.atomic.Value(usize).init(0),
        .mergeIdx = .init(@intCast(Io.Timestamp.now(io, .real).nanoseconds)),

        .mxTables = .init,
        .concurrency = concurrency,
        .memTables = memTables,
        .diskTables = tables,
        .diskMergeSem = .{
            .permits = @max(4, concurrency),
        },
        .memMergeSem = .{
            .permits = @max(4, concurrency),
        },
        .timerLoop = timerLoop,
        .taskCtx = undefined,
        .mergePool = mergePool,
        .path = path,
        .runtime = runtime,
        .timestampsEncoders = timestampsEncoders,
        .compressionPool = compressionPool,
        .decompressionPool = decompressionPool,
        .taskPool = taskPool,
        .tableTimerSlots = tableTimerSlots,
    };

    t.taskCtx = .{ .recorder = t, .io = io, .alloc = alloc, .mx = &t.mxPool, .pool = taskPool };
    for (shards) |*shard| shard.parent = t;
    for (&t.tableTimerSlots) |*slot| slot.recorder = t;

    return t;
}

pub fn createDir(io: Io, path: []const u8) !void {
    try fs.createDirAssert(io, path);
    try fs.syncPathAndParentDir(io, path);
}

pub fn startTasks(self: *DataRecorder, io: Io, alloc: Allocator) !void {
    for (0..self.concurrency) |_| {
        try self.startDiskTablesMerge(io, alloc);
    }

    try self.timerLoop.addWakeHandler(self, deadlineWakeHandler);

    try self.timerLoop.start();
}

// TODO: find an approach to make it never fail,
// the only option it fails is OOM, so cleaning more memory in advance might be more reliable
// another problem it's hard to test it via checkAllAllocationFailures.
// Then audit all deinits and use it instead
// TODO: make using this API instead of directly managing stopped state in the tests
// TODO: this theoretically is not enough to stop the other jobs form starting,
// either lock stop or find another way to make sure none of the task are running after g.wait
pub fn stop(self: *DataRecorder, io: Io, alloc: Allocator) !void {
    self.stopped.stop(io);
    // we ignore canceled error, we stop anyway
    // TODO: make sure it's not possible to run a job after we await,
    // so we block the following scenario:
    // - enter stop
    // - a merge process calls startX
    // - we do await
    // - a job passing a stopped flag runs a task
    // - we do flush and miss the executed job
    // therefore a dirty shutdown happens and we loose the data

    // don't shut down mergePool here: flushForce below can still submit a
    // straggler merge task (flushShard -> startMemTablesMerge), deinit()
    // drains and shuts the pool down after that has a chance to run
    self.waitForMergesToDrain(io);

    try self.flushForce(io, alloc);
}

pub fn flushForce(self: *DataRecorder, io: Io, alloc: Allocator) !void {
    try self.flushDataShards(io, alloc, true);
    try self.flushMemTables(io, alloc, true);
}

pub fn deinit(self: *DataRecorder, io: Io, alloc: Allocator) void {
    std.debug.assert(self.memTables.items.len == 0);

    self.waitForMergesToDrain(io);

    for (self.pendingTableArms.items) |table| table.release(io);
    for (&self.tableTimerSlots) |*slot| {
        if (slot.table) |table| table.release(io);
    }

    for (self.shards) |*shard| {
        shard.deinit(alloc);
    }
    for (self.diskTables.items) |table| {
        table.release(io);
    }
    for (self.memTables.items) |table| {
        table.release(io);
    }

    self.memTables.deinit(alloc);
    self.diskTables.deinit(alloc);
    alloc.free(self.shards);
    self.pendingShardArms.deinit(alloc);
    self.pendingTableArms.deinit(alloc);
    self.taskPool.deinit(alloc);
    alloc.destroy(self.taskPool);
    self.* = undefined;
    alloc.destroy(self);
}

fn waitForMergesToDrain(self: *DataRecorder, io: Io) void {
    while (self.pendingMerges.load(.acquire) != 0) {
        Io.sleep(io, .fromMilliseconds(1), .real) catch {};
    }
}

fn deadlineWakeHandler(ctx: *anyopaque, loop: *xev.Loop) void {
    const self: *DataRecorder = @ptrCast(@alignCast(ctx));
    if (self.stopped.isStopped()) return;

    var shardArms: std.ArrayList(*DataShard) = undefined;
    var tableArms: std.ArrayList(*Table) = undefined;
    {
        TimerLoop.spinLock(&self.pendingDeadlineMx);
        defer self.pendingDeadlineMx.unlock();
        shardArms = self.pendingShardArms;
        self.pendingShardArms = .empty;
        tableArms = self.pendingTableArms;
        self.pendingTableArms = .empty;
    }
    defer shardArms.deinit(self.taskCtx.alloc);
    defer tableArms.deinit(self.taskCtx.alloc);

    for (shardArms.items) |shard| self.armShardTimer(loop, shard);
    for (tableArms.items) |table| self.armTableTimer(loop, table);
}

fn armShardTimer(self: *DataRecorder, loop: *xev.Loop, shard: *DataShard) void {
    const flushAtUs = shard.flushAtUs orelse return; // flushed early (mustFlush) before the arm was drained
    const nowUs = Io.Timestamp.now(self.taskCtx.io, .real).toMicroseconds();
    shard.xevTimer.reset(loop, &shard.timerC, &shard.timerCancelC, deltaMs(flushAtUs, nowUs), DataShard, shard, shardTimerCallback);
}

fn shardTimerCallback(
    ud: ?*DataShard,
    loop: *xev.Loop,
    c: *xev.Completion,
    r: xev.Timer.RunError!void,
) xev.CallbackAction {
    _ = r catch {};
    const shard = ud.?;
    const self = shard.parent;

    if (self.stopped.isStopped()) return .disarm;

    const io = self.taskCtx.io;

    if (!shard.mx.tryLock()) {
        // addLines is actively appending; retry shortly instead of dropping the deadline
        shard.xevTimer.reset(loop, c, &shard.timerCancelC, 1, DataShard, shard, shardTimerCallback);
        return .disarm;
    }
    defer shard.mx.unlock(io);

    // flushAtUs is only ever cleared (mustFlush already flushed it) or left
    // unchanged for a shard's active window, never moved to a later deadline,
    // so a stale fire after an early flush is a safe no-op here.
    const flushAtUs = shard.flushAtUs orelse return .disarm;
    const nowUs = Io.Timestamp.now(io, .real).toMicroseconds();
    if (flushAtUs > nowUs) {
        shard.xevTimer.reset(loop, c, &shard.timerCancelC, deltaMs(flushAtUs, nowUs), DataShard, shard, shardTimerCallback);
        return .disarm;
    }

    self.flushShard(io, self.taskCtx.alloc, shard, false) catch |err| {
        if (err != error.Stopped) {
            self.stopped.stop(io);
            Logger.log(.err, "failed to run scheduled shard flush", .{ .err = err });
        }
    };
    return .disarm;
}

fn armTableTimer(self: *DataRecorder, loop: *xev.Loop, table: *Table) void {
    const slot = for (&self.tableTimerSlots) |*s| {
        if (s.table == null) break s;
    } else {
        // unreachable in practice: memTablesSem bounds live mem tables to
        // maxMemTables, matching tableTimerSlots.len exactly
        Logger.log(.err, "DataRecorder: no free table timer slot, dropping scheduled flush", .{});
        table.release(self.taskCtx.io);
        return;
    };

    slot.table = table;

    const nowUs = Io.Timestamp.now(self.taskCtx.io, .real).toMicroseconds();
    const delayMs = deltaMs(table.inner.mem.flushAtUs, nowUs);
    slot.xevTimer.reset(loop, &slot.completion, &slot.cancelCompletion, delayMs, TableTimerSlot, slot, tableTimerCallback);
}

fn deltaMs(deadlineUs: i64, nowUs: i64) u64 {
    if (deadlineUs <= nowUs) return 0;
    const deltaUs: u64 = @intCast(deadlineUs - nowUs);
    return deltaUs / std.time.us_per_ms;
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

fn flushMemTables(self: *DataRecorder, io: Io, allocator: Allocator, force: bool) !void {
    const nowUs = Io.Timestamp.now(io, .real).toMicroseconds();
    self.mxTables.lockUncancelable(io);

    var tablesBuf: [maxMemTables]*Table = undefined;
    var tables = std.ArrayList(*Table).initBuffer(&tablesBuf);

    for (self.memTables.items) |memTable| {
        const isTimeToMerge = memTable.inner.mem.flushAtUs <= nowUs;
        if (!memTable.inMerge and (force or isTimeToMerge)) {
            memTable.inMerge = true;
            tables.appendAssumeCapacity(memTable);
        }
    }

    self.mxTables.unlock(io);

    if (tables.items.len == 0) {
        return;
    }

    try self.flushMemTablesInChunks(io, allocator, tables);
}

fn flushMemTablesInChunks(self: *DataRecorder, io: Io, alloc: Allocator, toFlush: std.ArrayList(*Table)) !void {
    if (toFlush.items.len == 0) return;

    var tail = toFlush.items[0..];
    while (tail.len > 0) {
        const n = merger.selectTablesToMerge(tail);
        std.debug.assert(n > 0);

        // TODO: attempt to run it in parallel, add a semaphore then
        try self.mergeTables(io, alloc, tail[0..n], true, null);

        tail = tail[n..];
    }
}

fn flushDataShards(self: *DataRecorder, io: Io, allocator: Allocator, force: bool) !void {
    if (force) {
        for (self.shards) |*shard| {
            // if it's not locked we are adding lines just know, makes no sense to lock it yet
            shard.mx.lockUncancelable(io);
            defer shard.mx.unlock(io);
            try self.flushShard(io, allocator, shard, force);
        }
        return;
    }

    const nowUs = Io.Timestamp.now(io, .real).toMicroseconds();
    for (self.shards) |*shard| {
        // if it's not locked we are adding lines just know, makes no sense to lock it yet
        if (shard.mx.tryLock()) {
            defer shard.mx.unlock(io);
            if (shard.flushAtUs) |flushAtUs| {
                if (flushAtUs < nowUs) {
                    try self.flushShard(io, allocator, shard, force);
                }
            }
        } else {
            Logger.log(.debug, "skipping shard flush because it is locked", .{});
        }
    }
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

fn flushShard(self: *DataRecorder, io: Io, alloc: Allocator, shard: *DataShard, force: bool) !void {
    const maybeMemTable = try shard.flush(io, alloc, self.timestampsEncoders, self.compressionPool, self.decompressionPool, &self.memMergeSem);
    if (maybeMemTable) |memTable| {
        timedWait(&self.memTablesSem, io, std.time.ns_per_s / 10) catch |err| {
            errdefer memTable.release(io);

            switch (err) {
                error.Timeout => {
                    if (self.stopped.isStopped() and !force) {
                        return error.Stopped;
                    }

                    try self.flushMemTables(io, alloc, true);

                    // if the first sem wait couldn't free the space it times out
                    // and must flush to disk as is,
                    timedWait(&self.memTablesSem, io, std.time.ns_per_s * 1) catch |e| {
                        switch (e) {
                            error.Timeout => {
                                Logger.log(.warn, "mem table buffer is full, flush mem table", .{});

                                const destinationTablePath = try self.diskTablePath(alloc, .disk);
                                errdefer if (destinationTablePath.len > 0) alloc.free(destinationTablePath);

                                // pass empty list tables because we have nothing to merge/replace,
                                // it must only flush to disk a passed mem table and not remove existing tables,
                                // but perform semaphore
                                try self.flushMemTable(io, alloc, memTable.inner.mem, &[_]*Table{}, destinationTablePath, .disk);
                                memTable.release(io);
                            },
                        }

                        // second timeout, we flushed the table to the disk, early return
                        return;
                    };
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
    }
}

pub fn startDiskTablesMerge(self: *DataRecorder, io: Io, alloc: Allocator) !void {
    try self.submitMergeTask(io, alloc, runDiskTablesMerger);
}

pub fn startMemTablesMerge(self: *DataRecorder, io: Io, alloc: Allocator) !void {
    try self.submitMergeTask(io, alloc, runMemTableMerger);
}

fn submitMergeTask(
    self: *DataRecorder,
    io: Io,
    alloc: Allocator,
    run: *const fn (*DataRecorder, Io, Allocator) void,
) !void {
    if (self.stopped.isStopped()) return;

    self.mxPool.lockUncancelable(io);
    defer self.mxPool.unlock(io);

    const t = try self.taskPool.create(alloc);
    errdefer alloc.destroy(t);

    t.* = .{
        .task = .{ .callback = MergeTask.callback },
        .ctx = .{
            .recorder = self,
            .io = io,
            .alloc = alloc,
            .pool = self.taskPool,
            .mx = &self.mxPool,
        },
        .run = run,
    };

    _ = self.pendingMerges.fetchAdd(1, .monotonic);
    self.mergePool.schedule(.from(&t.task));
}

fn runDiskTablesMerger(self: *DataRecorder, io: Io, alloc: Allocator) void {
    self.tablesMerger(io, alloc, &self.diskTables, &self.diskMergeSem) catch |err| {
        if (err == error.Stopped) return;

        self.stopped.stop(io);
        Logger.log(.err, "failed to merge disk tables", .{ .err = err });
    };
}

fn runMemTableMerger(self: *DataRecorder, io: Io, alloc: Allocator) void {
    self.tablesMerger(io, alloc, &self.memTables, &self.memMergeSem) catch |err| {
        if (err == error.Stopped) return;

        self.stopped.stop(io);
        Logger.log(.err, "failed to merge mem tables", .{ .err = err });
    };
}

fn tablesMerger(
    self: *DataRecorder,
    io: Io,
    alloc: Allocator,
    tables: *std.ArrayList(*Table),
    sem: *Io.Semaphore,
) !void {
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

        sem.waitUncancelable(io);
        defer sem.post(io);
        try self.mergeTables(io, alloc, filteredTablesToMerge, false, &self.stopped);
    }
}

fn nextMergeIdx(self: *DataRecorder) usize {
    return self.mergeIdx.fetchAdd(1, .monotonic);
}

fn mergeTables(
    self: *DataRecorder,
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

    var newMemTable: ?*MemTable = null;
    const blockWriter = try BlockWriter.init(alloc);
    defer blockWriter.deinit(alloc);

    const streamWriter: *TableWriter = blk: {
        if (tableKind == .mem) {
            const memTable = try MemTable.init(alloc);
            newMemTable = memTable;
            break :blk try TableWriter.initMem(alloc, memTable, self.timestampsEncoders, self.compressionPool);
        } else {
            var sourceCompressedSizeTotal: u64 = 0;
            for (tables) |table| {
                sourceCompressedSizeTotal += table.tableHeader().compressedSize;
            }
            const fitsInCache = sourceCompressedSizeTotal <= merger.maxCachableTableSize(
                self.runtime.maxMem,
                self.runtime.cacheSize,
            );
            break :blk try TableWriter.initDisk(io, alloc, destinationTablePath, fitsInCache, self.timestampsEncoders, self.compressionPool);
        }
    };
    defer streamWriter.deinit(alloc);

    const tableHeader = mergeData(io, alloc, self.timestampsEncoders, self.decompressionPool, streamWriter, &readers, stopped) catch |err| {
        switch (err) {
            error.Stopped => {
                if (destinationTablePath.len > 0) {
                    fs.deleteTreeAbsolute(io, destinationTablePath) catch |deleteErr| {
                        Logger.log(.err, "failed to delete half way merged data table after stopped", .{ .err = deleteErr });
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

        // TODO: implement stack fallback that replaces stack size to 1 in tests,
        // add a tidy linter that restricts usage of std.heap.stackFallback
        var fba = std.heap.stackFallback(256, alloc);
        try tableHeader.writeFile(io, fba.get(), destinationTablePath);

        try fs.syncPathAndParentDir(io, destinationTablePath);
    }

    const openTable = try openCreatedTable(io, alloc, destinationTablePath, newMemTable, self.decompressionPool);
    errdefer openTable.release(io);

    try swapper.swapTables(self, io, alloc, tables, openTable, tableKind);
    swapped = true;

    if (tableKind == .mem) self.requestTableTimer(openTable);
}

pub fn diskTablePath(self: *DataRecorder, alloc: Allocator, kind: TableKind) ![]const u8 {
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
    self: *DataRecorder,
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

pub fn addLines(self: *DataRecorder, io: Io, alloc: Allocator, lines: []const Line, sid: SID) !void {
    const i = self.nextShard.fetchAdd(1, .monotonic) % self.shards.len;
    var shard = &self.shards[i];

    shard.mx.lockUncancelable(io);
    defer shard.mx.unlock(io);

    const start = shard.lines.items.len;
    shard.appendLines(alloc, lines, sid) catch |err| {
        switch (err) {
            Allocator.Error.OutOfMemory => {
                Logger.log(.warn, "processor: buffer overflow, decrease flush threashold", .{});
                const offset = shard.lines.items.len - start;
                try self.flushShard(io, alloc, shard, false);
                shard.appendLines(alloc, lines[offset..], sid) catch |e| {
                    Logger.log(.err, "processor: buffer doesn't fit input lines", .{ .err = e });
                    return e;
                };
            },
        }
    };

    if (shard.mustFlush()) {
        try self.flushShard(io, alloc, shard, false);
    } else if (shard.flushAtUs == null) {
        shard.flushAtUs = getFlushTime(io);
        self.requestShardTimer(shard);
    }
}

// queues an arm request for the loop thread; safe to call from any thread.
fn requestShardTimer(self: *DataRecorder, shard: *DataShard) void {
    TimerLoop.spinLock(&self.pendingDeadlineMx);
    self.pendingShardArms.append(self.taskCtx.alloc, shard) catch |err| {
        self.pendingDeadlineMx.unlock();
        Logger.log(.err, "DataRecorder: failed to queue shard timer arm", .{ .err = err });
        return;
    };
    self.pendingDeadlineMx.unlock();

    self.timerLoop.notify();
}

// queues an arm request for the loop thread; safe to call from any thread.
// retains the table for the timer's lifetime so a merge that frees it before
// the deadline fires can't leave the timer pointing at freed memory.
fn requestTableTimer(self: *DataRecorder, table: *Table) void {
    table.retain();

    TimerLoop.spinLock(&self.pendingDeadlineMx);
    self.pendingTableArms.append(self.taskCtx.alloc, table) catch |err| {
        self.pendingDeadlineMx.unlock();
        Logger.log(.err, "DataRecorder: failed to queue table timer arm", .{ .err = err });
        table.release(self.taskCtx.io);
        return;
    };
    self.pendingDeadlineMx.unlock();

    self.timerLoop.notify();
}

pub fn queryLines(self: *DataRecorder, io: Io, alloc: Allocator, sids: []SID, query: Query) !std.ArrayList(Line) {
    var tables = try self.getTables(io, alloc, query.start, query.end);
    defer {
        for (tables.items) |table| table.release(io);
        tables.deinit(alloc);
    }

    var linesDst = std.ArrayList(Line).empty;
    errdefer linesDst.deinit(alloc);
    for (tables.items) |table| {
        try table.queryLines(io, alloc, self.timestampsEncoders, self.decompressionPool, &linesDst, sids, query);
    }

    return linesDst;
}

pub fn getTables(self: *DataRecorder, io: Io, alloc: Allocator, start: u64, end: u64) !std.ArrayList(*Table) {
    self.mxTables.lockUncancelable(io);
    defer self.mxTables.unlock(io);

    const tablesLen = self.memTables.items.len + self.diskTables.items.len;
    var tables = try std.ArrayList(*Table).initCapacity(alloc, tablesLen);
    try selectTablesInRange(alloc, &tables, self.memTables.items, start, end);
    try selectTablesInRange(alloc, &tables, self.diskTables.items, start, end);

    return tables;
}

fn openCreatedTable(
    io: Io,
    alloc: Allocator,
    tablePath: []const u8,
    maybeMemTable: ?*MemTable,
    decompressionPool: *DecompressionPool,
) !*Table {
    if (maybeMemTable) |memTable| {
        memTable.flushAtUs = Consts.dataFlushIntervalUs + Io.Timestamp.now(io, .real).toMicroseconds();
        return Table.fromMem(io, alloc, memTable, decompressionPool);
    }

    return Table.open(io, alloc, tablePath, decompressionPool);
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

fn selectTablesInRange(
    alloc: Allocator,
    dst: *std.ArrayList(*Table),
    tables: []const *Table,
    start: u64,
    end: u64,
) !void {
    for (tables) |table| {
        if (table.tableHeader().maxTimestamp < start or table.tableHeader().minTimestamp > end) {
            continue;
        }
        table.retain();
        try dst.append(alloc, table);
    }
}

const testing = std.testing;
const DebugIo = @import("stds/Io/DebugIo.zig");
const makeUniqueFieldLines = @import("testing/fixtures.zig").makeUniqueFieldLines;

var stableFields = [_][2]Field{
    .{
        .{ .key = "level", .value = "info" },
        .{ .key = "app", .value = "ochi" },
    },
    .{
        .{ .key = "level", .value = "warn" },
        .{ .key = "app", .value = "ochi" },
    },
    .{
        .{ .key = "level", .value = "error" },
        .{ .key = "app", .value = "ochi" },
    },
    .{
        .{ .key = "region", .value = "us-east" },
        .{ .key = "service", .value = "api" },
    },
};

fn stableSID(streamID: u128) SID {
    return .{ .tenantID = 1, .id = streamID };
}

fn stableLine(ts: u64, variant: usize) Line {
    const fields = stableFields[variant % stableFields.len][0..];
    return .{
        .timestampNs = ts,
        .fields = fields,
    };
}

fn createMemTableFromLines(io: Io, alloc: Allocator, timestampsEncoders: *TimestampsEncoder.TimestampsEncoderPool, compressionPool: *CompressionPool, sid: SID, lines: []Line) !*Table {
    const memTable = try MemTable.init(alloc);
    errdefer memTable.deinit(alloc);
    const decompressionPool = try DecompressionPool.init(alloc, 1);
    defer decompressionPool.deinit(alloc);

    try memTable.addLinesForSid(io, alloc, timestampsEncoders, compressionPool, sid, lines);
    return Table.fromMem(io, alloc, memTable, decompressionPool);
}

fn createDiskTableFromLines(
    io: Io,
    alloc: Allocator,
    rootPath: []const u8,
    tableName: []const u8,
    timestampsEncoders: *TimestampsEncoder.TimestampsEncoderPool,
    compressionPool: *CompressionPool,
    decompressionPool: *DecompressionPool,
    sid: SID,
    lines: []Line,
) !*Table {
    const tablePath = try std.fmt.allocPrint(alloc, "{s}/{s}", .{ rootPath, tableName });
    errdefer alloc.free(tablePath);

    const memTable = try MemTable.init(alloc);
    defer memTable.deinit(alloc);

    try memTable.addLinesForSid(io, alloc, timestampsEncoders, compressionPool, sid, lines);
    try memTable.storeToDisk(io, alloc, tablePath);
    return Table.open(io, alloc, tablePath, decompressionPool);
}

fn countMemLinesInRecorder(recorder: *DataRecorder) u64 {
    var n: u64 = 0;
    for (recorder.memTables.items) |table| {
        n += table.tableHeader().len;
    }
    return n;
}

fn countDiskLinesInRecorder(recorder: *DataRecorder) u64 {
    var n: u64 = 0;
    for (recorder.diskTables.items) |table| {
        n += table.tableHeader().len;
    }
    return n;
}

test "tablesMerger handles more source tables than merge window" {
    const alloc = testing.allocator;
    const io = testing.io;

    const runtime = try Runtime.init(io, alloc, ".", 0.5);
    defer runtime.deinit(alloc);

    var recorder: DataRecorder = undefined;
    recorder.stopped = .{};
    recorder.runtime = runtime;
    recorder.mxTables = .init;

    var table: Table = undefined;
    table.inMerge = true;

    var tablesBuf = [_]*Table{&table} ** (amountOfTablesToMerge + 1);
    var tables = std.ArrayList(*Table).initBuffer(&tablesBuf);
    tables.items.len = tablesBuf.len;

    var sem: Io.Semaphore = .{ .permits = 1 };
    try recorder.tablesMerger(io, alloc, &tables, &sem);

    try testing.expectEqual(tablesBuf.len, tables.items.len);
}

test "selectTablesInRange selects overlap and handles gaps" {
    const alloc = testing.allocator;
    const io = testing.io;

    const Range = struct {
        min: u64,
        max: u64,
    };
    const Case = struct {
        from: u64,
        to: u64,
        expected: []const Range,
    };

    const check = struct {
        fn run(io_: Io, alloc_: Allocator, tables: []const *Table, cases: []const Case) !void {
            for (cases) |case| {
                var selected = std.ArrayList(*Table).empty;
                defer {
                    for (selected.items) |table| table.release(io_);
                    selected.deinit(alloc_);
                }

                try selectTablesInRange(alloc_, &selected, tables, case.from, case.to);
                try testing.expectEqual(case.expected.len, selected.items.len);
                for (case.expected, 0..) |expected, i| {
                    try testing.expectEqual(expected.min, selected.items[i].tableHeader().minTimestamp);
                    try testing.expectEqual(expected.max, selected.items[i].tableHeader().maxTimestamp);
                }
            }
        }
    }.run;

    const newTable = struct {
        fn new(allocator: Allocator, header: TableHeader) !Table {
            const memTable = try allocator.create(MemTable);
            memTable.tableHeader = header;
            return .{
                .inner = .{ .mem = memTable },
                .indexBlockHeaders = &.{},
                .size = 0,
                .path = "",
                .columnIDGen = undefined,
                .columnIdxs = .{},
                .alloc = allocator,
                .inMerge = false,
                .toRemove = .init(false),
                .refCounter = .init(1),
            };
        }
    }.new;

    {
        const tables = [_]*Table{};
        try check(io, alloc, &tables, &[_]Case{
            .{ .from = 0, .to = 0, .expected = &.{} },
            .{ .from = 0, .to = 100, .expected = &.{} },
            .{ .from = 10, .to = 20, .expected = &.{} },
        });
    }

    {
        const h = TableHeader{ .minTimestamp = 100, .maxTimestamp = 110 };
        var t = try newTable(alloc, h);
        defer alloc.destroy(t.inner.mem);
        const tables = [_]*Table{&t};
        try check(io, alloc, &tables, &[_]Case{
            .{ .from = 100, .to = 110, .expected = &.{.{ .min = 100, .max = 110 }} },
            .{ .from = 90, .to = 120, .expected = &.{.{ .min = 100, .max = 110 }} },
            .{ .from = 99, .to = 100, .expected = &.{.{ .min = 100, .max = 110 }} },
            .{ .from = 110, .to = 111, .expected = &.{.{ .min = 100, .max = 110 }} },
            .{ .from = 0, .to = 99, .expected = &.{} },
            .{ .from = 111, .to = 200, .expected = &.{} },
        });
    }

    {
        const h10 = TableHeader{ .minTimestamp = 10, .maxTimestamp = 19 };
        const h30 = TableHeader{ .minTimestamp = 30, .maxTimestamp = 39 };
        const h50 = TableHeader{ .minTimestamp = 50, .maxTimestamp = 59 };
        var t10 = try newTable(alloc, h10);
        defer alloc.destroy(t10.inner.mem);
        var t30 = try newTable(alloc, h30);
        defer alloc.destroy(t30.inner.mem);
        var t50 = try newTable(alloc, h50);
        defer alloc.destroy(t50.inner.mem);
        const tables = [_]*Table{ &t10, &t30, &t50 };
        try check(io, alloc, &tables, &[_]Case{
            .{ .from = 20, .to = 29, .expected = &.{} },
            .{ .from = 25, .to = 35, .expected = &.{.{ .min = 30, .max = 39 }} },
            .{ .from = 10, .to = 10, .expected = &.{.{ .min = 10, .max = 19 }} },
            .{ .from = 39, .to = 39, .expected = &.{.{ .min = 30, .max = 39 }} },
            .{ .from = 39, .to = 49, .expected = &.{.{ .min = 30, .max = 39 }} },
            .{ .from = 39, .to = 50, .expected = &.{ .{ .min = 30, .max = 39 }, .{ .min = 50, .max = 59 } } },
            .{ .from = 40, .to = 50, .expected = &.{.{ .min = 50, .max = 59 }} },
            .{ .from = 0, .to = 100, .expected = &.{
                .{ .min = 10, .max = 19 },
                .{ .min = 30, .max = 39 },
                .{ .min = 50, .max = 59 },
            } },
            .{ .from = 40, .to = 49, .expected = &.{} },
            .{ .from = 60, .to = 100, .expected = &.{} },
        });
    }

    {
        const h10 = TableHeader{ .minTimestamp = 10, .maxTimestamp = 19 };
        const h20 = TableHeader{ .minTimestamp = 20, .maxTimestamp = 29 };
        const h30 = TableHeader{ .minTimestamp = 30, .maxTimestamp = 39 };
        const h40 = TableHeader{ .minTimestamp = 40, .maxTimestamp = 49 };
        const h50 = TableHeader{ .minTimestamp = 50, .maxTimestamp = 59 };
        var t10 = try newTable(alloc, h10);
        defer alloc.destroy(t10.inner.mem);
        var t20 = try newTable(alloc, h20);
        defer alloc.destroy(t20.inner.mem);
        var t30 = try newTable(alloc, h30);
        defer alloc.destroy(t30.inner.mem);
        var t40 = try newTable(alloc, h40);
        defer alloc.destroy(t40.inner.mem);
        var t50 = try newTable(alloc, h50);
        defer alloc.destroy(t50.inner.mem);
        const tables = [_]*Table{ &t10, &t20, &t30, &t40, &t50 };
        try check(io, alloc, &tables, &[_]Case{
            .{ .from = 10, .to = 59, .expected = &.{
                .{ .min = 10, .max = 19 },
                .{ .min = 20, .max = 29 },
                .{ .min = 30, .max = 39 },
                .{ .min = 40, .max = 49 },
                .{ .min = 50, .max = 59 },
            } },
            .{ .from = 22, .to = 47, .expected = &.{
                .{ .min = 20, .max = 29 },
                .{ .min = 30, .max = 39 },
                .{ .min = 40, .max = 49 },
            } },
            .{ .from = 0, .to = 9, .expected = &.{} },
            .{ .from = 60, .to = 100, .expected = &.{} },
        });
    }
}

test "DataRecorder.addLines flushes DataShard on automatic triggers" {
    const alloc = testing.allocator;
    const io = testing.io;

    const Trigger = enum {
        sizeThreshold,
        checkpointsLimit,
        deadline,
        bufferOverflow,
    };
    const Case = struct {
        name: []const u8,
        trigger: Trigger,
        expectedFlushed: u64,
        expectedBuffered: usize,
    };

    const cases = [_]Case{
        .{
            .name = "size threshold",
            .trigger = .sizeThreshold,
            .expectedFlushed = flushSizeThreshold / (defaultMaxFieldValueSize) + 4,
            .expectedBuffered = 0,
        },
        .{
            .name = "checkpoints limit",
            .trigger = .checkpointsLimit,
            .expectedFlushed = DataShard.maxCheckpoints,
            .expectedBuffered = 0,
        },
        .{
            .name = "deadline",
            .trigger = .deadline,
            .expectedFlushed = 1,
            .expectedBuffered = 0,
        },
        .{
            .name = "buffer overflow retry",
            .trigger = .bufferOverflow,
            .expectedFlushed = 1,
            .expectedBuffered = 1,
        },
    };

    for (cases) |case| {
        var tmp = testing.tmpDir(.{});
        defer tmp.cleanup();
        const rootPath = try tmp.dir.realPathFileAlloc(io, ".", alloc);
        defer alloc.free(rootPath);

        const runtime = try Runtime.init(io, alloc, rootPath, 0.5);
        defer runtime.deinit(alloc);

        const timestampsEncoders = try TimestampsEncoder.TimestampsEncoderPool.init(alloc, 1);
        defer timestampsEncoders.deinit(alloc);
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
        const recorder = try DataRecorder.init(io, alloc, rootPath, runtime, timestampsEncoders, compressionPool, decompressionPool, &mergePool, timerLoop);
        defer recorder.deinit(io, alloc);
        defer {
            timerLoop.stop();
            timerLoop.join();
            timerLoop.deinit();
        }

        switch (case.trigger) {
            .sizeThreshold => {
                const valueLen = defaultMaxFieldValueSize;
                const lineCount = flushSizeThreshold / valueLen + 4;
                var lines: [lineCount]Line = undefined;
                var fields: [lineCount]Field = undefined;
                var values: [lineCount][]u8 = undefined;
                defer for (values) |value| alloc.free(value);

                for (0..lineCount) |i| {
                    const value = try alloc.alloc(u8, valueLen);
                    @memset(value, 'x');
                    std.mem.writeInt(usize, value[0..@sizeOf(usize)], i, .little);
                    values[i] = value;

                    fields[i] = .{
                        .key = "message",
                        .value = value,
                    };
                    lines[i] = .{
                        .timestampNs = @intCast(i + 1),
                        .fields = fields[i .. i + 1],
                    };
                }

                try recorder.addLines(io, alloc, &lines, stableSID(1));
            },
            .checkpointsLimit => {
                for (0..DataShard.maxCheckpoints) |i| {
                    recorder.nextShard.store(0, .monotonic);
                    var lines = [_]Line{stableLine(@intCast(i + 1), i)};
                    try recorder.addLines(io, alloc, lines[0..], stableSID(i + 1));
                }
            },
            .deadline => {
                recorder.nextShard.store(0, .monotonic);
                var lines = [_]Line{stableLine(1, 0)};
                try recorder.addLines(io, alloc, lines[0..], stableSID(1));

                try testing.expect(recorder.shards[0].flushAtUs != null);
                recorder.shards[0].flushAtUs = Io.Timestamp.now(io, .real).toMicroseconds() - std.time.us_per_s;
                try recorder.flushDataShards(io, alloc, false);
            },
            .bufferOverflow => {
                var seedLines = [_]Line{stableLine(1, 0)};
                try recorder.shards[0].appendLines(alloc, seedLines[0..], stableSID(1));

                const filler = try recorder.shards[0].buffer.allocator().alloc(u8, maxBlockSize - recorder.shards[0].buffer.end_index);
                @memset(filler, 'x');

                recorder.nextShard.store(0, .monotonic);
                var retryLines = [_]Line{stableLine(2, 1)};
                try recorder.addLines(io, alloc, retryLines[0..], stableSID(2));
            },
        }

        try testing.expectEqual(case.expectedBuffered, recorder.shards[0].lines.items.len);
        try testing.expectEqual(case.expectedFlushed, countMemLinesInRecorder(recorder));
        try testing.expectEqual(0, countDiskLinesInRecorder(recorder));
        try testing.expectEqual(recorder.memTables.items.len, 1);

        try recorder.flushForce(io, alloc);
    }
}

test "DataRecorder.addLines does not crash when a shard exceeds Block.maxLines" {
    const alloc = testing.allocator;
    const io = testing.io;

    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    const rootPath = try tmp.dir.realPathFileAlloc(io, ".", alloc);
    defer alloc.free(rootPath);

    const runtime = try Runtime.init(io, alloc, rootPath, 0.5);
    defer runtime.deinit(alloc);

    const timestampsEncoders = try TimestampsEncoder.TimestampsEncoderPool.init(alloc, 1);
    defer timestampsEncoders.deinit(alloc);
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
    const recorder = try DataRecorder.init(io, alloc, rootPath, runtime, timestampsEncoders, compressionPool, decompressionPool, &mergePool, timerLoop);
    defer recorder.deinit(io, alloc);
    defer {
        timerLoop.stop();
        timerLoop.join();
        timerLoop.deinit();
    }

    // all lines share the same sid and fields, so appendLines reuses the buffered
    // key/value pointers and the shard's byte-size flush threshold is never hit,
    // letting the checkpoint grow well past Block.maxLines before it's flushed.
    const lineCount = maxLines + 500;
    var lines: [maxLines + 500]Line = undefined;
    for (0..lineCount) |i| {
        lines[i] = stableLine(i + 1, 0);
    }
    try recorder.addLines(io, alloc, lines[0..], stableSID(1));

    try recorder.flushForce(io, alloc);

    try testing.expectEqual(0, recorder.memTables.items.len);
    try testing.expectEqual(lineCount, countDiskLinesInRecorder(recorder));
}

test "DataShard.flush limits block columns per tenant" {
    const alloc = testing.allocator;
    const io = testing.io;

    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    const rootPath = try tmp.dir.realPathFileAlloc(io, ".", alloc);
    defer alloc.free(rootPath);

    const runtime = try Runtime.init(io, alloc, rootPath, 0.5);
    defer runtime.deinit(alloc);

    const timestampsEncoders = try TimestampsEncoder.TimestampsEncoderPool.init(alloc, 1);
    defer timestampsEncoders.deinit(alloc);
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
    const recorder = try DataRecorder.init(io, alloc, rootPath, runtime, timestampsEncoders, compressionPool, decompressionPool, &mergePool, timerLoop);
    defer recorder.deinit(io, alloc);
    defer {
        timerLoop.stop();
        timerLoop.join();
        timerLoop.deinit();
    }

    // tenant 1
    var tenant1Lines = try makeUniqueFieldLines(alloc, maxColumns + 1, 1);
    defer deinitLinesFull(alloc, &tenant1Lines);
    // tenant 2
    var tenant2Lines = try makeUniqueFieldLines(alloc, maxColumns + 1, 2);
    defer deinitLinesFull(alloc, &tenant2Lines);

    try recorder.shards[0].appendLines(alloc, tenant1Lines.items, .{ .tenantID = 1, .id = 1 });
    try recorder.shards[0].appendLines(alloc, tenant2Lines.items, .{ .tenantID = 2, .id = 1 });

    const table = (try recorder.shards[0].flush(
        io,
        alloc,
        timestampsEncoders,
        recorder.compressionPool,
        recorder.decompressionPool,
        &recorder.memMergeSem,
    )).?;
    defer table.close(io);

    const blockReader = try BlockReader.initFromMemTable(io, alloc, table, recorder.decompressionPool);
    defer blockReader.deinit(alloc);

    var seenTenants = [_]bool{ false, false };
    var blocks: usize = 0;
    while (try blockReader.nextBlock(io, alloc)) {
        try testing.expectEqual(maxColumns, blockReader.blockData.len);
        try testing.expectEqual(maxColumns, blockReader.columnsLen());
        try testing.expect(blockReader.blockData.sid.tenantID == 1 or blockReader.blockData.sid.tenantID == 2);

        seenTenants[blockReader.blockData.sid.tenantID - 1] = true;
        blocks += 1;
    }

    try testing.expectEqual(2, blocks);
    try testing.expectEqualDeep(&[_]bool{ true, true }, &seenTenants);
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

    const timestampsEncoders = try TimestampsEncoder.TimestampsEncoderPool.init(alloc, 1);
    defer timestampsEncoders.deinit(alloc);
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
    const recorder = try DataRecorder.init(io, alloc, rootPath, runtime, timestampsEncoders, compressionPool, decompressionPool, &mergePool, timerLoop);
    defer recorder.deinit(io, alloc);
    defer {
        timerLoop.stop();
        timerLoop.join();
        timerLoop.deinit();
    }

    var lines = [_]Line{
        stableLine(1, 0),
        stableLine(2, 1),
        stableLine(3, 2),
    };
    const table = try createMemTableFromLines(io, alloc, timestampsEncoders, recorder.compressionPool, stableSID(1), lines[0..]);
    errdefer table.close(io);

    try recorder.memTables.append(alloc, table);
    table.inMerge = true;

    var single = [_]*Table{table};
    try recorder.mergeTables(io, alloc, single[0..], true, null);
    try testing.expectEqual(@as(usize, 0), recorder.memTables.items.len);
    try testing.expectEqual(@as(usize, 1), recorder.diskTables.items.len);
    try testing.expect(recorder.diskTables.items[0].inner == .disk);
}

test "DataRecorder.addAndReopenPreservesLineCount" {
    const alloc = testing.allocator;
    const io = testing.io;

    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    const rootPath = try tmp.dir.realPathFileAlloc(io, ".", alloc);
    defer alloc.free(rootPath);

    const inserted: usize = 96;
    {
        const runtime = try Runtime.init(io, alloc, rootPath, 0.5);
        defer runtime.deinit(alloc);

        const timestampsEncoders = try TimestampsEncoder.TimestampsEncoderPool.init(alloc, 1);
        defer timestampsEncoders.deinit(alloc);
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
        const recorder = try DataRecorder.init(io, alloc, rootPath, runtime, timestampsEncoders, compressionPool, decompressionPool, &mergePool, timerLoop);
        defer recorder.deinit(io, alloc);
        defer {
            timerLoop.stop();
            timerLoop.join();
            timerLoop.deinit();
        }

        for (0..inserted) |i| {
            var batch = [_]Line{stableLine(@intCast(i + 1), i)};
            try recorder.addLines(io, alloc, batch[0..], stableSID(1));
        }

        try recorder.flushForce(io, alloc);

        try testing.expectEqual(0, recorder.memTables.items.len);
        try testing.expect(recorder.diskTables.items.len > 0);
        try testing.expectEqual(0, countMemLinesInRecorder(recorder));
        try testing.expectEqual(inserted, countDiskLinesInRecorder(recorder));
    }

    {
        const runtime = try Runtime.init(io, alloc, rootPath, 0.5);
        defer runtime.deinit(alloc);

        const timestampsEncoders = try TimestampsEncoder.TimestampsEncoderPool.init(alloc, 1);
        defer timestampsEncoders.deinit(alloc);
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
        const reopened = try DataRecorder.init(io, alloc, rootPath, runtime, timestampsEncoders, compressionPool, decompressionPool, &mergePool, timerLoop);
        defer reopened.deinit(io, alloc);
        defer {
            timerLoop.stop();
            timerLoop.join();
            timerLoop.deinit();
        }

        try testing.expect(reopened.diskTables.items.len > 0);
        try testing.expectEqual(0, countMemLinesInRecorder(reopened));
        try testing.expectEqual(inserted, countDiskLinesInRecorder(reopened));
    }
}

test "flushShard overflows memTables past maxMemTables when the semaphore wait times out" {
    const alloc = testing.allocator;
    const io = testing.io;

    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    const rootPath = try tmp.dir.realPathFileAlloc(io, ".", alloc);
    defer alloc.free(rootPath);

    const runtime = try Runtime.init(io, alloc, rootPath, 0.5);
    defer runtime.deinit(alloc);

    const timestampsEncoders = try TimestampsEncoder.TimestampsEncoderPool.init(alloc, 1);
    defer timestampsEncoders.deinit(alloc);
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
    const recorder = try DataRecorder.init(io, alloc, rootPath, runtime, timestampsEncoders, compressionPool, decompressionPool, &mergePool, timerLoop);
    defer recorder.deinit(io, alloc);
    defer {
        timerLoop.stop();
        timerLoop.join();
        timerLoop.deinit();
    }

    // Fill memTables to its cap with tables that are still "in merge" (simulating a
    // slow concurrent merger), so flushShard's forced flush can't reclaim a slot.
    for (0..maxMemTables) |i| {
        var lines = [_]Line{stableLine(@intCast(i + 1), i)};
        const table = try createMemTableFromLines(io, alloc, timestampsEncoders, recorder.compressionPool, stableSID(1), lines[0..]);
        table.inMerge = true;
        try recorder.memTables.append(alloc, table);
    }

    // Exhaust the semaphore so flushShard's timedWait must fail with error.Timeout.
    recorder.memTablesSem.permits = 0;

    var extraLine = [_]Line{stableLine(1000, 0)};
    try recorder.shards[0].appendLines(alloc, extraLine[0..], stableSID(2));

    // since we setup max fake tables that are 'in merge', but never gonna merge,
    // we expect the job to timeout instead of crashing due to overflowing the mem tables buffer
    try recorder.flushShard(io, alloc, &recorder.shards[0], false);

    // deinit test data
    for (recorder.memTables.items) |t| t.inMerge = false;
    try recorder.flushForce(io, alloc);
}

test "flushShard resets checkpointsLen on semaphore timeout so the next appendLines doesn't overflow" {
    const alloc = testing.allocator;
    const io = testing.io;

    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    const rootPath = try tmp.dir.realPathFileAlloc(io, ".", alloc);
    defer alloc.free(rootPath);

    const runtime = try Runtime.init(io, alloc, rootPath, 0.5);
    defer runtime.deinit(alloc);

    const timestampsEncoders = try TimestampsEncoder.TimestampsEncoderPool.init(alloc, 1);
    defer timestampsEncoders.deinit(alloc);
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
    const recorder = try DataRecorder.init(io, alloc, rootPath, runtime, timestampsEncoders, compressionPool, decompressionPool, &mergePool, timerLoop);
    defer recorder.deinit(io, alloc);
    defer {
        timerLoop.stop();
        timerLoop.join();
        timerLoop.deinit();
    }

    // fill memTables to its cap with tables still "in merge" (simulating a slow concurrent
    // merger), so flushShard forced flush can't reclaim a slot and both semaphore waits time out
    for (0..maxMemTables) |i| {
        var lines = [_]Line{stableLine(@intCast(i + 1), i)};
        const table = try createMemTableFromLines(io, alloc, timestampsEncoders, recorder.compressionPool, stableSID(1), lines[0..]);
        table.inMerge = true;
        try recorder.memTables.append(alloc, table);
    }
    recorder.memTablesSem.permits = 0;

    // fill the shard checkpoints up to one below the limit with distinct sids, matching what
    // DataRecorder.addLines' round-robin shard assignment can produce under concurrency
    const shard = &recorder.shards[0];
    for (0..DataShard.maxCheckpoints - 1) |i| {
        var lines = [_]Line{stableLine(@intCast(i + 1), i)};
        try shard.appendLines(alloc, lines[0..], stableSID(@intCast(i + 2)));
    }
    try testing.expectEqual(DataShard.maxCheckpoints - 1, shard.checkpointsLen);

    var extraLine = [_]Line{stableLine(1000, 0)};
    try shard.appendLines(alloc, extraLine[0..], stableSID(1000));
    try testing.expectEqual(DataShard.maxCheckpoints, shard.checkpointsLen);

    // both semaphore waits time out
    // which must reset checkpointsLen
    // along with lines/buffer.
    try recorder.flushShard(io, alloc, shard, false);
    try testing.expectEqual(0, shard.checkpointsLen);

    // validate bound check, shard buffer must reset after flush
    var nextLine = [_]Line{stableLine(2000, 0)};
    try shard.appendLines(alloc, nextLine[0..], stableSID(2000));

    for (recorder.memTables.items) |t| t.inMerge = false;
    try recorder.flushForce(io, alloc);
}

// TODO: benchmark different filesystems
// TODO: benchmark different IO schedulers
// TODO: try tagging fadvise with different access patterns
// TODO: experiment with mmap files in merges
// since it's a single threaded operation we don't expect os lock,
// or write a blog post why it doesn't fit
