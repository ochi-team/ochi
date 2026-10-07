// TODO: data and index recorders are both hold a lot in common,
// we must desine a single component to manage both
const std = @import("std");
const Allocator = std.mem.Allocator;
const FixedBufferAllocator = std.heap.FixedBufferAllocator;
const Io = std.Io;

const tracy = @import("tracy");

const fs = @import("fs.zig");

const timedWait = @import("stds/sem.zig").timedWait;

const Line = @import("store/lines.zig").Line;
const Field = @import("store/lines.zig").Field;
const validate = @import("store/lines.zig").validate;
const maxLines = @import("store/data/Block.zig").maxLines;
const Query = @import("query/Query.zig");
const SID = @import("store/lines.zig").SID;

const MemTable = @import("store/data/MemTable.zig");
const BlockWriter = @import("store/data/BlockWriter.zig");
const TableWriter = @import("store/data/TableWriter.zig");
const TimestampsEncoder = @import("store/data/TimestampsEncoder.zig");
const CompressionPool = @import("store/compression/CompressionPool.zig");
const DecompressionPool = @import("store/compression/DecompressionPool.zig");
const Table = @import("store/data/Table.zig");
const BlockReader = @import("store/data/BlockReader.zig");
const mergeBlocks = @import("store/data/merge.zig").mergeBlocks;
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
};

const MergeTask = struct {
    task: xev.ThreadPool.Task,
    ctx: TaskCtx,
    run: *const fn (*DataRecorder, Io, Allocator) void,

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
    recorder: *DataRecorder = undefined,
    xevTimer: xev.Timer,
    completion: xev.Completion = .{},
    cancelCompletion: xev.Completion = .{},
    table: ?*Table = null,
};

pub const maxMemTables = 16;
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

    pub fn appendLines(shard: *DataShard, alloc: Allocator, lines: []const Line, sid: SID) !void {
        const z = tracy.Zone.begin(.{
            .src = @src(),
            .name = "DataShard.appendLines",
        });
        defer z.end();

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
    pub fn flush(
        self: *DataShard,
        io: Io,
        alloc: Allocator,
        timestampsEncoders: *TimestampsEncoder.TimestampsEncoderPool,
        compressionPool: *CompressionPool,
        decompressionPool: *DecompressionPool,
        sem: *Io.Semaphore,
    ) !?*Table {
        const z = tracy.Zone.begin(.{
            .src = @src(),
            .name = "DataShard.flush",
        });
        defer z.end();

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
// counter to complete all the active ticks before handling stopped event
activeTicks: std.atomic.Value(usize) = .init(0),
mergeIdx: std.atomic.Value(usize),
path: []const u8,
runtime: *Runtime,
timestampsEncoders: *TimestampsEncoder.TimestampsEncoderPool,
compressionPool: *CompressionPool,
decompressionPool: *DecompressionPool,

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
        .tableTimerSlots = tableTimerSlots,
    };

    t.taskCtx = .{ .recorder = t, .io = io, .alloc = alloc };
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
    self.waitForTicksToDrain(io);
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
    self.waitForMergesToDrain(io);
    try self.flushMemTables(io, alloc, true);
}

pub fn deinit(self: *DataRecorder, io: Io, alloc: Allocator) void {
    self.waitForMergesToDrain(io);

    std.debug.assert(self.memTables.items.len == 0);

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
    self.* = undefined;
    alloc.destroy(self);
}

fn waitForMergesToDrain(self: *DataRecorder, io: Io) void {
    while (self.pendingMerges.load(.acquire) != 0) {
        Io.sleep(io, .fromMilliseconds(1), .real) catch {
            return;
        };
    }
}

fn waitForTicksToDrain(self: *DataRecorder, io: Io) void {
    while (self.activeTicks.load(.acquire) != 0) {
        Io.sleep(io, .fromMilliseconds(1), .real) catch {
            return;
        };
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
    const z = tracy.Zone.begin(.{
        .src = @src(),
        .name = "DataRecorder.shardTimerCallback",
    });
    defer z.end();

    _ = r catch |err| {
        Logger.log(.err, "failed to run data shardTimerCallback", .{ .err = err });
    };
    const shard = ud.?;
    const self = shard.parent;

    _ = self.activeTicks.fetchAdd(1, .acquire);
    defer _ = self.activeTicks.fetchSub(1, .release);

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
    const io = self.taskCtx.io;

    self.mxTables.lockUncancelable(io);
    const found = for (&self.tableTimerSlots) |*s| {
        if (s.table == null or s.table.?.inMerge) break s;
    } else null;
    self.mxTables.unlock(io);

    const slot = found orelse {
        Logger.log(.err, "DataRecorder: no free table timer slot, dropping scheduled flush", .{});
        table.release(io);
        return;
    };

    if (slot.table) |stale| stale.release(io);
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
    const z = tracy.Zone.begin(.{
        .src = @src(),
        .name = "DataRecorder.tableTimerCallback",
    });
    defer z.end();

    _ = loop;
    _ = c;
    _ = r catch |err| {
        Logger.log(.err, "failed to run data tableTimerCallback", .{ .err = err });
    };
    const slot = ud.?;
    const self = slot.recorder;
    const table = slot.table.?;
    const io = self.taskCtx.io;

    defer {
        table.release(io);
        slot.table = null;
    }

    _ = self.activeTicks.fetchAdd(1, .acquire);
    defer _ = self.activeTicks.fetchSub(1, .release);

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

pub fn flushDataShards(self: *DataRecorder, io: Io, allocator: Allocator, force: bool) !void {
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

pub fn flushShard(self: *DataRecorder, io: Io, alloc: Allocator, shard: *DataShard, force: bool) !void {
    const z = tracy.Zone.begin(.{
        .src = @src(),
        .name = "DataRecorder.flushShard",
    });
    defer z.end();

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
                    // TODO: this wait times out quite often,
                    // we have to avoid it, it declines latency and throughput
                    timedWait(&self.memTablesSem, io, std.time.ns_per_s / 100) catch |e| {
                        switch (e) {
                            error.Timeout => {
                                Logger.log(.warn, "data: mem tables buffer is full, flush mem table", .{});

                                const destinationTablePath = try self.diskTablePath(alloc, .disk);
                                errdefer if (destinationTablePath.len > 0) alloc.free(destinationTablePath);

                                // pass empty list tables because we have nothing to merge/replace,
                                // it must only flush to disk a passed mem table and not remove existing tables,
                                // but perform semaphore
                                try self.flushMemTable(io, alloc, memTable.inner.mem, &[_]*Table{}, destinationTablePath, .disk);
                                memTable.release(io);
                            },
                            error.Canceled => return err,
                        }

                        // second timeout, we flushed the table to the disk, early return
                        return;
                    };
                },
                error.Canceled => return err,
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

fn runDiskTablesMerger(self: *DataRecorder, io: Io, alloc: Allocator) void {
    const z = tracy.Zone.begin(.{
        .src = @src(),
        .name = "DataRecorder.runDiskTablesMerger",
    });
    defer z.end();

    self.tablesMerger(io, alloc, &self.diskTables, &self.diskMergeSem) catch |err| {
        if (err == error.Stopped) return;

        self.stopped.stop(io);
        Logger.log(.err, "failed to merge disk tables", .{ .err = err });
    };
}

fn runMemTableMerger(self: *DataRecorder, io: Io, alloc: Allocator) void {
    const z = tracy.Zone.begin(.{
        .src = @src(),
        .name = "DataRecorder.runMemTableMerger",
    });
    defer z.end();

    self.tablesMerger(io, alloc, &self.memTables, &self.memMergeSem) catch |err| {
        if (err == error.Stopped) return;

        self.stopped.stop(io);
        Logger.log(.err, "failed to merge mem tables", .{ .err = err });
    };
}

pub fn tablesMerger(
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

pub fn mergeTables(
    self: *DataRecorder,
    io: Io,
    alloc: Allocator,
    tables: []*Table,
    force: bool,
    stopped: ?*const Stop,
) !void {
    const z = tracy.Zone.begin(.{
        .src = @src(),
        .name = "DataRecorder.mergeTables",
    });
    defer z.end();

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

    // TODO: this errdefer is broken, it can break on merge after the table owns it
    // and it creates double free;
    // all tables already own, make it using a static buffer and make tables copy the incoming buffer
    // to eliminate this "move"
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

    const tableHeader = mergeBlocks(io, alloc, self.timestampsEncoders, self.decompressionPool, streamWriter, &readers, stopped) catch |err| {
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

        try tableHeader.writeFile(io, destinationTablePath);

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
    const z = tracy.Zone.begin(.{
        .src = @src(),
        .name = "DataRecorder.flushMemTable",
    });
    defer z.end();

    try memTable.storeToDisk(io, destinationTablePath);

    const newTable = try openCreatedTable(io, alloc, destinationTablePath, null, self.decompressionPool);
    errdefer newTable.release(io);

    try swapper.swapTables(self, io, alloc, tables, newTable, tableKind);
}

pub fn addLines(self: *DataRecorder, io: Io, alloc: Allocator, lines: []const Line, sid: SID) !void {
    const z = tracy.Zone.begin(.{
        .src = @src(),
        .name = "DataRecorder.addLine",
    });
    defer z.end();

    const i = self.nextShard.fetchAdd(1, .monotonic) % self.shards.len;
    var shard = &self.shards[i];

    shard.mx.lockUncancelable(io);
    defer shard.mx.unlock(io);

    const start = shard.lines.items.len;
    shard.appendLines(alloc, lines, sid) catch |err| {
        switch (err) {
            Allocator.Error.OutOfMemory => {
                Logger.log(.warn, "data shard: buffer overflow, decrease flush threashold", .{});
                const offset = shard.lines.items.len - start;
                try self.flushShard(io, alloc, shard, false);
                shard.appendLines(alloc, lines[offset..], sid) catch |e| {
                    Logger.log(.err, "data shard: buffer doesn't fit input lines", .{ .err = e });
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

pub fn queryLines(self: *DataRecorder, io: Io, requestArena: Allocator, sids: []SID, query: Query) !std.ArrayList(Line) {
    var tables = try self.getTables(io, requestArena, query.start, query.end);
    defer {
        for (tables.items) |table| table.release(io);
        tables.deinit(requestArena);
    }

    var linesDst = std.ArrayList(Line).empty;
    errdefer linesDst.deinit(requestArena);
    for (tables.items) |table| {
        try table.queryLines(io, requestArena, true, self.timestampsEncoders, self.decompressionPool, &linesDst, sids, query);
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
        memTable.flushAtUs = getFlushTime(io);
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

pub fn selectTablesInRange(
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
