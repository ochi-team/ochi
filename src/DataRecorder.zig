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

const Stop = @import("stds/Stop.zig");

const flush = @import("store/table/flush.zig");
const merge = @import("store/table/merge.zig");
const cap = @import("store/table/cap.zig");
const swap = @import("store/table/swap.zig");

const Consts = @import("Consts.zig");

const flushSizeThreshold = Consts.flushSizeThreshold;
const amountOfTablesToMerge = Consts.amountOfTablesToMerge;
const maxBlockSize = Consts.maxBlockSize;

const maxMemTables = 24;
const merger = merge.Merger(*Table, maxMemTables, amountOfTablesToMerge);
const swapper = swap.Swapper(DataRecorder, Table);

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
    // TODO: take a meter to understand if we should increase checkpoints array size,
    // the ration between checkpoints len and size on flushing must be close
    checkpoints: [maxCheckpoints]SidCheckpoint = undefined,
    checkpointsLen: u16 = 0,
    buffer: FixedBufferAllocator,

    // TODO: currently there is a single background process flushing the data shards
    // try instead assign a timer task to a shard and benchmark on high amount of shard (high amount of cpu)
    flushAtUs: ?i64 = null,

    pub const maxCheckpoints = 16;

    fn reset(self: *DataShard) void {
        self.lines.clearRetainingCapacity();
        self.buffer.reset();
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
        }

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

    fn mustFlush(self: *const DataShard) bool {
        return self.buffer.end_index >= flushSizeThreshold or self.checkpointsLen == maxCheckpoints;
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
g: Io.Group = .init,
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

pub fn init(
    io: Io,
    alloc: Allocator,
    path: []const u8,
    runtime: *Runtime,
    timestampsEncoders: *TimestampsEncoder.TimestampsEncoderPool,
    compressionPool: *CompressionPool,
    decompressionPool: *DecompressionPool,
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
        shard.* = .{
            .buffer = FixedBufferAllocator.init(buf),
        };
        shardsInited += 1;
    }

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
        .path = path,
        .runtime = runtime,
        .timestampsEncoders = timestampsEncoders,
        .compressionPool = compressionPool,
        .decompressionPool = decompressionPool,
    };

    return t;
}

pub fn createDir(io: Io, path: []const u8) !void {
    try fs.createDirAssert(io, path);
    try fs.syncPathAndParentDir(io, path);
}

pub fn start(self: *DataRecorder, io: Io, alloc: Allocator) !void {
    for (0..self.concurrency) |_| {
        try self.startDiskTablesMerge(io, alloc);
    }

    try self.startMemTablesFlusher(io, alloc);
    try self.startDataShardsFlusher(io, alloc);
}

// TODO: find an approach to make it never fail,
// the only option it fails is OOM, so cleaning more memory in advance might be more reliable
// another problem it's hard to test it via checkAllAllocationFailures.
// Then audit all deinits and use it instead
// TODO: make using this API instead of directly managing stopped state in the tests
// TODO: this theoretically is not enough to stop the other jobs form starting,
// either lock stop or find another way to make sure none of the task are running after wg.wait
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
    self.g.await(io) catch |err| {
        switch (err) {
            error.Canceled => {},
        }
    };

    try self.flushForce(io, alloc);
}

pub fn flushForce(self: *DataRecorder, io: Io, alloc: Allocator) !void {
    try self.flushDataShards(io, alloc, true);
    try self.flushMemTables(io, alloc, true);
}

pub fn deinit(self: *DataRecorder, io: Io, allocator: Allocator) void {
    // make sure deinit is never called outside of stop
    std.debug.assert(self.memTables.items.len == 0);

    for (self.shards) |*shard| {
        shard.deinit(allocator);
    }
    for (self.diskTables.items) |table| {
        table.release(io);
    }
    for (self.memTables.items) |table| {
        table.release(io);
    }

    self.memTables.deinit(allocator);
    self.diskTables.deinit(allocator);
    allocator.free(self.shards);
    self.g.cancel(io);
    self.* = undefined;
    allocator.destroy(self);
}

fn startMemTablesFlusher(self: *DataRecorder, io: Io, alloc: Allocator) !void {
    errdefer self.g.cancel(io);

    try self.g.concurrent(io, runMemTablesFlusher, .{ self, io, alloc });
}

fn startDataShardsFlusher(self: *DataRecorder, io: Io, alloc: Allocator) !void {
    errdefer self.g.cancel(io);

    try self.g.concurrent(io, runDataShardsFlusher, .{ self, io, alloc });
}

fn runMemTablesFlusher(self: *DataRecorder, io: Io, alloc: Allocator) void {
    while (!self.stopped.isStopped()) {
        self.flushMemTables(io, alloc, false) catch |err| {
            if (err == error.Stopped) return;

            self.stopped.stop(io);
            Logger.log(.err, "failed to run mem tables flusher", .{ .err = err });
            return;
        };

        self.stopped.sleepOrStop(io, std.time.ns_per_s);
    }
}

fn runDataShardsFlusher(self: *DataRecorder, io: Io, alloc: Allocator) void {
    // half a sec
    // TODO: test it with 1 sec
    const flushInterval = std.time.ns_per_s / 2;

    while (!self.stopped.isStopped()) {
        self.flushDataShards(io, alloc, false) catch |err| {
            if (err == error.Stopped) return;

            self.stopped.stop(io);
            Logger.log(.err, "failed to run data shards flusher", .{ .err = err });
            return;
        };

        self.stopped.sleepOrStop(io, flushInterval);
    }

    self.flushDataShards(io, alloc, true) catch |err| {
        if (err == error.Stopped) return;

        self.stopped.stop(io);
        Logger.log(.err, "failed to run force data shards flusher", .{ .err = err });
        return;
    };
}

fn flushMemTables(self: *DataRecorder, io: Io, allocator: Allocator, force: bool) !void {
    const nowUs = Io.Timestamp.now(io, .real).toMicroseconds();
    self.mxTables.lockUncancelable(io);

    var tables = std.ArrayList(*Table).initCapacity(allocator, self.memTables.items.len) catch |err| {
        self.mxTables.unlock(io);
        return err;
    };
    defer tables.deinit(allocator);

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

    var left = std.ArrayList(*Table).initBuffer(toFlush.items[0..]);
    left.items.len = toFlush.items.len;

    while (left.items.len > 0) {
        const n = merger.selectTablesToMerge(&left);
        std.debug.assert(n > 0);

        // TODO: attempt to run it in parallel, add a semaphore then
        try self.mergeTables(io, alloc, left.items[0..n], true, null);

        const tail = left.items[n..];
        left = std.ArrayList(*Table).initBuffer(tail);
        left.items.len = tail.len;
    }
}

fn flushDataShards(self: *DataRecorder, io: Io, allocator: Allocator, force: bool) !void {
    if (force) {
        for (self.shards) |*shard| {
            // if it's not locked we are adding lines just know, makes no sense to lock it yet
            if (shard.mx.tryLock()) {
                defer shard.mx.unlock(io);
                try self.flushShard(io, allocator, shard);
            } else {
                Logger.log(.debug, "skipping shard flush because it is locked", .{});
            }
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
                    try self.flushShard(io, allocator, shard);
                }
            }
        } else {
            Logger.log(.debug, "skipping shard flush because it is locked", .{});
        }
    }
}

fn flushShard(self: *DataRecorder, io: Io, alloc: Allocator, shard: *DataShard) !void {
    const maybeMemTable = try shard.flush(io, alloc, self.timestampsEncoders, self.compressionPool, self.decompressionPool, &self.memMergeSem);
    if (maybeMemTable) |memTable| {
        self.mxTables.lockUncancelable(io);
        defer self.mxTables.unlock(io);
        try self.memTables.append(alloc, memTable);

        shard.flushAtUs = null;
        shard.checkpointsLen = 0;

        try self.startMemTablesMerge(io, alloc);
    }
}

pub fn startDiskTablesMerge(self: *DataRecorder, io: Io, alloc: Allocator) !void {
    if (self.stopped.isStopped()) return;

    errdefer self.g.cancel(io);

    try self.g.concurrent(io, runDiskTablesMerger, .{ self, io, alloc });
}

pub fn startMemTablesMerge(self: *DataRecorder, io: Io, alloc: Allocator) !void {
    if (self.stopped.isStopped()) return;

    errdefer self.g.cancel(io);

    try self.g.concurrent(io, runMemTableMerger, .{ self, io, alloc });
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
    var tablesToMerge = std.ArrayList(*Table).empty;
    defer tablesToMerge.deinit(alloc);

    while (!self.stopped.isStopped()) {
        const maxDiskTableSize = cap.getMaxTableSize(self.runtime.getFreeDiskSpace(io));

        self.mxTables.lockUncancelable(io);
        // TODO: we have to know the max amount of tables in advance
        tablesToMerge.ensureUnusedCapacity(alloc, tables.items.len) catch |err| {
            self.mxTables.unlock(io);
            return err;
        };
        // filteredTablesToMerge is a slice of tables ArrayList, no need to free it
        const window = merger.filterTablesToMerge(tables.items, &tablesToMerge, maxDiskTableSize);
        self.mxTables.unlock(io);

        const w = window orelse return;
        const filteredTablesToMerge = tablesToMerge.items[w.lower..w.upper];
        if (filteredTablesToMerge.len == 0) return;

        sem.waitUncancelable(io);
        errdefer sem.post(io);
        try self.mergeTables(io, alloc, filteredTablesToMerge, false, &self.stopped);
        sem.post(io);
        tablesToMerge.clearRetainingCapacity();
    }
}

fn nextMergeIdx(self: *DataRecorder) usize {
    return self.mergeIdx.fetchAdd(1, .acq_rel);
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

    const destinationTablePath: []u8 =
        if (tableKind == .disk) blk: {
            // 1 for / and 16 for 16 bytes of idx representation,
            // we can't bitcast it to [8]u8 because we need human readlable file names
            const mergeIdx = self.nextMergeIdx();

            const path = try alloc.alloc(u8, self.path.len + 1 + 16);
            errdefer alloc.free(path);

            _ = try std.fmt.bufPrint(path, "{s}/{X:0>16}", .{ self.path, mergeIdx });

            break :blk path;
        } else "";
    errdefer if (destinationTablePath.len > 0)
        alloc.free(destinationTablePath);

    if (force and tables.len == 1 and tables[0].inner == .mem) {
        const table = tables[0].inner.mem;
        try table.storeToDisk(io, alloc, destinationTablePath);

        const newTable = try openCreatedTable(io, alloc, destinationTablePath, tables, null, self.decompressionPool);
        errdefer newTable.release(io);

        try swapper.swapTables(self, io, alloc, tables, newTable, tableKind);
        swapped = true;
        return;
    }

    var readers = try openTableReaders(io, alloc, tables, self.decompressionPool);
    defer {
        for (readers.items) |reader| reader.deinit(alloc);
        readers.deinit(alloc);
    }

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

    const openTable = try openCreatedTable(io, alloc, destinationTablePath, tables, newMemTable, self.decompressionPool);
    errdefer openTable.release(io);

    try swapper.swapTables(self, io, alloc, tables, openTable, tableKind);
    swapped = true;
}

pub fn addLines(self: *DataRecorder, io: Io, alloc: Allocator, lines: []const Line, sid: SID) !void {
    const i = self.nextShard.fetchAdd(1, .acquire) % self.shards.len;
    var shard = &self.shards[i];

    shard.mx.lockUncancelable(io);
    defer shard.mx.unlock(io);

    shard.appendLines(alloc, lines, sid) catch |err| {
        switch (err) {
            Allocator.Error.OutOfMemory => {
                Logger.log(.warn, "processor: buffer overflow, decrease flush threashold", .{});
                try self.flushShard(io, alloc, shard);
                try shard.appendLines(alloc, lines, sid);
            },
        }
    };

    if (shard.mustFlush()) {
        try self.flushShard(io, alloc, shard);
    } else if (shard.flushAtUs == null) {
        shard.flushAtUs = getFlushTime(io);
    }
}

pub fn queryLines(self: *DataRecorder, io: Io, alloc: Allocator, sids: []SID, query: Query) !std.ArrayList(Line) {
    self.mxTables.lockUncancelable(io);

    const stackSize = 64;
    var fba = std.heap.stackFallback(stackSize, alloc);
    const fbaAlloc = fba.get();
    var tables = std.ArrayList(*Table).initCapacity(fbaAlloc, stackSize / @sizeOf(*Table)) catch |err| {
        // panic because we allocate precise amount on stack
        std.debug.panic("failed to allocate tables array list for query: {s}\n", .{@errorName(err)});
    };
    defer {
        for (tables.items) |table| table.release(io);
        tables.deinit(fbaAlloc);
    }

    selectTablesInRange(fbaAlloc, &tables, self.memTables.items, query.start, query.end) catch |err| {
        self.mxTables.unlock(io);
        return err;
    };
    selectTablesInRange(fbaAlloc, &tables, self.diskTables.items, query.start, query.end) catch |err| {
        self.mxTables.unlock(io);
        return err;
    };
    self.mxTables.unlock(io);

    var linesDst = std.ArrayList(Line).empty;
    errdefer linesDst.deinit(alloc);
    for (tables.items) |table| {
        try table.queryLines(io, alloc, self.timestampsEncoders, self.decompressionPool, &linesDst, sids, query);
    }

    return linesDst;
}

fn openCreatedTable(
    io: Io,
    alloc: Allocator,
    tablePath: []const u8,
    tables: []*Table,
    maybeMemTable: ?*MemTable,
    decompressionPool: *DecompressionPool,
) !*Table {
    if (maybeMemTable) |memTable| {
        memTable.flushAtUs = flush.getFlushTablesToDiskDeadline(io, *Table, tables);
        return Table.fromMem(io, alloc, memTable, decompressionPool);
    }

    return Table.open(io, alloc, tablePath, decompressionPool);
}

fn openTableReaders(io: Io, alloc: Allocator, tables: []*Table, decompressionPool: *DecompressionPool) !std.ArrayList(*BlockReader) {
    var readers = try std.ArrayList(*BlockReader).initCapacity(alloc, tables.len);
    errdefer {
        for (readers.items) |reader| reader.deinit(alloc);
        readers.deinit(alloc);
    }
    for (tables) |table| {
        const reader = try BlockReader.init(io, alloc, table, decompressionPool);
        readers.appendAssumeCapacity(reader);
    }

    return readers;
}

fn selectTablesInRange(
    alloc: Allocator,
    dst: *std.ArrayList(*Table),
    tables: []const *Table,
    start_: u64,
    end: u64,
) !void {
    for (tables) |table| {
        if (table.tableHeader().maxTimestamp < start_ or table.tableHeader().minTimestamp > end) {
            continue;
        }
        table.retain();
        try dst.append(alloc, table);
    }
}

const testing = std.testing;
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

        const recorder = try DataRecorder.init(io, alloc, rootPath, runtime, timestampsEncoders, compressionPool, decompressionPool);
        defer recorder.deinit(io, alloc);

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
                    recorder.nextShard.store(0, .release);
                    var lines = [_]Line{stableLine(@intCast(i + 1), i)};
                    try recorder.addLines(io, alloc, lines[0..], stableSID(i + 1));
                }
            },
            .deadline => {
                recorder.nextShard.store(0, .release);
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

                recorder.nextShard.store(0, .release);
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

    const recorder = try DataRecorder.init(io, alloc, rootPath, runtime, timestampsEncoders, compressionPool, decompressionPool);
    defer recorder.deinit(io, alloc);

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

    const recorder = try DataRecorder.init(io, alloc, rootPath, runtime, timestampsEncoders, compressionPool, decompressionPool);
    defer recorder.deinit(io, alloc);

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

        const recorder = try DataRecorder.init(io, alloc, rootPath, runtime, timestampsEncoders, compressionPool, decompressionPool);
        defer recorder.deinit(io, alloc);

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

        const reopened = try DataRecorder.init(io, alloc, rootPath, runtime, timestampsEncoders, compressionPool, decompressionPool);
        defer reopened.deinit(io, alloc);

        try testing.expect(reopened.diskTables.items.len > 0);
        try testing.expectEqual(0, countMemLinesInRecorder(reopened));
        try testing.expectEqual(inserted, countDiskLinesInRecorder(reopened));
    }
}

// TODO: benchmark different filesystems
// TODO: benchmark different IO schedulers
// TODO: try tagging fadvise with different access patterns
// TODO: experiment with mmap files in merges
// since it's a single threaded operation we don't expect os lock,
// or write a blog post why it doesn't fit
