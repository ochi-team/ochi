// TODO: data and index recorders are both hold a lot in common,
// we must desine a single component to manage both
const std = @import("std");
const Allocator = std.mem.Allocator;
const Io = std.Io;

const Line = @import("store/lines.zig").Line;
const Field = @import("store/lines.zig").Field;
const defaultMaxFieldValueSize = @import("store/lines.zig").defaultMaxFieldValueSize;
const deinitLinesFull = @import("store/lines.zig").deinitLinesFull;
const maxColumns = @import("store/data/Block.zig").maxColumns;
const maxLines = @import("store/data/Block.zig").maxLines;
const SID = @import("store/lines.zig").SID;

const MemTable = @import("store/data/MemTable.zig");
const TimestampsEncoder = @import("store/data/TimestampsEncoder.zig");
const CompressionPool = @import("store/compression/CompressionPool.zig");
const DecompressionPool = @import("store/compression/DecompressionPool.zig");
const TableHeader = @import("store/data/TableHeader.zig");
const Table = @import("store/data/Table.zig");
const BlockReader = @import("store/data/BlockReader.zig");
const Runtime = @import("Runtime.zig");
const xev = @import("xev");

const TimerLoop = @import("stds/xev/TimerLoop.zig");

const Consts = @import("Consts.zig");

const flushSizeThreshold = Consts.flushSizeThreshold;
const amountOfTablesToMerge = Consts.amountOfTablesToMerge;
const maxBlockSize = Consts.maxBlockSize;

const DataRecorder = @import("DataRecorder.zig");
const DataShard = DataRecorder.DataShard;
const TableTimerSlot = DataRecorder.TableTimerSlot;
const tableTimerCallback = DataRecorder.tableTimerCallback;
const selectTablesInRange = DataRecorder.selectTablesInRange;
const maxMemTables = DataRecorder.maxMemTables;

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
        .fields = .view(fields),
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
    try memTable.storeToDisk(io, tablePath);
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
    table.size = 1;

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
                        .fields = .view(fields[i .. i + 1]),
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

test "DataRecorder.addLines skips busy shards and preserves data after reopen" {
    const alloc = testing.allocator;
    const io = testing.io;

    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    const rootPath = try tmp.dir.realPathFileAlloc(io, ".", alloc);
    defer alloc.free(rootPath);

    const runtime = try Runtime.init(io, alloc, rootPath, 0.5);
    defer runtime.deinit(alloc);
    runtime.cpus = 3;

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

    const cases = [_]struct { start: usize, held: []const usize, expected: usize }{
        .{ .start = 0, .held = &.{}, .expected = 0 },
        .{ .start = 0, .held = &.{0}, .expected = 1 },
        .{ .start = 0, .held = &.{ 0, 1 }, .expected = 2 },
        .{ .start = 2, .held = &.{ 2, 0 }, .expected = 1 },
    };

    for (0..2) |pass| {
        const timerLoop = try TimerLoop.init(alloc);
        const recorder = try DataRecorder.init(io, alloc, rootPath, runtime, timestampsEncoders, compressionPool, decompressionPool, &mergePool, timerLoop);
        defer recorder.deinit(io, alloc);
        defer {
            timerLoop.stop();
            timerLoop.join();
            timerLoop.deinit();
        }

        if (pass == 0) {
            for (cases, 0..) |case, caseIndex| {
                {
                    for (case.held) |index| recorder.shards[index].mx.lockUncancelable(io);
                    defer for (case.held) |index| recorder.shards[index].mx.unlock(io);

                    recorder.nextShard.store(case.start, .monotonic);
                    const lines = [_]Line{stableLine(caseIndex + 1, 3)};
                    try recorder.addLines(io, alloc, &lines, stableSID(caseIndex + 1));

                    try testing.expectEqual(case.start + 1, recorder.nextShard.load(.monotonic));
                    for (recorder.shards, 0..) |*shard, index| {
                        try testing.expectEqual(@intFromBool(index == case.expected), shard.lines.items.len);
                    }
                    const acquired = &recorder.shards[case.expected];
                    try testing.expectEqualDeep(lines[0], acquired.lines.items[0]);
                    try testing.expect(acquired.flushAtUs != null);
                    try testing.expect(acquired.mx.tryLock());
                    acquired.mx.unlock(io);
                    for (case.held) |index| try testing.expect(!recorder.shards[index].mx.tryLock());
                }
                try recorder.flushForce(io, alloc);
            }
        }

        try testing.expectEqual(0, recorder.memTables.items.len);
        try testing.expectEqual(cases.len, countDiskLinesInRecorder(recorder));

        var arena = std.heap.ArenaAllocator.init(alloc);
        defer arena.deinit();
        for (cases, 0..) |_, caseIndex| {
            var sids = [_]SID{stableSID(caseIndex + 1)};
            const queried = try recorder.queryLines(io, arena.allocator(), &sids, .{ .start = 0, .end = cases.len + 1 });
            try testing.expectEqual(1, queried.items.len);
            const expected = stableLine(caseIndex + 1, 3);
            const actual = queried.items[0];
            try testing.expectEqualDeep(expected, Line{
                .timestampNs = actual.timestampNs,
                .fields = .view(actual.fields.items),
            });
        }
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

test "data table timer retains and rearms an early callback" {
    const State = struct {
        retainsTable: bool,
        references: u32,
        completion: xev.CompletionState,
        activeTicks: usize,
        inMerge: bool,
    };

    // keep deadlines ahead of execution so scheduler rearms
    for ([_]i64{ 10, 60 }) |remainingSeconds| {
        var loop = try xev.Loop.init(.{});
        defer loop.deinit();
        var recorder: DataRecorder = undefined;
        recorder.taskCtx = .{ .recorder = &recorder, .io = testing.io, .alloc = testing.allocator };
        recorder.stopped = .{};
        recorder.activeTicks = .init(0);
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
            .activeTicks = 0,
            .inMerge = false,
        }, State{
            .retainsTable = slot.table == &table,
            .references = table.refCounter.load(.acquire),
            .completion = slot.completion.state(),
            .activeTicks = recorder.activeTicks.load(.acquire),
            .inMerge = table.inMerge,
        });
    }
}

// TODO: benchmark different filesystems (btrfs)
// TODO: benchmark different IO schedulers
// TODO: try tagging fadvise with different access patterns
// TODO: experiment with mmap files in merges
// since it's a single threaded operation we don't expect os lock,
// or write a blog post why it doesn't fit
