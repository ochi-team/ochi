const std = @import("std");
const Allocator = std.mem.Allocator;

const Swapper = @import("swap.zig").Swapper;
const TableKind = @import("merge.zig").TableKind;

const testing = std.testing;
const checkAllAllocationFailures = @import("../../stds/testing.zig").checkAllAllocationFailures;

const Recorder = struct {
    const Self = @This();

    const TestTable = struct {
        toRemove: std.atomic.Value(bool) = .init(false),
        releases: usize = 0,
        catalogWrites: usize = 0,

        failCatalog: bool = false,

        pub fn release(self: *@This(), _: std.Io) void {
            self.releases += 1;
        }

        pub fn writeNames(_: std.Io, _: Allocator, _: []const u8, tables: []*@This()) !void {
            for (tables) |table| {
                table.catalogWrites += 1;
                if (table.failCatalog) return error.CatalogFailure;
            }
        }
    };
    mxTables: std.Io.Mutex = .init,
    memTables: std.ArrayList(*TestTable) = .empty,
    diskTables: std.ArrayList(*TestTable) = .empty,
    memTablesSem: std.Io.Semaphore = .{ .permits = 0 },
    path: []const u8 = "",
    memMerges: usize = 0,
    diskMerges: usize = 0,

    failScheduling: bool = false,

    pub fn startMemTablesMerge(self: *Self, _: std.Io, _: Allocator) !void {
        if (self.failScheduling) return error.SchedulingFailure;
        self.memMerges += 1;
    }

    pub fn startDiskTablesMerge(self: *Self, _: std.Io, _: Allocator) !void {
        if (self.failScheduling) return error.SchedulingFailure;
        self.diskMerges += 1;
    }
};

test "swapTables preserves sources when publication fails" {
    const f = struct {
        fn f(alloc: Allocator, kind: TableKind, failScheduling: bool, expectedErr: anyerror) !void {
            var source: Recorder.TestTable = .{};
            var destination: Recorder.TestTable = .{ .failCatalog = true };

            var recorder: Recorder = .{ .failScheduling = failScheduling };
            defer recorder.memTables.deinit(alloc);
            defer recorder.diskTables.deinit(alloc);

            try recorder.memTables.append(alloc, &source);

            var sources = [_]*Recorder.TestTable{&source};
            Swapper(Recorder, Recorder.TestTable).swapTables(
                &recorder,
                testing.io,
                alloc,
                &sources,
                &destination,
                kind,
            ) catch |err| {
                try testing.expectEqualSlices(*Recorder.TestTable, &.{&source}, recorder.memTables.items);
                try testing.expectEqualSlices(*Recorder.TestTable, &.{}, recorder.diskTables.items);
                try testing.expectEqual(0, recorder.memTablesSem.permits);
                try testing.expectEqualDeep(Recorder.TestTable{}, source);
                try testing.expectEqualDeep(Recorder.TestTable{
                    .failCatalog = true,
                    .catalogWrites = @intFromBool(!failScheduling and err != error.OutOfMemory),
                }, destination);
                try testing.expect(recorder.mxTables.tryLock());
                recorder.mxTables.unlock(testing.io);

                if (err == error.OutOfMemory) return err;
                try testing.expectEqual(expectedErr, err);
                return;
            };
            return error.TestUnexpectedSuccess;
        }
    }.f;

    // Disk-merge scheduling fails before catalog publication.
    try checkAllAllocationFailures(testing.allocator, f, .{ .disk, true, error.SchedulingFailure });
    // Memory-merge scheduling fails before the source tables are replaced.
    try checkAllAllocationFailures(testing.allocator, f, .{ .mem, true, error.SchedulingFailure });
    // Disk-merge scheduling succeeds, then the destination rejects catalog publication.
    try checkAllAllocationFailures(testing.allocator, f, .{ .disk, false, error.CatalogFailure });
}

test "removeTables removes exact pointers" {
    const alloc = testing.allocator;
    var one: Recorder.TestTable = .{};
    var two: Recorder.TestTable = .{};
    var three: Recorder.TestTable = .{};

    const swapper = Swapper(Recorder, Recorder.TestTable);

    var tables = try std.ArrayList(*Recorder.TestTable).initCapacity(alloc, 3);
    defer tables.deinit(alloc);
    tables.appendAssumeCapacity(&one);
    tables.appendAssumeCapacity(&two);
    tables.appendAssumeCapacity(&three);

    var removeList = [_]*Recorder.TestTable{&two};
    const removed = swapper.removeTables(&tables, removeList[0..]);
    try testing.expectEqual(1, removed);
    try testing.expectEqualSlices(*Recorder.TestTable, &.{ &one, &three }, tables.items);
}

test "swapTables one-to-one memory swap does not access the semaphore" {
    const alloc = testing.allocator;

    const Worker = struct {
        recorder: *Recorder,
        source: *Recorder.TestTable,
        destination: *Recorder.TestTable,
        done: std.Io.Event = .unset,
        err: ?anyerror = null,

        fn run(self: *@This()) void {
            defer self.done.set(testing.io);
            var sources = [_]*Recorder.TestTable{self.source};
            Swapper(Recorder, Recorder.TestTable).swapTables(
                self.recorder,
                testing.io,
                alloc,
                &sources,
                self.destination,
                .mem,
            ) catch |err| {
                self.err = err;
            };
        }
    };

    var source: Recorder.TestTable = .{};
    var destination: Recorder.TestTable = .{};
    var recorder: Recorder = .{};
    defer recorder.memTables.deinit(alloc);
    defer recorder.diskTables.deinit(alloc);
    try recorder.memTables.ensureUnusedCapacity(alloc, 1);
    recorder.memTables.appendAssumeCapacity(&source);
    var worker: Worker = .{ .recorder = &recorder, .source = &source, .destination = &destination };

    // A one-to-one replacement inherits its slot even when another operation holds the semaphore mutex.
    recorder.memTablesSem.mutex.lockUncancelable(testing.io);
    const thread = std.Thread.spawn(.{}, Worker.run, .{&worker}) catch |err| {
        recorder.memTablesSem.mutex.unlock(testing.io);
        return err;
    };
    const completed: bool = blk: {
        // deadline fails if the calculation of semaphore posts is not precise
        const deadline = (std.Io.Timeout{ .duration = .{ .raw = .fromSeconds(1), .clock = .awake } }).toDeadline(testing.io);
        while (true) {
            worker.done.waitTimeout(testing.io, deadline) catch |err| switch (err) {
                error.Timeout => {
                    // Event waits may wake spuriously; keep the original deadline.
                    if (deadline.deadline.untilNow(testing.io).raw.nanoseconds < 0) continue;
                    break :blk false;
                },
                error.Canceled => break :blk false,
            };
            break :blk true;
        }
    };
    // Unblock the old implementation before joining, including on assertion failure.
    recorder.memTablesSem.mutex.unlock(testing.io);
    thread.join();

    try testing.expect(completed);
    if (worker.err) |err| return err;
    try testing.expectEqual(0, recorder.memTablesSem.permits);
    try testing.expectEqualSlices(*Recorder.TestTable, &.{&destination}, recorder.memTables.items);
    try testing.expectEqualDeep(Recorder.TestTable{
        .toRemove = .init(true),
        .releases = 1,
    }, source);
}

test "swapTables releases net memory permits" {
    const alloc = testing.allocator;
    const capacity = 16;

    const Case = struct { kind: TableKind, memSources: usize, diskSources: usize = 0 };
    const cases = [_]Case{
        .{ .kind = .mem, .memSources = 1 },
        .{ .kind = .mem, .memSources = 2 },
        .{ .kind = .mem, .memSources = capacity },
        .{ .kind = .disk, .memSources = 1 },
        .{ .kind = .disk, .memSources = capacity, .diskSources = 1 },
        .{ .kind = .disk, .memSources = 0, .diskSources = 1 },
        .{ .kind = .disk, .memSources = 0 },
    };
    for (cases) |case| {
        var memTables = [_]Recorder.TestTable{.{}} ** capacity;
        var diskTable: Recorder.TestTable = .{};
        var destination: Recorder.TestTable = .{};
        var memBuf: [capacity]*Recorder.TestTable = undefined;
        var diskBuf: [2]*Recorder.TestTable = undefined;
        var sourcesBuf: [capacity + 1]*Recorder.TestTable = undefined;
        var sources = std.ArrayList(*Recorder.TestTable).initBuffer(&sourcesBuf);
        var recorder: Recorder = .{
            .memTables = .initBuffer(&memBuf),
            .diskTables = .initBuffer(&diskBuf),
        };
        for (0..memTables[0..].len) |i| recorder.memTables.appendAssumeCapacity(&memTables[i]);
        for (0..case.memSources) |i| sources.appendAssumeCapacity(&memTables[i]);
        if (case.diskSources != 0) {
            recorder.diskTables.appendAssumeCapacity(&diskTable);
            sources.appendAssumeCapacity(&diskTable);
        }

        try Swapper(Recorder, Recorder.TestTable).swapTables(&recorder, testing.io, alloc, sources.items, &destination, case.kind);

        try testing.expect(recorder.memTablesSem.permits + recorder.memTables.items.len <= capacity);
        const memDestination: usize = @intFromBool(case.kind == .mem);
        const diskDestination: usize = @intFromBool(case.kind == .disk);
        const freed = case.memSources - memDestination;
        const State = struct {
            permits: usize,
            memTables: usize,
            diskTables: usize,
            memMerges: usize,
            diskMerges: usize,
        };
        try testing.expectEqualDeep(State{
            .permits = freed,
            .memTables = capacity - freed,
            .diskTables = diskDestination,
            .memMerges = memDestination,
            .diskMerges = diskDestination,
        }, State{
            .permits = recorder.memTablesSem.permits,
            .memTables = recorder.memTables.items.len,
            .diskTables = recorder.diskTables.items.len,
            .memMerges = recorder.memMerges,
            .diskMerges = recorder.diskMerges,
        });
        for (memTables, 0..) |table, i| {
            const removed = i < case.memSources;
            try testing.expectEqualDeep(Recorder.TestTable{
                .toRemove = .init(removed),
                .releases = @intFromBool(removed),
            }, table);
            try testing.expectEqual(!removed, std.mem.indexOfScalar(*Recorder.TestTable, recorder.memTables.items, &memTables[i]) != null);
        }
        try testing.expectEqualDeep(Recorder.TestTable{
            .toRemove = .init(case.diskSources != 0),
            .releases = case.diskSources,
        }, diskTable);
        try testing.expectEqualDeep(Recorder.TestTable{ .catalogWrites = diskDestination }, destination);
        const destinationTables = if (case.kind == .mem) recorder.memTables.items else recorder.diskTables.items;
        try testing.expect(std.mem.indexOfScalar(*Recorder.TestTable, destinationTables, &destination) != null);
    }
}
