const std = @import("std");
const Allocator = std.mem.Allocator;

const Swapper = @import("swap.zig").Swapper;
const TableKind = @import("merge.zig").TableKind;

const testing = std.testing;

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
        fn f(kind: TableKind, failScheduling: bool, failAllocation: bool) !void {
            const alloc = testing.allocator;

            var source: Recorder.TestTable = .{};
            var destination: Recorder.TestTable = .{ .failCatalog = true };

            var recorder: Recorder = .{ .failScheduling = failScheduling };
            defer recorder.memTables.deinit(alloc);
            defer recorder.diskTables.deinit(alloc);

            try recorder.memTables.append(alloc, &source);

            var sources = [_]*Recorder.TestTable{&source};
            var failing = testing.FailingAllocator.init(alloc, .{ .fail_index = 0 });

            const expectedErr = if (failAllocation) error.OutOfMemory else if (failScheduling) error.SchedulingFailure else error.CatalogFailure;

            try testing.expectError(expectedErr, Swapper(Recorder, Recorder.TestTable).swapTables(
                &recorder,
                testing.io,
                if (failAllocation) failing.allocator() else alloc,
                &sources,
                &destination,
                kind,
            ));
            try testing.expectEqualSlices(*Recorder.TestTable, &.{&source}, recorder.memTables.items);
            try testing.expectEqualSlices(*Recorder.TestTable, &.{}, recorder.diskTables.items);
            try testing.expectEqual(0, recorder.memTablesSem.permits);
            try testing.expectEqualDeep(Recorder.TestTable{}, source);
            try testing.expectEqualDeep(Recorder.TestTable{
                .failCatalog = true,
                .catalogWrites = @intFromBool(!failAllocation and !failScheduling),
            }, destination);
            try testing.expect(recorder.mxTables.tryLock());
            recorder.mxTables.unlock(testing.io);
        }
    }.f;

    try f(.disk, false, true);
    try f(.disk, true, false);
    try f(.mem, true, false);
    try f(.disk, false, false);
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

test "swapTables releases net memory permits" {
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
        for (0..capacity) |i| recorder.memTables.appendAssumeCapacity(&memTables[i]);
        for (0..case.memSources) |i| sources.appendAssumeCapacity(&memTables[i]);
        if (case.diskSources != 0) {
            recorder.diskTables.appendAssumeCapacity(&diskTable);
            sources.appendAssumeCapacity(&diskTable);
        }

        try Swapper(Recorder, Recorder.TestTable).swapTables(&recorder, testing.io, testing.allocator, sources.items, &destination, case.kind);

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
