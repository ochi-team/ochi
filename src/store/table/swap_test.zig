const std = @import("std");
const Allocator = std.mem.Allocator;

const DecompressionPool = @import("../compression/DecompressionPool.zig");

const Swapper = @import("swap.zig").Swapper;

const testing = std.testing;

const Table = @import("../index/Table.zig");
const MemTable = @import("../index/MemTable.zig");
const IndexRecorder = @import("../index/IndexRecorder.zig");

fn createSizedMemTable(alloc: Allocator, decompressionPool: *DecompressionPool, size: usize) !*Table {
    const memTable = try MemTable.empty(alloc);
    errdefer memTable.deinit(alloc);

    try memTable.entriesBuf.resize(alloc, size);

    return Table.fromMem(testing.io, alloc, memTable, decompressionPool);
}

test "removeTables removes exact pointers" {
    const alloc = testing.allocator;
    const io = testing.io;
    const decompressionPool = try DecompressionPool.init(alloc, 1);
    defer decompressionPool.deinit(alloc);

    const one = try createSizedMemTable(alloc, decompressionPool, 100);
    defer one.close(io);

    const two = try createSizedMemTable(alloc, decompressionPool, 100);
    defer two.close(io);

    const three = try createSizedMemTable(alloc, decompressionPool, 100);
    defer three.close(io);

    const swapper = Swapper(IndexRecorder, Table);

    var tables = try std.ArrayList(*Table).initCapacity(alloc, 3);
    defer tables.deinit(alloc);
    tables.appendAssumeCapacity(one);
    tables.appendAssumeCapacity(two);
    tables.appendAssumeCapacity(three);

    var removeList = [_]*Table{two};
    const removed = swapper.removeTables(&tables, removeList[0..]);
    try testing.expectEqual(@as(u32, 1), removed);
    try testing.expectEqual(@as(usize, 2), tables.items.len);
    try testing.expect(tables.items[0] != two);
    try testing.expect(tables.items[1] != two);
}

test "swapTables transfers memory permits without transient excess or reacquiring" {
    const capacity = 16;
    const Recorder = struct {
        const Self = @This();
        const TestTable = struct {
            toRemove: std.atomic.Value(bool) = .init(false),
            refs: usize = 2,
            releases: usize = 0,
            catalogWrites: usize = 0,

            pub fn release(self: *@This(), _: std.Io) void {
                self.refs -= 1;
                self.releases += 1;
            }

            pub fn writeNames(_: std.Io, _: Allocator, _: []const u8, tables: []*@This()) !void {
                for (tables) |table| table.catalogWrites += 1;
            }
        };
        const Semaphore = struct {
            const Publication = struct { permits: usize, live: usize };

            permits: usize = 0,
            producers: usize = 0,
            posts: usize = 0,
            waits: usize = 0,
            blockedWaits: usize = 0,
            publications: [capacity]Publication = undefined,

            pub fn post(self: *@This(), _: std.Io) void {
                const recorder: *Self = @fieldParentPtr("memTablesSem", self);
                self.permits += 1;
                self.publications[self.posts] = .{
                    .permits = self.permits,
                    .live = recorder.memTables.items.len + self.producers,
                };
                self.posts += 1;

                // Let producers claim every posted permit before the swapper can wait.
                self.producers += self.permits;
                self.permits = 0;
            }

            pub fn waitUncancelable(self: *@This(), _: std.Io) void {
                self.waits += 1;
                if (self.permits == 0) {
                    self.blockedWaits += 1;
                } else {
                    self.permits -= 1;
                }
            }
        };

        mxTables: std.Io.Mutex = .init,
        memTables: std.ArrayList(*TestTable),
        diskTables: std.ArrayList(*TestTable),
        memTablesSem: Semaphore = .{},
        path: []const u8 = "",
        memMerges: usize = 0,
        diskMerges: usize = 0,

        pub fn startMemTablesMerge(self: *Self, _: std.Io, _: Allocator) !void {
            self.memMerges += 1;
        }

        pub fn startDiskTablesMerge(self: *Self, _: std.Io, _: Allocator) !void {
            self.diskMerges += 1;
        }
    };
    const Case = struct { kind: @import("merge.zig").TableKind, memSources: usize, diskSources: usize = 0 };
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
        var destination: Recorder.TestTable = .{ .refs = 1 };
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

        for (recorder.memTablesSem.publications[0..recorder.memTablesSem.posts]) |publication| {
            try testing.expect(publication.permits + publication.live <= capacity);
        }
        const memDestination: usize = @intFromBool(case.kind == .mem);
        const diskDestination: usize = @intFromBool(case.kind == .disk);
        const freed = case.memSources - memDestination;
        const State = struct {
            permits: usize,
            producers: usize,
            posts: usize,
            waits: usize,
            blockedWaits: usize,
            memTables: usize,
            diskTables: usize,
            memMerges: usize,
            diskMerges: usize,
        };
        try testing.expectEqualDeep(State{
            .permits = 0,
            .producers = freed,
            .posts = freed,
            .waits = 0,
            .blockedWaits = 0,
            .memTables = capacity - freed,
            .diskTables = diskDestination,
            .memMerges = memDestination,
            .diskMerges = diskDestination,
        }, State{
            .permits = recorder.memTablesSem.permits,
            .producers = recorder.memTablesSem.producers,
            .posts = recorder.memTablesSem.posts,
            .waits = recorder.memTablesSem.waits,
            .blockedWaits = recorder.memTablesSem.blockedWaits,
            .memTables = recorder.memTables.items.len,
            .diskTables = recorder.diskTables.items.len,
            .memMerges = recorder.memMerges,
            .diskMerges = recorder.diskMerges,
        });
        for (memTables, 0..) |table, i| {
            const removed = i < case.memSources;
            try testing.expectEqualDeep(Recorder.TestTable{
                .toRemove = .init(removed),
                .refs = if (removed) 1 else 2,
                .releases = @intFromBool(removed),
            }, table);
            try testing.expectEqual(!removed, std.mem.indexOfScalar(*Recorder.TestTable, recorder.memTables.items, &memTables[i]) != null);
        }
        try testing.expectEqualDeep(Recorder.TestTable{
            .toRemove = .init(case.diskSources != 0),
            .refs = if (case.diskSources != 0) 1 else 2,
            .releases = case.diskSources,
        }, diskTable);
        try testing.expectEqualDeep(Recorder.TestTable{ .refs = 1, .catalogWrites = diskDestination }, destination);
        const destinationTables = if (case.kind == .mem) recorder.memTables.items else recorder.diskTables.items;
        try testing.expect(std.mem.indexOfScalar(*Recorder.TestTable, destinationTables, &destination) != null);
    }
}
