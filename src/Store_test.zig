const std = @import("std");
const Allocator = std.mem.Allocator;
const Io = std.Io;
const Dir = Io.Dir;

const zeit = @import("zeit");

const Layout = @import("Layout.zig");

const Line = @import("store/lines.zig").Line;
const Field = @import("store/lines.zig").Field;
const freeFields = @import("store/lines.zig").freeFields;
const deinitLinesFull = @import("store/lines.zig").deinitLinesFull;

const Partition = @import("Partition.zig");
const filenames = @import("filenames.zig");
const Conf = @import("Conf.zig");
const Runtime = @import("Runtime.zig");

pub const Store = @import("Store.zig");

const testing = std.testing;

test "partitionKeyFormat" {
    const io = testing.io;
    const inst = try zeit.instant(io, .{ .source = .{ .time = .{
        .day = 1,
        .month = .jan,
        .year = 2026,
    } } });
    var key: [8]u8 = undefined;
    const now: u64 = @intCast(inst.timestamp);
    const day = now / std.time.ns_per_day;
    const keySlice = try Store.partitionKeyBuf(io, &key, @intCast(day));
    try testing.expectEqualStrings("01012026", key[0..]);
    try testing.expectEqualStrings("01012026", keySlice);
}

test "dayFromKey parses partition keys and roundtrips with partitionKeyBuf" {
    const io = testing.io;

    const Case = struct {
        key: []const u8,
        day: u5,
        month: zeit.Month,
        year: i32,
    };

    const cases = [_]Case{
        .{ .key = "01012026", .day = 1, .month = .jan, .year = 2026 },
        .{ .key = "29022024", .day = 29, .month = .feb, .year = 2024 },
        .{ .key = "31121999", .day = 31, .month = .dec, .year = 1999 },
    };

    for (cases) |case| {
        const parsedDay = try Store.dayFromKey(io, case.key);
        const inst = try zeit.instant(io, .{ .source = .{ .time = .{
            .day = case.day,
            .month = case.month,
            .year = case.year,
        } } });
        const expectedTs: u64 = @intCast(inst.timestamp);
        const expectedDay = expectedTs / std.time.ns_per_day;
        try testing.expectEqual(expectedDay, parsedDay);

        var keyBuf: [8]u8 = undefined;
        const keySlice = try Store.partitionKeyBuf(io, &keyBuf, parsedDay);
        try testing.expectEqualStrings(case.key, keySlice);
    }
}

test "init opens existing partitions, sorts them and sets lru, ignores files in partitions" {
    const alloc = testing.allocator;
    const io = testing.io;

    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    const rootPath = try tmp.dir.realPathFileAlloc(io, ".", alloc);
    defer alloc.free(rootPath);

    var storePathBuf: [std.fs.max_path_bytes]u8 = undefined;
    var storePathWriter = std.Io.Writer.fixed(&storePathBuf);
    try std.fs.path.fmtJoin(&.{ rootPath, "store" }).format(&storePathWriter);
    const storePath = storePathWriter.buffered();
    try Dir.createDirAbsolute(io, storePath, .default_dir);

    var partitionsPathBuf: [std.fs.max_path_bytes]u8 = undefined;
    var partitionsPathWriter = std.Io.Writer.fixed(&partitionsPathBuf);
    try std.fs.path.fmtJoin(&.{ storePath, filenames.partitions }).format(&partitionsPathWriter);
    const partitionsPath = partitionsPathWriter.buffered();
    try Dir.createDirAbsolute(io, partitionsPath, .default_dir);

    const keys = [_][]const u8{ "31121999", "01012026", "29022024" };
    for (keys) |key| {
        var partitionPathBuf: [std.fs.max_path_bytes]u8 = undefined;
        var partitionPathWriter = std.Io.Writer.fixed(&partitionPathBuf);
        try std.fs.path.fmtJoin(&.{ partitionsPath, key }).format(&partitionPathWriter);
        const partitionPath = partitionPathWriter.buffered();

        var indexPathBuf: [std.fs.max_path_bytes]u8 = undefined;
        var indexPathWriter = std.Io.Writer.fixed(&indexPathBuf);
        try std.fs.path.fmtJoin(&.{ partitionPath, filenames.indexTables }).format(&indexPathWriter);
        const indexPath = indexPathWriter.buffered();

        var dataPathBuf: [std.fs.max_path_bytes]u8 = undefined;
        var dataPathWriter = std.Io.Writer.fixed(&dataPathBuf);
        try std.fs.path.fmtJoin(&.{ partitionPath, filenames.dataTables }).format(&dataPathWriter);
        const dataPath = dataPathWriter.buffered();

        try Dir.createDirAbsolute(io, partitionPath, .default_dir);
        try Dir.createDirAbsolute(io, indexPath, .default_dir);
        try Dir.createDirAbsolute(io, dataPath, .default_dir);
    }
    // files is ignored in a partitions folder
    var dummyFilePathBuf: [std.fs.max_path_bytes]u8 = undefined;
    var dummyFileWriter: std.Io.Writer = .fixed(&dummyFilePathBuf);
    try std.fs.path.fmtJoin(&.{ partitionsPath, ".DS_Store" }).format(&dummyFileWriter);
    const dummyFile = try Dir.createFileAbsolute(io, dummyFileWriter.buffered(), .{});
    defer dummyFile.close(io);

    const conf = Conf.getConf();
    const runtime = try Runtime.init(io, alloc, storePath, conf.app.maxCachePortion);
    defer runtime.deinit(alloc);

    const layout = try Layout.make(io, storePath, &partitionsPathBuf);
    var store = try Store.init(io, alloc, &conf, runtime, layout);
    defer store.deinit(io, alloc);

    try testing.expectEqual(keys.len, store.partitions.items.len);

    const day0 = try Store.dayFromKey(io, "31121999");
    const day1 = try Store.dayFromKey(io, "29022024");
    const day2 = try Store.dayFromKey(io, "01012026");

    try testing.expectEqualSlices(u32, &.{ day0, day1, day2 }, &.{
        store.partitions.items[0].day,
        store.partitions.items[1].day,
        store.partitions.items[2].day,
    });

    const lru = store.lruPartition orelse return error.TestExpectedPartition;
    try testing.expectEqual(day2, lru.day);
}

test "selectPartitionsSlice selects correct range and handles gaps" {
    const Case = struct {
        minDay: u32,
        maxDay: u32,
        expectedDays: []const u64,
    };

    const check = struct {
        fn run(partitions: []const *Partition, cases: []const Case) !void {
            for (cases) |case| {
                const slice = Store.selectPartitionsSliceInRange(partitions, case.minDay, case.maxDay);
                try testing.expectEqual(case.expectedDays.len, slice.len);
                for (case.expectedDays, 0..) |day, i| {
                    try testing.expectEqual(day, slice[i].day);
                }
            }
        }
    }.run;
    const newPartition = struct {
        fn new(day: u32) Partition {
            const p = Partition{
                .alloc = undefined,
                .day = day,
                .path = "",
                .key = "",
                .index = undefined,
                .data = undefined,
                .streamCache = undefined,
            };
            return p;
        }
    }.new;

    // Empty list: any range returns nothing
    {
        const partitions = [_]*Partition{};
        try check(&partitions, &[_]Case{
            .{ .minDay = 0, .maxDay = 0, .expectedDays = &.{} },
            .{ .minDay = 0, .maxDay = 100, .expectedDays = &.{} },
            .{ .minDay = 5, .maxDay = 5, .expectedDays = &.{} },
        });
    }

    // Single partition at day 7
    {
        var p7 = newPartition(7);
        const partitions = [_]*Partition{&p7};
        try check(&partitions, &[_]Case{
            // Exact hit
            .{ .minDay = 7, .maxDay = 7, .expectedDays = &.{7} },
            // Range enclosing the partition
            .{ .minDay = 6, .maxDay = 8, .expectedDays = &.{7} },
            // Range entirely before the partition
            .{ .minDay = 0, .maxDay = 6, .expectedDays = &.{} },
            // Range entirely after the partition
            .{ .minDay = 8, .maxDay = 100, .expectedDays = &.{} },
        });
    }

    // Three sparse partitions with gaps at days 1, 3, 5
    {
        var p1 = newPartition(1);
        var p3 = newPartition(3);
        var p5 = newPartition(5);
        const partitions = [_]*Partition{ &p1, &p3, &p5 };
        try check(&partitions, &[_]Case{
            // Middle range: only day 3 falls in [2, 4]
            .{ .minDay = 2, .maxDay = 4, .expectedDays = &.{3} },
            // Full range: all partitions returned
            .{ .minDay = 0, .maxDay = 100, .expectedDays = &.{ 1, 3, 5 } },
            // Starts before first partition
            .{ .minDay = 0, .maxDay = 3, .expectedDays = &.{ 1, 3 } },
            // Ends after last partition
            .{ .minDay = 3, .maxDay = 100, .expectedDays = &.{ 3, 5 } },
            // Exact boundary match on both ends
            .{ .minDay = 1, .maxDay = 5, .expectedDays = &.{ 1, 3, 5 } },
            // Single partition: first
            .{ .minDay = 1, .maxDay = 2, .expectedDays = &.{1} },
            // Single partition: middle (minDay == maxDay == partition day)
            .{ .minDay = 3, .maxDay = 3, .expectedDays = &.{3} },
            // Single partition: last
            .{ .minDay = 4, .maxDay = 5, .expectedDays = &.{5} },
            // minDay == maxDay at exact first/last partition day
            .{ .minDay = 1, .maxDay = 1, .expectedDays = &.{1} },
            .{ .minDay = 5, .maxDay = 5, .expectedDays = &.{5} },
            // No match: range beyond last partition
            .{ .minDay = 6, .maxDay = 100, .expectedDays = &.{} },
            // No match: range before first partition
            .{ .minDay = 0, .maxDay = 0, .expectedDays = &.{} },
            // No match: range falls entirely in gap between 1 and 3
            .{ .minDay = 2, .maxDay = 2, .expectedDays = &.{} },
            // No match: range falls entirely in gap between 3 and 5
            .{ .minDay = 4, .maxDay = 4, .expectedDays = &.{} },
        });
    }

    // Five consecutive partitions at days 10–14
    {
        var p10 = newPartition(10);
        var p11 = newPartition(11);
        var p12 = newPartition(12);
        var p13 = newPartition(13);
        var p14 = newPartition(14);
        const partitions = [_]*Partition{ &p10, &p11, &p12, &p13, &p14 };
        try check(&partitions, &[_]Case{
            // Full range
            .{ .minDay = 10, .maxDay = 14, .expectedDays = &.{ 10, 11, 12, 13, 14 } },
            // Range wider than the partition set
            .{ .minDay = 9, .maxDay = 15, .expectedDays = &.{ 10, 11, 12, 13, 14 } },
            // Interior sub-range (no boundary touch)
            .{ .minDay = 11, .maxDay = 13, .expectedDays = &.{ 11, 12, 13 } },
            // Single day in the middle
            .{ .minDay = 12, .maxDay = 12, .expectedDays = &.{12} },
            // Overlap only at the start
            .{ .minDay = 0, .maxDay = 11, .expectedDays = &.{ 10, 11 } },
            // Overlap only at the end
            .{ .minDay = 13, .maxDay = 100, .expectedDays = &.{ 13, 14 } },
            // Before all
            .{ .minDay = 0, .maxDay = 9, .expectedDays = &.{} },
            // After all
            .{ .minDay = 15, .maxDay = 100, .expectedDays = &.{} },
        });
    }
}

test "keepLatestLines sorts by timestamp and trims old rows" {
    const alloc = testing.allocator;

    const makeLine = struct {
        fn run(allocator: Allocator, timestampNs: u64, value: []const u8) !Line {
            const fields = try allocator.alloc(Field, 1);
            errdefer allocator.free(fields);
            const key = try allocator.dupe(u8, "id");
            errdefer allocator.free(key);
            const copiedValue = try allocator.dupe(u8, value);
            fields[0] = .{ .key = key, .value = copiedValue };
            return .{ .timestampNs = timestampNs, .fields = fields };
        }
    }.run;
    const appendLine = struct {
        fn run(allocator: Allocator, dst: *std.ArrayList(Line), timestampNs: u64, value: []const u8) !void {
            const line = try makeLine(allocator, timestampNs, value);
            errdefer freeFields(allocator, line.fields);
            try dst.append(allocator, line);
        }
    }.run;

    var lines = std.ArrayList(Line).empty;
    defer deinitLinesFull(alloc, &lines);

    try appendLine(alloc, &lines, 10, "oldest");
    try appendLine(alloc, &lines, 50, "newest");
    try appendLine(alloc, &lines, 20, "old");
    try appendLine(alloc, &lines, 40, "newer");
    try appendLine(alloc, &lines, 30, "middle");

    Store.keepLatestLines(alloc, &lines, 3);

    try testing.expectEqual(3, lines.items.len);
    try testing.expectEqual(50, lines.items[0].timestampNs);
    try testing.expectEqual(40, lines.items[1].timestampNs);
    try testing.expectEqual(30, lines.items[2].timestampNs);
    try testing.expectEqualStrings("newest", lines.items[0].fields[0].value);
    try testing.expectEqualStrings("newer", lines.items[1].fields[0].value);
    try testing.expectEqualStrings("middle", lines.items[2].fields[0].value);
}

test "getPartition reuses partition, updates lru, deinit closes partitions and recorders" {
    const alloc = testing.allocator;
    const io = testing.io;

    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    const rootPath = try tmp.dir.realPathFileAlloc(io, ".", alloc);
    defer alloc.free(rootPath);

    var partitionsRootBuf: [std.fs.max_path_bytes]u8 = undefined;
    var partitionsRootWriter = std.Io.Writer.fixed(&partitionsRootBuf);
    try std.fs.path.fmtJoin(&.{ rootPath, filenames.partitions }).format(&partitionsRootWriter);
    const partitionsRoot = partitionsRootWriter.buffered();
    try Dir.createDirAbsolute(io, partitionsRoot, .default_dir);

    const conf = Conf.getConf();
    const runtime = try Runtime.init(io, alloc, rootPath, conf.app.maxCachePortion);
    defer runtime.deinit(alloc);

    var partitionsPathBuf: [std.fs.max_path_bytes]u8 = undefined;
    const layout = try Layout.make(io, rootPath, &partitionsPathBuf);
    var store = try Store.init(io, alloc, &conf, runtime, layout);
    defer store.deinit(io, alloc);

    const dayOne: u64 = 10;
    const dayTwo: u64 = 11;

    try testing.expectEqual(0, store.partitions.items.len);

    const first = try store.getPartition(io, alloc, dayOne);
    defer first.release(io);
    try testing.expectEqual(1, store.partitions.items.len);
    try testing.expectEqual(first, store.partitions.items[0]);
    try testing.expectEqual(first, store.lruPartition.?);

    const firstAgain = try store.getPartition(io, alloc, dayOne);
    defer firstAgain.release(io);
    try testing.expectEqual(first, firstAgain);
    try testing.expectEqual(1, store.partitions.items.len);
    try testing.expectEqual(first, store.lruPartition.?);

    const second = try store.getPartition(io, alloc, dayTwo);
    defer second.release(io);
    try testing.expectEqual(2, store.partitions.items.len);
    try testing.expectEqual(second, store.partitions.items[1]);
    try testing.expectEqual(second, store.lruPartition.?);
}
