const std = @import("std");
const Allocator = std.mem.Allocator;
const Io = std.Io;

const Table = @import("../index/Table.zig");
const MemTable = @import("../index/MemTable.zig");
const CompressionPool = @import("../compression/CompressionPool.zig");
const DecompressionPool = @import("../compression/DecompressionPool.zig");

const Merger = @import("merge.zig").Merger;
const TableKind = @import("merge.zig").TableKind;
const MergeWindowBound = @import("merge.zig").MergeWindowBound;

const testing = std.testing;
const MemBlock = @import("../index/MemBlock.zig");

test "selectTablesToMerge moves selected window to the beginning and returns edge" {
    const alloc = testing.allocator;
    const io = testing.io;
    const decompressionPool = try DecompressionPool.init(alloc, 1);
    defer decompressionPool.deinit(alloc);

    const Case = struct {
        sizes: []const u16,
        bound: MergeWindowBound,
        expected: []const u16,
        expectedLeft: []const u16,
    };

    const cases = [_]Case{
        .{
            .sizes = &.{ 47, 55, 65, 76, 107, 108, 111, 117, 124, 131, 133, 162, 164, 187 },
            .bound = .{ .lower = 0, .upper = 13 },
            .expected = &.{ 47, 55, 65, 76, 107, 108, 111, 117, 124, 131, 133, 162, 164 },
            .expectedLeft = &.{187},
        },
        .{
            .sizes = &.{ 15, 43, 51, 69, 85, 89, 89, 124, 154, 164, 168, 176, 185, 194 },
            .bound = .{ .lower = 0, .upper = 14 },
            .expected = &.{ 15, 43, 51, 69, 85, 89, 89, 124, 154, 164, 168, 176, 185, 194 },
            .expectedLeft = &.{},
        },
        .{
            .sizes = &.{ 12, 37, 40, 84, 90, 93, 101, 106, 135, 146, 155, 159, 171, 171 },
            .bound = .{ .lower = 1, .upper = 14 },
            .expected = &.{ 37, 40, 84, 90, 93, 101, 106, 135, 146, 155, 159, 171, 171 },
            .expectedLeft = &.{12},
        },
        .{
            .sizes = &.{ 1, 67, 92, 101, 104, 105, 116, 123, 132, 136, 139, 171, 189 },
            .bound = .{ .lower = 1, .upper = 11 },
            .expected = &.{ 67, 92, 101, 104, 105, 116, 123, 132, 136, 139 },
            .expectedLeft = &.{ 1, 171, 189 },
        },
        .{
            .sizes = &.{ 4, 20, 26, 56, 86, 97, 98, 118, 119, 122, 122, 135, 142, 168, 219, 222, 229, 231, 236, 248 },
            .bound = .{ .lower = 4, .upper = 20 },
            .expected = &.{ 86, 97, 98, 118, 119, 122, 122, 135, 142, 168, 219, 222, 229, 231, 236, 248 },
            .expectedLeft = &.{ 4, 20, 26, 56 },
        },
    };

    for (cases) |case| {
        var tables = try std.ArrayList(*Table).initCapacity(alloc, case.sizes.len);
        defer {
            for (tables.items) |table| table.close(io);
            tables.deinit(alloc);
        }

        for (case.sizes) |size| {
            const table = try MemTable.empty(alloc);
            try table.entriesBuf.resize(alloc, size);
            const t = try Table.fromMem(io, alloc, table, decompressionPool);
            tables.appendAssumeCapacity(t);
        }

        const merger = Merger(*Table, 16, 16);
        const edge = merger.selectTablesToMerge(tables.items);
        try testing.expectEqual(case.bound.upper - case.bound.lower, edge);
        var actual = try alloc.alloc(u16, edge);
        defer alloc.free(actual);
        for (0..edge) |i| {
            actual[i] = @intCast(tables.items[i].size);
        }
        try testing.expectEqualSlices(u16, case.expected, actual);

        const leftLen = tables.items.len - edge;
        var left = try alloc.alloc(u16, leftLen);
        defer alloc.free(left);
        for (0..leftLen) |i| {
            left[i] = @intCast(tables.items[edge + i].size);
        }
        try testing.expectEqualSlices(u16, case.expectedLeft, left);
    }
}

fn createSizedMemTable(alloc: Allocator, decompressionPool: *DecompressionPool, size: usize) !*Table {
    const memTable = try MemTable.empty(alloc);
    try memTable.entriesBuf.resize(alloc, size);
    return Table.fromMem(testing.io, alloc, memTable, decompressionPool);
}

test "filterTablesToMerge marks only selected tables inMerge" {
    const alloc = testing.allocator;
    const io = testing.io;
    const decompressionPool = try DecompressionPool.init(alloc, 1);
    defer decompressionPool.deinit(alloc);

    const sizes = [_]u16{ 47, 55, 65, 76, 107, 108, 111, 117, 124, 131, 133, 162, 164, 187 };
    var tables = try std.ArrayList(*Table).initCapacity(alloc, sizes.len);
    defer {
        for (tables.items) |table| table.close(io);
        tables.deinit(alloc);
    }
    for (sizes) |size| {
        const table = try createSizedMemTable(alloc, decompressionPool, size);
        tables.appendAssumeCapacity(table);
    }

    const merger = Merger(*Table, 16, 16);
    var buf: [16]*Table = undefined;
    const window = merger.filterTablesToMerge(tables.items, &buf, std.math.maxInt(u64));
    try testing.expect(window != null);
    const w = window.?;

    for (tables.items) |table| {
        const expected = std.mem.indexOfScalar(*Table, w, table) != null;
        try testing.expectEqual(expected, table.inMerge);
    }
}

test "filterTablesToMerge scans beyond full bounded destination" {
    const alloc = testing.allocator;
    const io = testing.io;
    const decompressionPool = try DecompressionPool.init(alloc, 1);
    defer decompressionPool.deinit(alloc);

    const maxTablesToMerge = 16;
    const merger = Merger(*Table, 16, maxTablesToMerge);

    var tables = try std.ArrayList(*Table).initCapacity(alloc, maxTablesToMerge * 2);
    defer {
        for (tables.items) |table| table.close(io);
        tables.deinit(alloc);
    }
    for (0..maxTablesToMerge * 2) |i| {
        const size: usize = if (i < maxTablesToMerge) 6000 else 100;
        const table = try createSizedMemTable(alloc, decompressionPool, size);
        tables.appendAssumeCapacity(table);
    }

    var toMergeBuf: [maxTablesToMerge]*Table = undefined;
    const selected = merger.filterTablesToMerge(tables.items, &toMergeBuf, 10_000);
    try testing.expect(selected != null);
    try testing.expectEqual(maxTablesToMerge, selected.?.len);
    for (selected.?) |table| {
        try testing.expectEqual(100, table.size);
        try testing.expect(table.inMerge);
    }
}

test "filterLeveledTables returns null when size filter removes candidates" {
    const alloc = testing.allocator;
    const io = testing.io;
    const decompressionPool = try DecompressionPool.init(alloc, 1);
    defer decompressionPool.deinit(alloc);

    const Case = struct {
        sizes: []const u16,
    };

    const cases = [_]Case{
        .{
            .sizes = &.{ 6000, 6000, 6000, 6000, 6000, 6000, 6000, 6000, 6000, 6000, 6000, 6000, 6000, 6000, 6000, 6000 },
        },
        .{
            .sizes = &.{ 6000, 6000, 6000, 6000, 6000, 6000, 6000, 6000, 6000, 6000, 6000, 6000, 6000, 6000, 6000, 100 },
        },
    };

    for (cases) |case| {
        var tables = try std.ArrayList(*Table).initCapacity(alloc, case.sizes.len);
        defer {
            for (tables.items) |table| table.close(io);
            tables.deinit(alloc);
        }
        for (case.sizes) |size| {
            const table = try createSizedMemTable(alloc, decompressionPool, size);
            tables.appendAssumeCapacity(table);
        }

        const merger = Merger(*Table, 16, 16);
        var buf: [16]*Table = undefined;
        const window = merger.filterTablesToMerge(tables.items, &buf, 10_000);
        try testing.expectEqual(null, window);
    }
}

fn createDiskTableFromItems(
    io: Io,
    alloc: Allocator,
    tablePath: []const u8,
    items: []const []const u8,
    compressionPool: *CompressionPool,
) !*Table {
    const decompressionPool = try DecompressionPool.init(alloc, 1);
    defer decompressionPool.deinit(alloc);

    const memTable = try createMemTableFromItems(io, alloc, items, compressionPool);
    defer memTable.close(io);
    const mem = memTable.inner.mem;
    try mem.storeToDisk(io, tablePath);
    return Table.open(io, alloc, tablePath, decompressionPool);
}

fn createMemTableFromItems(io: Io, alloc: Allocator, items: []const []const u8, compressionPool: *CompressionPool) !*Table {
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

test "getDestinationTableKind rules" {
    const alloc = testing.allocator;
    const io = testing.io;
    const compressionPool = try CompressionPool.init(alloc, 1);
    defer compressionPool.deinit(alloc);
    const decompressionPool = try DecompressionPool.init(alloc, 1);
    defer decompressionPool.deinit(alloc);

    const small1 = try createSizedMemTable(alloc, decompressionPool, 256);
    defer small1.close(io);
    const small2 = try createSizedMemTable(alloc, decompressionPool, 512);
    defer small2.close(io);

    var bothSmall = [_]*Table{ small1, small2 };
    const merger = Merger(*Table, 16, 16);
    const maxInmemoryTableSize = merger.getMaxInmemoryTableSize(1024 * 1024 * 1024);

    try testing.expectEqual(TableKind.mem, merger.getDestinationTableKind(bothSmall[0..], false, maxInmemoryTableSize));
    try testing.expectEqual(TableKind.disk, merger.getDestinationTableKind(bothSmall[0..], true, maxInmemoryTableSize));

    const large = try createSizedMemTable(alloc, decompressionPool, @intCast(maxInmemoryTableSize + 1));
    defer large.close(io);
    var onlyLarge = [_]*Table{large};
    try testing.expectEqual(TableKind.disk, merger.getDestinationTableKind(onlyLarge[0..], false, maxInmemoryTableSize));

    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    const rootPath = try tmp.dir.realPathFileAlloc(io, ".", alloc);
    defer alloc.free(rootPath);
    const diskPath = try std.fs.path.join(alloc, &.{ rootPath, "disk-tbl" });
    errdefer alloc.free(diskPath);
    const disk = try createDiskTableFromItems(io, alloc, diskPath, &.{ "a", "b", "c" }, compressionPool);
    defer disk.close(io);
    var mixed = [_]*Table{ small1, disk };
    try testing.expectEqual(TableKind.disk, merger.getDestinationTableKind(mixed[0..], false, maxInmemoryTableSize));
}

// TODO: do fuzz with bound.upper - bound.lower == (j + i) - j == i validation
// to test the output limits
