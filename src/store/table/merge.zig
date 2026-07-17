const std = @import("std");
const Allocator = std.mem.Allocator;
const Io = std.Io;

const Table = @import("../index/Table.zig");
const MemTable = @import("../index/MemTable.zig");
const CompressionPool = @import("../compression/CompressionPool.zig");
const DecompressionPool = @import("../compression/DecompressionPool.zig");

// avoid merges where one big part is rewritten with tiny additions (leads to high write amplification)
// guess based number, might be changed on the practical data
const mergeMultiple = 2;

// 4mb is a minimal size for mem table,
// technically it makes minimum requirement as 1GB for the software
pub const minMemTableSize: u64 = 4 * 1024 * 1024;

pub const TableKind = enum {
    mem,
    disk,
};

const MergeWindowBound = struct {
    upper: usize,
    lower: usize,
};

pub fn Merger(
    comptime T: type,
    maxMemTables: comptime_int,
    maxTablesToMerge: comptime_int,
) type {
    comptime {
        if (maxTablesToMerge < 2) @compileError("maxTablesToMerge must be >= 2");
    }

    return struct {
        /// assumes toMerge destination has preallocated capacity
        pub fn filterTablesToMerge(
            tables: []T,
            dst: *std.ArrayList(T),
            maxDiskTableSize: u64,
        ) ?MergeWindowBound {
            sortToMerge(tables);
            for (tables) |table| {
                if (!table.inMerge) {
                    dst.appendBounded(table) catch {
                        // tables slice is larger then destination array
                        break;
                    };
                }
            }

            // tablesToMerge is a slice of toMerge ArrayList, no need to free it
            const window = filterLeveledTables(dst, maxDiskTableSize);
            if (window) |w| {
                const tablesToMerge = dst.items[w.lower..w.upper];
                for (tablesToMerge) |table| {
                    std.debug.assert(!table.inMerge);
                    table.inMerge = true;
                }
            }

            return window;
        }

        pub fn selectTablesToMerge(
            tables: *std.ArrayList(T),
        ) usize {
            if (tables.items.len < 2) return tables.items.len;

            sortToMerge(tables.items);
            const maybeWindow = filterLeveledTables(tables, std.math.maxInt(u64));
            const w = maybeWindow orelse return tables.items.len;
            if (w.lower > 0) {
                std.mem.reverse(T, tables.items[0..w.lower]);
                std.mem.reverse(T, tables.items[w.lower..]);
                std.mem.reverse(T, tables.items);
            }

            // sort the leftovers
            const edge = w.upper - w.lower;
            std.debug.assert(edge != 0);
            if (edge < tables.items.len) {
                sortToMerge(tables.items[edge..]);
            }

            return edge;
        }

        // TODO: we probably might define few levels of tables and
        // split the for compaction accordingly
        // TODO: try designing a destination in a way that skips merging multiple mem tables to a larger one
        // in order to reduce unncecessary load,
        // we can skip a merge and wait a bit to flush them all together immediately to disk
        pub fn getDestinationTableKind(tables: []T, force: bool, maxInmemoryTableSize: u64) TableKind {
            if (force) return .disk;

            const size = getTablesSize(tables);
            if (size > maxInmemoryTableSize) return .disk;
            if (!areTablesMem(tables)) return .disk;

            return .mem;
        }

        pub fn getTablesSize(tables: []T) u64 {
            var n: u64 = 0;
            for (tables) |table| {
                n += table.size;
            }
            return n;
        }

        // only 10% of cache available for mem index
        // TODO: experiment with tuning cache size to 5%, 15%
        pub fn getMaxInmemoryTableSize(cacheSize: u64) u64 {
            const maxmem = (cacheSize / 10) / maxMemTables;
            return @max(maxmem, minMemTableSize);
        }

        fn areTablesMem(tables: []T) bool {
            for (tables) |table| {
                if (table.inner == .mem) {
                    continue;
                } else {
                    return false;
                }
            }

            return true;
        }

        const tablePageCacheSize = 8 * 1024 * 1024;
        // TODO: move it to config instead of computed property
        // TODO: ideally we move the division per table to availble mem calculation side,
        // to make the operation rarely happen and keep the calculated value ready
        // TODO: we must experiment with different min sizes like 4 and 2 mb
        pub fn maxCachableTableSize(maxMem: u64, cacheSize: u64) u64 {
            const restMem = maxMem - cacheSize;
            // 8mb min page cache size
            // TODO: better to make it configurable
            const freePerTable = @max(restMem / maxTablesToMerge, tablePageCacheSize);
            return freePerTable;
        }

        // TODO: test implementation with greedier merge, if there are many small times are merging into one
        // we should evaluate whether the resulted bigger one could be merged with another larger table
        fn filterLeveledTables(
            toMerge: *std.ArrayList(T),
            maxDiskTableSize: u64,
        ) ?MergeWindowBound {
            if (toMerge.items.len < 2) return null;

            // TODO: concern is passing max int for mem tables might be not the most reliable option,
            // we must pass comptime flag whether it's a mem table / force flag to skip some of the tables to merge
            const maxSize = maxDiskTableSize / mergeMultiple;
            var idx: usize = 0;
            while (idx < toMerge.items.len) {
                const tableSize: u64 = @intCast(toMerge.items[idx].size);
                if (tableSize > maxSize) {
                    _ = toMerge.swapRemove(idx);
                    continue;
                }
                idx += 1;
            }
            if (toMerge.items.len < 2) return null;

            // we want to merge at least a half of them
            const upperBound = @min(maxTablesToMerge, toMerge.items.len);
            const lowerBound = @max(2, (upperBound + 1) / 2);
            var maxScore: f64 = 0;
            var windowToMerge: ?MergeWindowBound = null;

            // +1 to make upperBound inclusive
            for (lowerBound..upperBound + 1) |i| {
                for (0..toMerge.items.len - i + 1) |j| {
                    const bound = MergeWindowBound{ .lower = j, .upper = j + i };
                    const mergeWindow = toMerge.items[bound.lower..bound.upper];

                    // last item is the largerst, we expect them sorted by size in sortToMerge
                    const largestTableSize: u64 = @intCast(mergeWindow[mergeWindow.len - 1].size);
                    const firstTableSize: u64 = @intCast(mergeWindow[0].size);
                    if (firstTableSize * mergeWindow.len < largestTableSize) {
                        // too much of a difference, it's not a balanced merge, unncecessary write
                        continue;
                    }

                    var resultSize: u64 = 0;
                    for (mergeWindow) |table| resultSize += @intCast(table.size);
                    // further iterations bring only bigger tables,
                    // but we alraedy hit the disk limit
                    if (resultSize > maxDiskTableSize) break;

                    const score: f64 = @as(f64, @floatFromInt(resultSize)) / @as(f64, @floatFromInt(largestTableSize));
                    if (score < maxScore) continue;

                    maxScore = score;
                    windowToMerge = bound;
                }
            }

            const minScore: f64 = @max(@as(f64, @floatFromInt(maxTablesToMerge)) / 2, 2, mergeMultiple);
            if (maxScore < minScore) {
                // nothing to merge
                return null;
            }

            return windowToMerge;
        }

        const ownerType = switch (@typeInfo(T)) {
            .pointer => |ptr_info| ptr_info.child,
            .@"struct" => T,
            else => @compileError(std.fmt.comptimePrint(
                "{s} must be a struct or a pointer to a struct",
                .{
                    @typeName(T),
                },
            )),
        };

        const lessThanFn = @field(ownerType, "lessThan");
        fn sortToMerge(toMerge: []T) void {
            std.sort.pdq(T, toMerge, {}, lessThanFn);
        }
    };
}

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
        const edge = merger.selectTablesToMerge(&tables);
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

    var toMerge = try std.ArrayList(*Table).initCapacity(alloc, tables.items.len);
    defer toMerge.deinit(alloc);

    const merger = Merger(*Table, 16, 16);
    const window = merger.filterTablesToMerge(tables.items, &toMerge, std.math.maxInt(u64));
    try testing.expect(window != null);
    const w = window.?;

    for (toMerge.items, 0..) |table, i| {
        const expected = i >= w.lower and i < w.upper;
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
    var toMerge = std.ArrayList(*Table).initBuffer(&toMergeBuf);

    const window = merger.filterTablesToMerge(tables.items, &toMerge, 10_000);
    try testing.expect(window != null);
    const selected = toMerge.items[window.?.lower..window.?.upper];
    try testing.expectEqual(maxTablesToMerge, selected.len);
    for (selected) |table| {
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

        var toMerge = try std.ArrayList(*Table).initCapacity(alloc, tables.items.len);
        defer toMerge.deinit(alloc);

        const merger = Merger(*Table, 16, 16);
        const window = merger.filterTablesToMerge(tables.items, &toMerge, 10_000);
        try testing.expectEqual(null, window);
    }
}

fn createDiskTableFromItems(io: Io, alloc: Allocator, tablePath: []const u8, items: []const []const u8, compressionPool: *CompressionPool) !*Table {
    const decompressionPool = try DecompressionPool.init(alloc, 1);
    defer decompressionPool.deinit(alloc);

    const memTable = try createMemTableFromItems(io, alloc, items, compressionPool);
    defer memTable.close(io);
    const mem = memTable.inner.mem;
    try mem.storeToDisk(io, alloc, tablePath);
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
