const std = @import("std");
const Allocator = std.mem.Allocator;

const Contract = @import("../../stds/Contract.zig");

const Table = @import("../index/Table.zig");
const MemTable = @import("../index/MemTable.zig");

// avoid merges where one big part is rewritten with tiny additions (leads to high write amplification)
// guess based number, might be changed on the practical data
const mergeMultiple = 2;

// we need to balance throughput and memory limits
// this number is just a guess
const amountOfTablesToMerge = 16;

pub fn TableContract(T: type) Contract {
    return Contract{
        .fields = &.{
            .{ .name = "size", .type = u64 },
            .{ .name = "inMerge", .type = bool },
        },
        .funcs = &.{
            .{ .name = "lessThan", .type = fn (void, T, T) bool },
        },
    };
}

const MergeWindowBound = struct {
    upper: usize,
    lower: usize,
};

pub fn Merger(
    comptime T: type,
) type {
    const contract = TableContract(T);
    comptime {
        contract.satisfies(T, true) catch |err| {
            @compileError("TableContract is not satisfied by " ++ @typeName(T) ++ ": " ++ @errorName(err));
        };
    }
    const lessThanFnType = contract.funcs[0].type;

    return struct {
        pub fn filterTablesToMerge(
            alloc: Allocator,
            tables: []T,
            toMerge: *std.ArrayList(T),
            maxDiskTableSize: u64,
        ) Allocator.Error!?MergeWindowBound {
            try toMerge.ensureUnusedCapacity(alloc, tables.len);

            for (tables) |table| {
                if (!table.inMerge) {
                    toMerge.appendAssumeCapacity(table);
                }
            }

            // tablesToMerge is a slice of toMerge ArrayList, no need to free it
            const window = filterLeveledTables(toMerge, maxDiskTableSize, amountOfTablesToMerge);
            if (window) |w| {
                const tablesToMerge = toMerge.items[w.lower..w.upper];
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

            const maybeWindow = filterLeveledTables(tables, std.math.maxInt(u64), amountOfTablesToMerge);
            const w = maybeWindow orelse return tables.items.len;
            if (w.lower > 0) {
                std.mem.reverse(T, tables.items[0..w.lower]);
                std.mem.reverse(T, tables.items[w.lower..]);
                std.mem.reverse(T, tables.items);
            }
            // TODO: if we can put all the edge.. items on stack,
            // it's easier to create a new slice and collect them there,
            // so instead of a window we return a window + left slice,
            // it can eliminate expensive sorting here
            const edge = w.upper - w.lower;
            std.debug.assert(edge != 0);
            if (edge < tables.items.len) {
                sortToMerge(tables.items[edge..]);
            }

            return edge;
        }

        fn filterLeveledTables(
            toMerge: *std.ArrayList(T),
            maxDiskTableSize: u64,
            maxTablesToMerge: comptime_int,
        ) ?MergeWindowBound {
            comptime {
                if (maxTablesToMerge < 2) @compileError("maxTablesToMerge must be >= 2");
            }

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

            sortToMerge(toMerge.items);

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
                    const largestTableSize: u64 = @intCast(mergeWindow[mergeWindow.len - 1].size);

                    const firstTableSize: u64 = @intCast(mergeWindow[0].size);
                    if (firstTableSize * mergeWindow.len < largestTableSize) {
                        // too much of a difference, it's not a balanced merge, unncecessary write
                        continue;
                    }

                    var resultSize: u64 = 0;
                    for (mergeWindow) |table| resultSize += @intCast(table.size);
                    // further iterations bring only bigger tables
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
            .@"struct" => |_| T,
            else => @compileError(std.fmt.comptimePrint(
                "{s} must be a struct or a pointer to a struct",
                .{
                    @typeName(T),
                },
            )),
        };
        const lessThanFn: lessThanFnType = @field(ownerType, "lessThan");
        fn sortToMerge(toMerge: []T) void {
            std.mem.sortUnstable(T, toMerge, {}, lessThanFn);
        }
    };
}

const testing = std.testing;

test "selectTablesToMerge moves selected window to the beginning and returns edge" {
    const alloc = testing.allocator;

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
            for (tables.items) |table| table.close();
            tables.deinit(alloc);
        }

        for (case.sizes) |size| {
            const table = try MemTable.empty(alloc);
            try table.entriesBuf.resize(alloc, size);
            const t = try Table.fromMem(alloc, table);
            tables.appendAssumeCapacity(t);
        }

        const merger = Merger(*Table);
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

fn createSizedMemTable(alloc: Allocator, size: usize) !*Table {
    const memTable = try MemTable.empty(alloc);
    try memTable.entriesBuf.resize(alloc, size);
    return Table.fromMem(alloc, memTable);
}

test "filterTablesToMerge marks only selected tables inMerge" {
    const alloc = testing.allocator;

    const sizes = [_]u16{ 47, 55, 65, 76, 107, 108, 111, 117, 124, 131, 133, 162, 164, 187 };
    var tables = try std.ArrayList(*Table).initCapacity(alloc, sizes.len);
    defer {
        for (tables.items) |table| table.close();
        tables.deinit(alloc);
    }
    for (sizes) |size| {
        const table = try createSizedMemTable(alloc, size);
        tables.appendAssumeCapacity(table);
    }

    var toMerge = std.ArrayList(*Table).empty;
    defer toMerge.deinit(alloc);

    const merger = Merger(*Table);
    const window = try merger.filterTablesToMerge(alloc, tables.items, &toMerge, std.math.maxInt(u64));
    try testing.expect(window != null);
    const w = window.?;

    for (toMerge.items, 0..) |table, i| {
        const expected = i >= w.lower and i < w.upper;
        try testing.expectEqual(expected, table.inMerge);
    }
}
