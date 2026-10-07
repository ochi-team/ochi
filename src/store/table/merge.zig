const std = @import("std");

const slices = @import("../../stds/slices.zig");

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

pub const MergeWindowBound = struct {
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
            buf: []T,
            maxDiskTableSize: u64,
        ) ?[]T {
            var dst = std.ArrayList(T).initBuffer(buf);

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
            const window = filterLeveledTables(dst.items, maxDiskTableSize);
            const w = window orelse return null;

            const tablesToMerge = dst.items[w.lower..w.upper];
            for (tablesToMerge) |table| {
                std.debug.assert(!table.inMerge);
                table.inMerge = true;
            }

            return tablesToMerge;
        }

        pub fn selectTablesToMerge(
            tables: []T,
        ) usize {
            if (tables.len < 2) return tables.len;

            sortToMerge(tables);
            const maybeWindow = filterLeveledTables(tables, std.math.maxInt(u64));
            const w = maybeWindow orelse return tables.len;
            if (w.lower > 0) {
                std.mem.reverse(T, tables[0..w.lower]);
                std.mem.reverse(T, tables[w.lower..]);
                std.mem.reverse(T, tables);
            }

            // sort the leftovers
            const edge = w.upper - w.lower;
            std.debug.assert(edge != 0);
            if (edge < tables.len) {
                sortToMerge(tables[edge..]);
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
            toMerge: []T,
            maxDiskTableSize: u64,
        ) ?MergeWindowBound {
            if (toMerge.len < 2) return null;

            var window = toMerge;

            // TODO: concern is passing max int for mem tables might be not the most reliable option,
            // we must pass comptime flag whether it's a mem table / force flag to skip some of the tables to merge
            const maxSize = maxDiskTableSize / mergeMultiple;
            var idx: usize = 0;
            while (idx < window.len) {
                const tableSize: u64 = @intCast(window[idx].size);
                if (tableSize > maxSize) {
                    window = slices.swapRemove(T, window, idx);
                    continue;
                }
                idx += 1;
            }
            if (window.len < 2) return null;

            // we want to merge at least a half of them
            const upperBound = @min(maxTablesToMerge, window.len);
            const lowerBound = @max(2, (upperBound + 1) / 2);
            var maxScore: f64 = 0;
            var windowToMerge: ?MergeWindowBound = null;

            // +1 to make upperBound inclusive
            for (lowerBound..upperBound + 1) |i| {
                for (0..window.len - i + 1) |j| {
                    const bound = MergeWindowBound{ .lower = j, .upper = j + i };
                    const boundedWindow = window[bound.lower..bound.upper];

                    // last item is the largerst, we expect them sorted by size in sortToMerge
                    const largestTableSize: u64 = @intCast(boundedWindow[boundedWindow.len - 1].size);
                    const firstTableSize: u64 = @intCast(boundedWindow[0].size);
                    if (firstTableSize * boundedWindow.len < largestTableSize) {
                        // too much of a difference, it's not a balanced merge, unncecessary write
                        continue;
                    }

                    var resultSize: u64 = 0;
                    for (boundedWindow) |table| resultSize += @intCast(table.size);
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
