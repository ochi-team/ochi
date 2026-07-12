const std = @import("std");
const Allocator = std.mem.Allocator;
const Io = std.Io;

const merge = @import("merge.zig");
const CompressionPool = @import("../CompressionPool.zig").CompressionPool;

pub fn Swapper(
    comptime Self: type,
    comptime T: type,
) type {
    return struct {
        pub fn swapTables(
            self: *Self,
            io: Io,
            alloc: Allocator,
            tables: []*T,
            newTable: *T,
            tableKind: merge.TableKind,
        ) !void {
            self.mxTables.lockUncancelable(io);
            errdefer self.mxTables.unlock(io);

            const removedMemTables = removeTables(&self.memTables, tables);
            const removedDiskTables = removeTables(&self.diskTables, tables);

            switch (tableKind) {
                .disk => {
                    try self.diskTables.append(alloc, newTable);
                    try self.startDiskTablesMerge(io, alloc);
                },
                .mem => {
                    try self.memTables.append(alloc, newTable);
                    try self.startDiskTablesMerge(io, alloc);
                },
            }

            if (removedDiskTables > 0 or tableKind == .disk) {
                try T.writeNames(io, alloc, self.path, self.diskTables.items);
            }
            self.mxTables.unlock(io);

            for (0..removedMemTables) |_| self.memTablesSem.post(io);
            if (tableKind == .mem) self.memTablesSem.waitUncancelable(io);

            std.debug.assert(tables.len == removedDiskTables + removedMemTables);

            for (tables) |table| {
                // remove via reference counter,
                // it could have been open by a client.
                // order flag doesn't matter, we don't expect any other part to change it back to
                table.toRemove.store(true, .unordered);
                table.release(io);
            }
        }

        fn removeTables(tables: *std.ArrayList(*T), remove: []*T) u32 {
            var removed: u32 = 0;
            var i: usize = 0;
            while (i < tables.items.len) {
                var isRemoved = false;
                for (remove) |r| {
                    if (tables.items[i] == r) {
                        _ = tables.swapRemove(i);
                        removed += 1;
                        isRemoved = true;
                        break;
                    }
                }
                if (!isRemoved) i += 1;
            }

            return removed;
        }
    };
}

const testing = std.testing;

const Table = @import("../index/Table.zig");
const MemTable = @import("../index/MemTable.zig");
const IndexRecorder = @import("../index/IndexRecorder.zig");

fn createSizedMemTable(alloc: Allocator, compressionPool: *CompressionPool, size: usize) !*Table {
    const memTable = try MemTable.empty(alloc);
    errdefer memTable.deinit(alloc);

    try memTable.entriesBuf.resize(alloc, size);

    return Table.fromMem(testing.io, alloc, memTable, compressionPool);
}

test "removeTables removes exact pointers" {
    const alloc = testing.allocator;
    const io = testing.io;
    const compressionPool = try CompressionPool.init(alloc, 1);
    defer compressionPool.deinit(alloc);

    const one = try createSizedMemTable(alloc, compressionPool, 100);
    defer one.close(io);

    const two = try createSizedMemTable(alloc, compressionPool, 100);
    defer two.close(io);

    const three = try createSizedMemTable(alloc, compressionPool, 100);
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
