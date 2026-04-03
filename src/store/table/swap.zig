const std = @import("std");
const Allocator = std.mem.Allocator;

const merge = @import("merge.zig");

const TableRefCountContract = @import("contract.zig").TableRefCountContract;

pub fn Swapper(
    comptime Self: type,
    comptime T: type,
) type {
    const contract = TableRefCountContract(T);
    comptime {
        contract.satisfies(T, false) catch |err| {
            @compileError("TableRefCountContract is not satisfied by " ++ @typeName(T) ++ ": " ++ @errorName(err));
        };
    }

    return struct {
        pub fn swapTables(
            self: *Self,
            alloc: Allocator,
            tables: []*T,
            newTable: *T,
            tableKind: merge.TableKind,
        ) !void {
            self.mxTables.lock();
            errdefer self.mxTables.unlock();

            const removedMemTables = removeTables(&self.memTables, tables);
            const removedDiskTables = removeTables(&self.diskTables, tables);

            switch (tableKind) {
                .disk => {
                    try self.diskTables.append(alloc, newTable);
                    self.startDiskTablesMerge(alloc);
                },
                .mem => {
                    try self.memTables.append(alloc, newTable);
                    self.startMemTablesMerge(alloc);
                },
            }

            if (removedDiskTables > 0 or tableKind == .disk) {
                try T.writeNames(alloc, self.path, self.diskTables.items);
            }
            self.mxTables.unlock();

            for (0..removedMemTables) |_| self.memTablesSem.post();
            if (tableKind == .mem) self.memTablesSem.wait();

            std.debug.assert(tables.len == removedDiskTables + removedMemTables);

            for (tables) |table| {
                // remove via reference counter,
                // it could have been open by a client.
                // order flag doesn't matter, we don't expect any other part to change it back to
                table.toRemove.store(true, .unordered);
                table.release();
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
