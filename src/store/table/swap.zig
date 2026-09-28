const std = @import("std");
const Allocator = std.mem.Allocator;
const Io = std.Io;

const tracy = @import("tracy");

const merge = @import("merge.zig");

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
            const z = tracy.Zone.begin(.{
                .src = @src(),
                .name = "swapTables",
            });
            defer z.end();

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
                    try self.startMemTablesMerge(io, alloc);
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
                table.toRemove.store(true, .release);
                table.release(io);
            }
        }

        pub fn removeTables(tables: *std.ArrayList(*T), remove: []*T) u32 {
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

test {
    _ = @import("swap_test.zig");
}
