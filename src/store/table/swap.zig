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

            var memTables = try std.ArrayList(*T).initCapacity(alloc, self.memTables.capacity);
            defer memTables.deinit(alloc);
            var diskTables = try std.ArrayList(*T).initCapacity(alloc, self.diskTables.capacity);
            defer diskTables.deinit(alloc);

            memTables.appendSliceAssumeCapacity(self.memTables.items);
            diskTables.appendSliceAssumeCapacity(self.diskTables.items);

            const removedMemTables = removeTables(&memTables, tables);
            const removedDiskTables = removeTables(&diskTables, tables);

            switch (tableKind) {
                .disk => {
                    try diskTables.append(alloc, newTable);
                    try self.diskTables.ensureTotalCapacity(alloc, diskTables.items.len);
                    try self.startDiskTablesMerge(io, alloc);
                },
                .mem => {
                    std.debug.assert(removedMemTables > 0);

                    try memTables.append(alloc, newTable);
                    try self.memTables.ensureTotalCapacity(alloc, memTables.items.len);
                    try self.startMemTablesMerge(io, alloc);
                },
            }

            if (removedDiskTables > 0 or tableKind == .disk) {
                try T.writeNames(io, alloc, self.path, diskTables.items);
            }
            self.memTables.clearRetainingCapacity();
            self.memTables.appendSliceAssumeCapacity(memTables.items);
            self.diskTables.clearRetainingCapacity();
            self.diskTables.appendSliceAssumeCapacity(diskTables.items);
            self.mxTables.unlock(io);

            // A memory destination inherits one source's permit without reacquiring it.
            const freedMemTables = removedMemTables - @intFromBool(tableKind == .mem);
            for (0..freedMemTables) |_| self.memTablesSem.post(io);

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
