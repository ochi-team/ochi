const std = @import("std");
const builtin = @import("builtin");

const zeit = @import("zeit");

const build = @import("build");
const Conf = @import("Conf.zig");
const server = @import("server.zig");

pub fn main() !void {
    var debugAlloc: ?std.heap.DebugAllocator(.{}) = null;

    const alloc: std.mem.Allocator = blk: {
        if (builtin.mode == .Debug) {
            debugAlloc = .init;
            break :blk debugAlloc.?.allocator();
        } else {
            break :blk std.heap.page_allocator;
        }
    };
    defer {
        if (debugAlloc) |*da| _ = da.deinit();
    }

    // TODO set io based on build options
    var io_impl: std.Io.Threaded = .init(alloc, .{});
    defer io_impl.deinit();
    const io = io_impl.io();

    std.debug.print("Ochi version {s}", .{build.version});

    const config = Conf.default(alloc);
    const now = try zeit.instant(io, .{ .source = .now });
    var nowBuf: [32]u8 = undefined;
    const nowStr = try now.time().bufPrint(&nowBuf, .rfc3339);

    // TODO: introduce structured logger
    std.debug.print("Ochi in mono mode starting at port={d}, time={s}\n", .{ config.server.port, nowStr });

    try server.startServer(io, alloc, config);
}

test {
    _ = @import("tidy.zig");
    _ = @import("test/server.zig");
    std.testing.refAllDecls(server);
}

// TODO: good to move the packages to its places:
// - extract structs from store/inmem/block_header.zig
// - move data.zig to data/Data.zig
// - extract components from BlockData
// - separate data and data/MemTable packages
// - separate index and index/Memtable packages
