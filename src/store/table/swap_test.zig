const std = @import("std");
const Allocator = std.mem.Allocator;

const DecompressionPool = @import("../compression/DecompressionPool.zig");

const Swapper = @import("swap.zig").Swapper;

const testing = std.testing;

const Table = @import("../index/Table.zig");
const MemTable = @import("../index/MemTable.zig");
const IndexRecorder = @import("../index/IndexRecorder.zig");

fn createSizedMemTable(alloc: Allocator, decompressionPool: *DecompressionPool, size: usize) !*Table {
    const memTable = try MemTable.empty(alloc);
    errdefer memTable.deinit(alloc);

    try memTable.entriesBuf.resize(alloc, size);

    return Table.fromMem(testing.io, alloc, memTable, decompressionPool);
}

test "removeTables removes exact pointers" {
    const alloc = testing.allocator;
    const io = testing.io;
    const decompressionPool = try DecompressionPool.init(alloc, 1);
    defer decompressionPool.deinit(alloc);

    const one = try createSizedMemTable(alloc, decompressionPool, 100);
    defer one.close(io);

    const two = try createSizedMemTable(alloc, decompressionPool, 100);
    defer two.close(io);

    const three = try createSizedMemTable(alloc, decompressionPool, 100);
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
