const std = @import("std");

const Block = @import("Block.zig");
const BlockData = @import("BlockData.zig").BlockData;
const MemTable = @import("MemTable.zig");
const Table = @import("../data/Table.zig");
const BlockHeader = @import("BlockHeader.zig");
const TimestampsEncoder = @import("TimestampsEncoder.zig");
const CompressionPool = @import("../compression/CompressionPool.zig");
const DecompressionPool = @import("../compression/DecompressionPool.zig");

const TableWriter = @import("TableWriter.zig");

const testing = std.testing;

const Line = @import("../lines.zig").Line;
const Field = @import("../lines.zig").Field;
const SID = @import("../lines.zig").SID;
const TableReader = @import("TableReader.zig");

test "writeBlock and writeData produce identical buffer output" {
    const alloc = testing.allocator;
    // TODO: intrument custom io to catch resource leaks, e.g. not closed files
    const io = testing.io;
    const timestampsEncoders = try TimestampsEncoder.TimestampsEncoderPool.init(alloc, 1);
    defer timestampsEncoders.deinit(alloc);
    const compressionPool = try CompressionPool.init(alloc, 1);
    defer compressionPool.deinit(alloc);
    const decompressionPool = try DecompressionPool.init(alloc, 1);
    defer decompressionPool.deinit(alloc);

    var fields1 = [_]Field{
        .{ .key = "app", .value = "seq" },
        .{ .key = "level", .value = "info" },
    };
    var fields2 = [_]Field{
        .{ .key = "app", .value = "seq" },
        .{ .key = "level", .value = "warn" },
    };
    var fields3 = [_]Field{
        .{ .key = "app", .value = "seq" },
        .{ .key = "level", .value = "warn" },
    };
    const sid = SID{ .id = 1, .tenantID = 1111 };
    const line1 = Line{ .timestampNs = 1, .fields = &fields1 };
    const line2 = Line{ .timestampNs = 2, .fields = &fields2 };
    const line3 = Line{ .timestampNs = 3, .fields = &fields3 };
    var lines = [_]Line{ line1, line2, line3 };

    // Writer 1: encode via writeBlock
    const memTable1 = try MemTable.init(alloc);
    const table1 = try Table.fromMem(io, alloc, memTable1, decompressionPool);
    defer table1.close(io);

    const writer1 = try TableWriter.initMem(alloc, memTable1, timestampsEncoders, compressionPool);
    defer writer1.deinit(alloc);

    var block = try Block.initFromLines(alloc, &lines);
    defer block.deinit(alloc);

    var bh1 = BlockHeader.initFromBlock(&block, sid);
    try writer1.writeBlock(io, alloc, &block, &bh1);

    // Build StreamReader from writer1's buffers to populate BlockData
    const sr = TableReader{
        .table = table1,
        .metaIndexBuf = writer1.metaindexDst.buffer.items,
        .columnsKeysBuf = writer1.columnKeysDst.buffer.items,
        .columnIdxsBuf = writer1.columnIdxsDst.buffer.items,
        .columnIDGen = writer1.columnIDGen,
        .colIdx = &writer1.colIdx,
    };

    var bd = BlockData.initEmpty();
    defer bd.deinit(alloc);
    bd.sid = sid;
    try bd.readFrom(io, alloc, &bh1, &sr);

    // Writer 2: re-encode the same data via writeData
    const memTable2 = try MemTable.init(alloc);
    defer memTable2.deinit(alloc);
    const writer2 = try TableWriter.initMem(alloc, memTable2, timestampsEncoders, compressionPool);
    defer writer2.deinit(alloc);

    var bh2 = BlockHeader.initFromData(&bd, sid);
    try writer2.writeData(io, alloc, &bh2, &bd);

    // Finalize both writers
    try writer1.writeColumnKeys(io, alloc);
    try writer1.writeColumnIndexes(io, alloc);
    try writer2.writeColumnKeys(io, alloc);
    try writer2.writeColumnIndexes(io, alloc);

    // Compare all data buffers
    try testing.expectEqualSlices(u8, writer1.timestampsDst.buffer.items, writer2.timestampsDst.buffer.items);
    try testing.expectEqualSlices(u8, writer1.columnsHeaderDst.buffer.items, writer2.columnsHeaderDst.buffer.items);
    try testing.expectEqualSlices(u8, writer1.columnsHeaderIndexDst.buffer.items, writer2.columnsHeaderIndexDst.buffer.items);
    try testing.expectEqualSlices(u8, writer1.messageBloomValuesDst.buffer.items, writer2.messageBloomValuesDst.buffer.items);
    try testing.expectEqualSlices(u8, writer1.messageBloomTokensDst.buffer.items, writer2.messageBloomTokensDst.buffer.items);
    try testing.expectEqual(writer1.bloomValuesList.items.len, writer2.bloomValuesList.items.len);
    for (writer1.bloomValuesList.items, writer2.bloomValuesList.items) |b1, b2| {
        try testing.expectEqualSlices(u8, b1.buffer.items, b2.buffer.items);
    }
    try testing.expectEqual(writer1.bloomTokensList.items.len, writer2.bloomTokensList.items.len);
    for (writer1.bloomTokensList.items, writer2.bloomTokensList.items) |b1, b2| {
        try testing.expectEqualSlices(u8, b1.buffer.items, b2.buffer.items);
    }
    try testing.expectEqualSlices(u8, writer1.columnKeysDst.buffer.items, writer2.columnKeysDst.buffer.items);
    try testing.expectEqualSlices(u8, writer1.columnIdxsDst.buffer.items, writer2.columnIdxsDst.buffer.items);
}

test "writeBlock with many columns does not overflow columns header index buffer" {
    const alloc = testing.allocator;
    const io = testing.io;
    const timestampsEncoders = try TimestampsEncoder.TimestampsEncoderPool.init(alloc, 1);
    defer timestampsEncoders.deinit(alloc);
    const compressionPool = try CompressionPool.init(alloc, 1);
    defer compressionPool.deinit(alloc);

    var fields = [_]Field{
        .{ .key = "k01", .value = "v1" },
        .{ .key = "k02", .value = "v2" },
        .{ .key = "k03", .value = "v3" },
        .{ .key = "k04", .value = "v4" },
        .{ .key = "k05", .value = "v5" },
        .{ .key = "k06", .value = "v6" },
        .{ .key = "k07", .value = "v7" },
        .{ .key = "k08", .value = "v8" },
        .{ .key = "k09", .value = "v9" },
        .{ .key = "k10", .value = "v10" },
        .{ .key = "k11", .value = "v11" },
        .{ .key = "k12", .value = "v12" },
    };

    const sid = SID{ .id = 1, .tenantID = 1111 };
    const line = Line{ .timestampNs = 1, .fields = &fields };
    var lines = [_]Line{line};

    const memTable = try MemTable.init(alloc);
    defer memTable.deinit(alloc);
    const writer = try TableWriter.initMem(alloc, memTable, timestampsEncoders, compressionPool);
    defer writer.deinit(alloc);

    var block = try Block.initFromLines(alloc, &lines);
    defer block.deinit(alloc);

    var bh = BlockHeader.initFromBlock(&block, sid);
    try writer.writeBlock(io, alloc, &block, &bh);
}
