const std = @import("std");
const Allocator = std.mem.Allocator;

const MemTable = @import("../inmem/MemTable.zig");
const DiskTable = @import("DiskTable.zig");
const IndexBlockHeader = @import("../inmem/IndexBlockHeader.zig");
const TableHeader = @import("../inmem/TableHeader.zig");
const ColumnIDGen = @import("../inmem/ColumnIDGen.zig");
const encoding = @import("encoding");

pub const BloomValuesReaderAt = struct {
    bloom: []const u8,
    values: []const u8,
};

const Table = @This();

// either one has to be available
// // NOTE: adding a third one like object table may complicated it,
// so then it would require implement at able as an interface
disk: ?*DiskTable,
mem: ?*MemTable,

// fields for all the tables
indexBlockHeaders: []IndexBlockHeader,
tableHeader: *TableHeader,
// size is amount of bytes of compressed buffers content
size: u64,
path: []const u8,

indexBuf: []const u8,
columnsHeaderIndexBuf: []const u8,
columnsHeaderBuf: []const u8,
timestampsBuf: []const u8,

messageBloomValues: BloomValuesReaderAt,
bloomValuesShards: []BloomValuesReaderAt,

columnIDGen: *ColumnIDGen,
columnIdxs: std.StringHashMapUnmanaged(u16),

// holds ownership,
// it's necessary in order to support ref counter
alloc: Allocator,

// state

// inMerge defines whether the table is taken by a merge job
inMerge: bool = false,
// toRemove defines if the table must be removed on releasing,
// we do it via a flag instead of a direct removal,
// because a table could be retained in a reader
toRemove: std.atomic.Value(bool) = .init(false),
// refCounter follows how many clients open a table,
// first time it's open on start up,
// then readers can retain it
refCounter: std.atomic.Value(u32),

pub fn fromMem(alloc: Allocator, memTable: *MemTable) !*Table {
    // TODO: move ownership of the original meta index to the table, not only the buffers,
    // but it requires index collecting during ingestion
    var indexBlockHeaders: []IndexBlockHeader = &.{};
    const metaIndexBuf = memTable.streamWriter.metaIndexBuf.items;
    if (metaIndexBuf.len > 0) {
        indexBlockHeaders = try IndexBlockHeader.readIndexBlockHeaders(alloc, metaIndexBuf);
    }
    errdefer {
        for (indexBlockHeaders) |*hdr| hdr.deinitRead(alloc);
        if (indexBlockHeaders.len > 0) alloc.free(indexBlockHeaders);
    }

    // TODO: avoid decoding column ids, we can simply assign what we have from the stream writer
    var columnIDGen: *ColumnIDGen = undefined;
    if (memTable.streamWriter.columnKeysBuf.items.len > 0) {
        columnIDGen = try ColumnIDGen.decode(alloc, memTable.streamWriter.columnKeysBuf.items);
    } else {
        columnIDGen = try ColumnIDGen.init(alloc);
    }
    errdefer columnIDGen.deinit(alloc);

    var columnIdxs = std.StringHashMapUnmanaged(u16){};
    errdefer columnIdxs.deinit(alloc);

    if (memTable.streamWriter.columnIdxsBuf.items.len > 0) {
        var dec = encoding.Decoder.init(memTable.streamWriter.columnIdxsBuf.items);
        const count = dec.readVarInt();
        try columnIdxs.ensureTotalCapacity(alloc, @intCast(count));
        for (0..count) |_| {
            const colID: u16 = @intCast(dec.readVarInt());
            const shardIdx: u16 = @intCast(dec.readVarInt());
            const colName = columnIDGen.keyIDs.keys()[colID];
            columnIdxs.putAssumeCapacity(colName, shardIdx);
        }
    }

    const bloomTokensList = memTable.streamWriter.bloomTokensList.items;
    const bloomValuesList = memTable.streamWriter.bloomValuesList.items;
    var bloomValuesShards = try alloc.alloc(BloomValuesReaderAt, bloomValuesList.len);
    errdefer alloc.free(bloomValuesShards);

    for (0..bloomValuesList.len) |i| {
        bloomValuesShards[i] = .{
            .bloom = bloomTokensList[i].items,
            .values = bloomValuesList[i].items,
        };
    }

    const table = try alloc.create(Table);
    table.* = .{
        .mem = memTable,
        .disk = null,
        .size = memTable.streamWriter.size(),
        .path = "",
        .indexBlockHeaders = indexBlockHeaders,
        .tableHeader = &memTable.tableHeader,
        .indexBuf = memTable.streamWriter.indexBuf.items,
        .columnsHeaderIndexBuf = memTable.streamWriter.columnsHeaderIndexBuf.items,
        .columnsHeaderBuf = memTable.streamWriter.columnsHeaderBuf.items,
        .timestampsBuf = memTable.streamWriter.timestampsBuf.items,
        .messageBloomValues = .{
            .bloom = memTable.streamWriter.messageBloomTokensBuf.items,
            .values = memTable.streamWriter.messageBloomValuesBuf.items,
        },
        .bloomValuesShards = bloomValuesShards,
        .columnIDGen = columnIDGen,
        .columnIdxs = columnIdxs,
        .refCounter = .init(1),
        .alloc = alloc,
    };

    return table;
}

pub fn close(self: *Table) void {
    if (self.disk) |disk| {
        disk.deinit(self.alloc);
    }

    if (self.mem) |mem| {
        mem.deinit(self.alloc);
    }

    for (self.indexBlockHeaders) |*hdr| hdr.deinitRead(self.alloc);
    if (self.indexBlockHeaders.len > 0) self.alloc.free(self.indexBlockHeaders);

    self.columnIDGen.deinit(self.alloc);
    self.columnIdxs.deinit(self.alloc);
    self.alloc.free(self.bloomValuesShards);

    const shouldRemove = self.disk != null and self.toRemove.load(.acquire);
    if (shouldRemove) {
        // TODO: replace to an error log
        std.fs.deleteTreeAbsolute(self.path) catch |err| {
            std.debug.panic("failed to delete table '{s}': {s}", .{ self.path, @errorName(err) });
        };
    }

    if (self.path.len > 0) {
        self.alloc.free(self.path);
    }

    self.alloc.destroy(self);
}

pub fn retain(self: *Table) void {
    _ = self.refCounter.fetchAdd(1, .acquire);
}

pub fn release(self: *Table) void {
    const prev = self.refCounter.fetchSub(1, .acq_rel);
    std.debug.assert(prev > 0);

    if (prev != 1) return;

    self.close();
}

pub fn lessThan(_: void, one: *Table, another: *Table) bool {
    return one.size < another.size;
}

const testing = std.testing;

test "release fromMem does not affect filesystem path" {
    const alloc = testing.allocator;

    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    const rootPath = try tmp.dir.realpathAlloc(alloc, ".");
    defer alloc.free(rootPath);
    const missingPath = try std.fs.path.join(alloc, &.{ rootPath, "never-created" });
    defer alloc.free(missingPath);

    const memTable = try MemTable.init(alloc);

    const table = try Table.fromMem(alloc, memTable);
    // we expected only second release close cleans the table, otherwise it's a memory leak
    table.retain();

    try testing.expectError(error.FileNotFound, std.fs.accessAbsolute(missingPath, .{}));
    table.release();
    try testing.expectError(error.FileNotFound, std.fs.accessAbsolute(missingPath, .{}));
    table.release();
    try testing.expectError(error.FileNotFound, std.fs.accessAbsolute(missingPath, .{}));
}

const Line = @import("../lines.zig").Line;
const Field = @import("../lines.zig").Field;

test "fromMem creates proper table from mem table with populated data" {
    const alloc = testing.allocator;
    const memTable = try MemTable.init(alloc);

    var fields1 = [_]Field{
        .{ .key = "level", .value = "info" },
        .{ .key = "app", .value = "seq" },
    };
    var fields2 = [_]Field{
        .{ .key = "level", .value = "warn" },
        .{ .key = "app", .value = "seq" },
    };
    const line1 = Line{
        .timestampNs = 1,
        .sid = .{ .id = 1, .tenantID = "1234" },
        .fields = fields1[0..],
    };
    const line2 = Line{
        .timestampNs = 2,
        .sid = .{ .id = 1, .tenantID = "1234" },
        .fields = fields2[0..],
    };

    var lines = [_]*const Line{ &line1, &line2 };

    try memTable.addLines(alloc, lines[0..]);

    const table = try Table.fromMem(alloc, memTable);
    defer table.release();

    try testing.expect(table.indexBuf.len > 0);
    try testing.expect(table.columnsHeaderIndexBuf.len > 0);
    try testing.expect(table.columnsHeaderBuf.len > 0);
    try testing.expect(table.timestampsBuf.len > 0);

    try testing.expectEqual(@as(usize, 1), table.bloomValuesShards.len);
    // wait, if "info" or "seq" is added, the bloom filters and values could be generated
    // try testing.expect(table.bloomValuesShards[0].values.len > 0);

    try testing.expect(table.columnIdxs.count() > 0);
    try testing.expect(table.columnIDGen.keyIDs.count() > 0);

    try testing.expectEqual(memTable.streamWriter.indexBuf.items.len, table.indexBuf.len);
    try testing.expectEqual(memTable.streamWriter.columnsHeaderIndexBuf.items.len, table.columnsHeaderIndexBuf.len);
    try testing.expectEqual(memTable.streamWriter.columnsHeaderBuf.items.len, table.columnsHeaderBuf.len);
    try testing.expectEqual(memTable.streamWriter.timestampsBuf.items.len, table.timestampsBuf.len);
}
