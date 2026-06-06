const std = @import("std");
const Allocator = std.mem.Allocator;
const Io = std.Io;
const Dir = Io.Dir;

const filenames = @import("../../filenames.zig");
const fs = @import("../../fs.zig");
const MemTable = @import("../inmem/MemTable.zig");
const DiskTable = @import("DiskTable.zig");
const IndexBlockHeader = @import("../inmem/IndexBlockHeader.zig");
const BlockHeader = @import("../inmem/BlockHeader.zig");
const TableHeader = @import("../inmem/TableHeader.zig");
const ColumnIDGen = @import("../inmem/ColumnIDGen.zig");
const BlockData = @import("../inmem/BlockData.zig").BlockData;
const Block = @import("../inmem/Block.zig");
const Unpacker = @import("../inmem/Unpacker.zig");
const ValuesDecoder = @import("../inmem/ValuesDecoder.zig");
const StreamReader = @import("../inmem/reader.zig").StreamReader;

const Line = @import("../lines.zig").Line;
const SID = @import("../lines.zig").SID;
const copyFields = @import("../lines.zig").copyFields;
const freeFields = @import("../lines.zig").freeFields;
const Query = @import("../../query/Query.zig");

const catalog = @import("../table/catalog.zig");

const InnerTag = enum { mem, disk };
const Inner = union(InnerTag) {
    mem: *MemTable,
    disk: *DiskTable,
};

const Table = @This();

inner: Inner,

indexBlockHeaders: []IndexBlockHeader,
columnIDGen: *ColumnIDGen,
columnIdxs: std.StringHashMapUnmanaged(u16),

// size is amount of bytes of compressed buffers content
size: u64,
path: []const u8,

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
// TODO: make it u16
refCounter: std.atomic.Value(u32),

// TODO: investigate how we could make a checksum and validate it on opening a table
pub fn openAll(io: Io, parentAlloc: Allocator, path: []const u8) !std.ArrayList(*Table) {
    Dir.createDirAbsolute(io, path, .default_dir) catch |err| switch (err) {
        // TODO: if the foler already exists we must read it's content and log an error
        // in case the tables on the disk are missing in the tables list
        Dir.CreateDirError.PathAlreadyExists => {},
        else => std.debug.panic(
            "failed to create a table dir '{s}': {s}",
            .{ path, @errorName(err) },
        ),
    };

    var fba = std.heap.stackFallback(2048, parentAlloc);
    const alloc = fba.get();

    // read table names,
    // they are given either from a file or listed directories in the path
    const tablesFilePath = try std.fs.path.join(alloc, &[_][]const u8{ path, filenames.tables });
    defer alloc.free(tablesFilePath);
    var tableNames = try catalog.readNames(io, alloc, tablesFilePath, true);
    defer {
        for (tableNames.items) |tableName| alloc.free(tableName);
        tableNames.deinit(alloc);
    }

    // syncing tables with a json, make sure all the listed dirs exist
    try catalog.validateTablesExist(io, alloc, path, tableNames.items);

    // syncing tables with the given names remove all the not listed dirs
    try catalog.removeUnusedTables(io, alloc, path, tableNames.items);

    // open tables
    var tables = try std.ArrayList(*Table).initCapacity(parentAlloc, tableNames.items.len);
    errdefer {
        tables.deinit(parentAlloc);
    }
    for (tableNames.items) |tableName| {
        // don't clean tablePath, Table owns it
        const tablePath = try std.fs.path.join(parentAlloc, &.{ path, tableName });
        errdefer parentAlloc.free(tablePath);
        const table = try Table.open(io, parentAlloc, tablePath);
        tables.appendAssumeCapacity(table);
    }

    // fsync after opening tables because it creates the files
    try fs.syncPathAndParentDir(io, path);

    return tables;
}

pub fn open(io: Io, alloc: Allocator, path: []const u8) !*Table {
    var pathBuf: [std.fs.max_path_bytes]u8 = undefined;
    var pathWriter = std.Io.Writer.fixed(&pathBuf);

    const header = try TableHeader.readFile(io, alloc, path);

    try std.fs.path.fmtJoin(&.{ path, filenames.columnKeys }).format(&pathWriter);
    const columnKeysContent = try fs.readAll(io, alloc, pathWriter.buffered());
    defer alloc.free(columnKeysContent);
    pathWriter.end = 0;
    var columnIDGen: *ColumnIDGen = blk: {
        if (columnKeysContent.len > 0) {
            break :blk try ColumnIDGen.decode(alloc, columnKeysContent);
        } else {
            break :blk try ColumnIDGen.init(alloc);
        }
    };
    errdefer columnIDGen.deinit(alloc);

    var columnIdxs = std.StringHashMapUnmanaged(u16){};
    errdefer columnIdxs.deinit(alloc);

    try std.fs.path.fmtJoin(&.{ path, filenames.columnIdxs }).format(&pathWriter);
    const columnIdxsContent = try fs.readAll(io, alloc, pathWriter.buffered());
    defer alloc.free(columnIdxsContent);
    pathWriter.end = 0;
    if (columnIdxsContent.len > 0) {
        columnIdxs = try columnIDGen.decodeColumnIdxs(alloc, columnIdxsContent);
    }

    try std.fs.path.fmtJoin(&.{ path, filenames.metaindex }).format(&pathWriter);
    const metaindexContent = try fs.readAll(io, alloc, pathWriter.buffered());
    defer alloc.free(metaindexContent);
    pathWriter.end = 0;
    var indexBlockHeaders: []IndexBlockHeader = &.{};
    if (metaindexContent.len > 0) {
        indexBlockHeaders = try IndexBlockHeader.readIndexBlockHeaders(alloc, metaindexContent);
    }
    errdefer if (indexBlockHeaders.len > 0) alloc.free(indexBlockHeaders);

    try std.fs.path.fmtJoin(&.{ path, filenames.index }).format(&pathWriter);
    const indexFile = try std.Io.Dir.openFileAbsolute(io, pathWriter.buffered(), .{});
    errdefer indexFile.close(io);
    pathWriter.end = 0;

    try std.fs.path.fmtJoin(&.{ path, filenames.columnsHeaderIndex }).format(&pathWriter);
    const columnsHeaderIndexFile = try std.Io.Dir.openFileAbsolute(io, pathWriter.buffered(), .{});
    errdefer columnsHeaderIndexFile.close(io);
    pathWriter.end = 0;

    try std.fs.path.fmtJoin(&.{ path, filenames.columnsHeader }).format(&pathWriter);
    const columnsHeaderFile = try std.Io.Dir.openFileAbsolute(io, pathWriter.buffered(), .{});
    errdefer columnsHeaderFile.close(io);
    pathWriter.end = 0;

    try std.fs.path.fmtJoin(&.{ path, filenames.timestamps }).format(&pathWriter);
    const timestampsFile = try std.Io.Dir.openFileAbsolute(io, pathWriter.buffered(), .{});
    errdefer timestampsFile.close(io);
    pathWriter.end = 0;

    try std.fs.path.fmtJoin(&.{ path, filenames.messageTokens }).format(&pathWriter);
    const messageBloomTokensFile = try std.Io.Dir.openFileAbsolute(io, pathWriter.buffered(), .{});
    errdefer messageBloomTokensFile.close(io);
    pathWriter.end = 0;

    try std.fs.path.fmtJoin(&.{ path, filenames.messageValues }).format(&pathWriter);
    const messageBloomValuesFile = try std.Io.Dir.openFileAbsolute(io, pathWriter.buffered(), .{});
    errdefer messageBloomValuesFile.close(io);
    pathWriter.end = 0;

    const shardCount: usize = @intCast(header.bloomValuesBuffersAmount);
    var bloomTokensFiles = try alloc.alloc(std.Io.File, shardCount);
    errdefer alloc.free(bloomTokensFiles);
    var bloomValuesFiles = try alloc.alloc(std.Io.File, shardCount);
    errdefer alloc.free(bloomValuesFiles);

    var shardIdx: usize = 0;
    errdefer {
        for (bloomTokensFiles[0..shardIdx]) |file| file.close(io);
        for (bloomValuesFiles[0..shardIdx]) |file| file.close(io);
    }
    while (shardIdx < shardCount) : (shardIdx += 1) {
        const bloomTokensPath = try filenames.writeBloomFilePath(
            &pathBuf,
            path,
            filenames.bloomTokens,
            @intCast(shardIdx),
        );
        const bloomValuesPath = try filenames.writeBloomFilePath(
            &pathBuf,
            path,
            filenames.bloomValues,
            @intCast(shardIdx),
        );

        const bloomTokensFile = try std.Io.Dir.openFileAbsolute(io, bloomTokensPath, .{});
        errdefer bloomTokensFile.close(io);

        const bloomValuesFile = try std.Io.Dir.openFileAbsolute(io, bloomValuesPath, .{});
        errdefer bloomValuesFile.close(io);

        bloomTokensFiles[shardIdx] = bloomTokensFile;
        bloomValuesFiles[shardIdx] = bloomValuesFile;
    }

    const disk = try alloc.create(DiskTable);
    errdefer alloc.destroy(disk);
    disk.* = .{
        .tableHeader = header,
        .indexFile = indexFile,
        .columnsHeaderIndexFile = columnsHeaderIndexFile,
        .columnsHeaderFile = columnsHeaderFile,
        .timestampsFile = timestampsFile,
        .messageBloomTokensFile = messageBloomTokensFile,
        .messageBloomValuesFile = messageBloomValuesFile,
        .bloomTokensFiles = bloomTokensFiles,
        .bloomValuesFiles = bloomValuesFiles,
    };

    const table = try alloc.create(Table);
    table.* = .{
        .inner = .{ .disk = disk },
        .size = header.compressedSize,
        .path = path,
        .indexBlockHeaders = indexBlockHeaders,
        .columnIDGen = columnIDGen,
        .columnIdxs = columnIdxs,
        .refCounter = .init(1),
        .alloc = alloc,
    };

    return table;
}

pub fn close(self: *Table, io: Io) void {
    switch (self.inner) {
        .disk => |disk| {
            disk.deinit(io, self.alloc);
        },
        .mem => |mem| {
            mem.deinit(self.alloc);
        },
    }

    if (self.indexBlockHeaders.len > 0) self.alloc.free(self.indexBlockHeaders);

    self.columnIDGen.deinit(self.alloc);
    self.columnIdxs.deinit(self.alloc);

    const shouldRemove = self.inner == .disk and self.toRemove.load(.acquire);
    if (shouldRemove) {
        // TODO: replace to an error log
        // TODO: review it to make removing more reliable,
        // e.g. deletion must be intrrupted in the middle leaving a half baked table
        fs.deleteTreeAbsolute(io, self.path) catch |err| {
            std.debug.panic("failed to delete table '{s}': {s}", .{ self.path, @errorName(err) });
        };
    }

    if (self.path.len > 0) {
        self.alloc.free(self.path);
    }

    self.alloc.destroy(self);
}

pub fn fromMem(alloc: Allocator, memTable: *MemTable) !*Table {
    std.debug.assert(memTable.size() == memTable.tableHeader.compressedSize);

    // TODO: move ownership of the original meta index to the table, not only the buffers,
    // but it requires index collecting during ingestion
    var indexBlockHeaders: []IndexBlockHeader = &.{};
    const metaIndexBuf = memTable.metaIndexBuf.items;
    if (metaIndexBuf.len > 0) {
        indexBlockHeaders = try IndexBlockHeader.readIndexBlockHeaders(alloc, metaIndexBuf);
    }
    errdefer if (indexBlockHeaders.len > 0) alloc.free(indexBlockHeaders);

    // TODO: avoid decoding column ids, we can simply assign what we have from the stream writer
    const columnIDGen = blk: {
        if (memTable.columnKeysBuf.items.len > 0) {
            break :blk try ColumnIDGen.decode(alloc, memTable.columnKeysBuf.items);
        } else {
            break :blk try ColumnIDGen.init(alloc);
        }
    };
    errdefer columnIDGen.deinit(alloc);

    var columnIdxs = std.StringHashMapUnmanaged(u16){};
    errdefer columnIdxs.deinit(alloc);

    if (memTable.columnIdxsBuf.items.len > 0) {
        columnIdxs = try columnIDGen.decodeColumnIdxs(alloc, memTable.columnIdxsBuf.items);
    }

    const table = try alloc.create(Table);
    table.* = .{
        .inner = .{ .mem = memTable },
        .size = memTable.tableHeader.compressedSize,
        .path = "",
        .indexBlockHeaders = indexBlockHeaders,
        .columnIDGen = columnIDGen,
        .columnIdxs = columnIdxs,
        .refCounter = .init(1),
        .alloc = alloc,
    };

    return table;
}

pub fn tableHeader(self: *const Table) TableHeader {
    switch (self.inner) {
        .disk => |disk| return disk.tableHeader,
        .mem => |mem| return mem.tableHeader,
    }
}

pub fn writeNames(io: Io, alloc: Allocator, path: []const u8, tables: []*Table) anyerror!void {
    var stackFba = std.heap.stackFallback(1024, alloc);
    const fba = stackFba.get();

    var tableNames = try std.ArrayList([]const u8).initCapacity(fba, tables.len);
    defer tableNames.deinit(fba);

    for (tables) |table| {
        std.debug.assert(table.inner == .disk);
        tableNames.appendAssumeCapacity(std.fs.path.basename(table.path));
    }

    // TODO: worth migrating json to names suparated by new line \n
    // since they are limited to 16 symbols hex symbols [0-9A-F]
    const data = try std.json.Stringify.valueAlloc(fba, tableNames.items, .{
        .whitespace = .minified,
    });
    defer fba.free(data);

    const tablesFilePath = try std.fs.path.join(fba, &.{ path, filenames.tables });
    defer fba.free(tablesFilePath);

    try fs.writeBufferToFileAtomic(io, tablesFilePath, data, true);
}

pub fn retain(self: *Table) void {
    _ = self.refCounter.fetchAdd(1, .acquire);
}

// Table is a managed object, so it does not accept an allocator,
// because the allocator is in a read path is (arena) not the same as in a write path which
// which created that table
// TODO: find how we can explicitly carry an allocator
pub fn release(self: *Table, io: Io) void {
    const prev = self.refCounter.fetchSub(1, .acq_rel);
    std.debug.assert(prev > 0);

    if (prev != 1) return;

    self.close(io);
}

pub fn readIndex(self: *const Table, io: Io, buf: []u8, offset: u64) !usize {
    switch (self.inner) {
        .disk => |disk| return readFile(disk.indexFile, io, buf, offset),
        .mem => |mem| return readBuf(buf, mem.indexBuf.items, offset),
    }
}

pub fn readColumnsHeaderIndex(self: *const Table, io: Io, buf: []u8, offset: u64) !usize {
    switch (self.inner) {
        .disk => |disk| return readFile(disk.columnsHeaderIndexFile, io, buf, offset),
        .mem => |mem| return readBuf(buf, mem.columnsHeaderIndexBuf.items, offset),
    }
}

pub fn readColumnsHeader(self: *const Table, io: Io, buf: []u8, offset: u64) !usize {
    switch (self.inner) {
        .disk => |disk| return readFile(disk.columnsHeaderFile, io, buf, offset),
        .mem => |mem| return readBuf(buf, mem.columnsHeaderBuf.items, offset),
    }
}

pub fn readTimestamps(self: *const Table, io: Io, buf: []u8, offset: u64) !usize {
    switch (self.inner) {
        .disk => |disk| return readFile(disk.timestampsFile, io, buf, offset),
        .mem => |mem| return readBuf(buf, mem.timestampsBuf.items, offset),
    }
}

pub fn readMessageBloomTokens(self: *const Table, io: Io, buf: []u8, offset: u64) !usize {
    switch (self.inner) {
        .disk => |disk| return readFile(disk.messageBloomTokensFile, io, buf, offset),
        .mem => |mem| return readBuf(buf, mem.messageBloomTokensBuf.items, offset),
    }
}

pub fn readMessageBloomValues(self: *const Table, io: Io, buf: []u8, offset: u64) !usize {
    switch (self.inner) {
        .disk => |disk| return readFile(disk.messageBloomValuesFile, io, buf, offset),
        .mem => |mem| return readBuf(buf, mem.messageBloomValuesBuf.items, offset),
    }
}

pub fn readBloomTokens(self: *const Table, io: Io, buf: []u8, offset: u64, shardIdx: usize) !usize {
    switch (self.inner) {
        .disk => |disk| return readFile(disk.bloomTokensFiles[shardIdx], io, buf, offset),
        .mem => |mem| {
            std.debug.assert(shardIdx == 0);
            return readBuf(buf, mem.bloomTokensBuf.items, offset);
        },
    }
}

pub fn readBloomValues(self: *const Table, io: Io, buf: []u8, offset: u64, shardIdx: usize) !usize {
    switch (self.inner) {
        .disk => |disk| return readFile(disk.bloomValuesFiles[shardIdx], io, buf, offset),
        .mem => |mem| {
            std.debug.assert(shardIdx == 0);
            return readBuf(buf, mem.bloomValuesBuf.items, offset);
        },
    }
}

// TODO: compare to reading from the interface,
// the trick is the read operations are page aligned and it makes sense to continue reading
// from the rest of the cache in case of many small read calls,
// ideally get the page size at the compile time if possible
fn readFile(file: Io.File, io: Io, buf: []u8, offset: u64) !usize {
    const n = try file.readPositionalAll(io, buf, offset);
    return n;
}

fn readBuf(dst: []u8, src: []const u8, offset: u64) !usize {
    const start: usize = @intCast(offset);
    if (start >= src.len) return 0;
    const n = @min(dst.len, src.len - start);
    @memcpy(dst[0..n], src[start .. start + n]);
    return n;
}

pub fn queryLines(self: *Table, io: Io, alloc: Allocator, dst: *std.ArrayList(Line), sids: []SID, query: Query) !void {
    // TODO: assert sids are sorted
    var indexBlockHeaders = self.indexBlockHeaders;
    var sidsToFind = sids;

    while (indexBlockHeaders.len > 0 and sidsToFind.len > 0) {
        var sid = sidsToFind[0];
        var indexBlockHeader = indexBlockHeaders[0];

        if (sid.lessThan(&indexBlockHeader.sid)) {
            const n = std.sort.lowerBound(SID, sidsToFind, indexBlockHeader.sid, SID.order);
            if (n == sidsToFind.len) {
                sidsToFind = sidsToFind[0..0];
                break;
            }

            sid = sidsToFind[n];
            sidsToFind = sidsToFind[n..];
        }

        var n: usize = 0;
        if (indexBlockHeaders[0].sid.lessThan(&sid)) {
            n = std.sort.lowerBound(IndexBlockHeader, indexBlockHeaders, sid, indexBlockHeaderSidLowerBoundOrder);
            // n can be indexBlockHeaders.len,
            // so we make a step back to check the last value
            if (n > 0) n -= 1;
        }

        indexBlockHeader = indexBlockHeaders[n];
        indexBlockHeaders = indexBlockHeaders[n + 1 ..];

        if (query.start > indexBlockHeader.maxTs or query.end < indexBlockHeader.minTs) {
            // block doesn't contain the requested range
            continue;
        }

        var blockHeaders = std.ArrayList(BlockHeader).empty;
        defer blockHeaders.deinit(alloc);
        const indexBuffer = try alloc.alloc(u8, indexBlockHeader.size - indexBlockHeader.offset);
        defer alloc.free(indexBuffer);
        n = try self.readIndex(io, indexBuffer, indexBlockHeader.offset);
        std.debug.assert(indexBuffer.len == n);
        try BlockHeader.decodeIndexWindow(alloc, &blockHeaders, indexBuffer, indexBlockHeader);

        var blockHeadersToRead = blockHeaders.items;
        while (blockHeadersToRead.len > 0) {
            n = std.sort.lowerBound(BlockHeader, blockHeadersToRead, sid, blockHeaderSidLowerBoundOrder);
            blockHeadersToRead = blockHeadersToRead[n..];

            while (blockHeadersToRead.len > 0 and blockHeadersToRead[0].sid.eql(&sid)) {
                const blockHeader = blockHeadersToRead[0];
                blockHeadersToRead = blockHeadersToRead[1..];

                if (query.start > blockHeader.timestampsHeader.max or query.end < blockHeader.timestampsHeader.min) {
                    // block doesn't contain the requested range
                    continue;
                }

                try self.queryBlock(io, alloc, dst, blockHeader, query);
            }

            if (blockHeadersToRead.len == 0) {
                break;
            }

            sid = blockHeadersToRead[0].sid;
            n = std.sort.lowerBound(SID, sidsToFind, sid, SID.order);
            if (n == sidsToFind.len) {
                sidsToFind = sidsToFind[0..0];
                break;
            }

            sid = sidsToFind[n];
            sidsToFind = sidsToFind[n..];
        }
    }
}

fn queryBlock(
    self: *const Table,
    io: Io,
    alloc: Allocator,
    dst: *std.ArrayList(Line),
    blockHeader: BlockHeader,
    query: Query,
) !void {
    var colIdx = std.AutoHashMap(u16, u16).init(alloc);
    defer colIdx.deinit();

    try colIdx.ensureTotalCapacity(self.columnIdxs.count());
    var idxIt = self.columnIdxs.iterator();
    while (idxIt.next()) |entry| {
        const colID = self.columnIDGen.keyIDs.get(entry.key_ptr.*) orelse continue;
        colIdx.putAssumeCapacity(colID, entry.value_ptr.*);
    }

    const streamReader: *StreamReader = try .init(io, alloc, self);
    defer streamReader.deinit(alloc);

    var blockData = BlockData.initEmpty();
    defer blockData.deinit(alloc);
    try blockData.readFrom(alloc, &blockHeader, streamReader);

    const unpacker = try Unpacker.init(alloc);
    defer unpacker.deinit(alloc);
    const decoder = try ValuesDecoder.init(alloc);
    defer decoder.deinit();

    const block = try Block.initFromData(io, alloc, &blockData, unpacker, decoder);
    defer block.deinit(alloc);

    var i = dst.items.len;
    try block.gatherLines(alloc, dst);
    var converted: usize = 0;
    errdefer {
        for (dst.items[i .. i + converted]) |line| freeFields(alloc, line.fields);
        for (dst.items[i + converted ..]) |line| alloc.free(line.fields);
        dst.shrinkRetainingCapacity(i);
    }
    for (dst.items[i..]) |*line| {
        const gatheredFields = line.fields;
        const copiedFields = try copyFields(alloc, gatheredFields);
        alloc.free(gatheredFields);
        line.fields = copiedFields;
        converted += 1;
    }

    std.debug.assert(dst.items.len > i);
    while (i < dst.items.len) {
        const line = dst.items[i];

        if (line.timestampNs < query.start or line.timestampNs > query.end) {
            const removed = dst.swapRemove(i);
            freeFields(alloc, removed.fields);
            continue;
        }
        if (query.fieldsExpr) |expr| {
            if (!try matchesFilterExpression(line.fields, expr)) {
                const removed = dst.swapRemove(i);
                freeFields(alloc, removed.fields);
                continue;
            }
        }

        dst.items[i].sid = .{
            .tenantID = blockHeader.sid.tenantID,
            .id = blockHeader.sid.id,
        };
        i += 1;
    }
}

fn matchesFilterExpression(fields: []const Field, expr: *const Query.FilterExpression) !bool {
    return switch (expr.*) {
        .predicate => |p| matchesPredicate(fields, p),
        .andOp => |ops| (try matchesFilterExpression(fields, ops[0])) and (try matchesFilterExpression(fields, ops[1])),
        .orOp => |ops| (try matchesFilterExpression(fields, ops[0])) or (try matchesFilterExpression(fields, ops[1])),
    };
}

fn matchesPredicate(fields: []const Field, p: Query.FilterPredicate) !bool {
    switch (p.op) {
        .equal => {
            for (fields) |f| {
                if (std.mem.eql(u8, f.key, p.key) and std.mem.eql(u8, f.value, p.value))
                    return true;
            }
            return false;
        },
        .notEqual => {
            for (fields) |f| {
                if (std.mem.eql(u8, f.key, p.key) and !std.mem.eql(u8, f.value, p.value))
                    return true;
            }
            return false;
        },
        else => return error.QueryMatchOperationNotImplemented,
    }
}

fn indexBlockHeaderSidLowerBoundOrder(ctx: SID, self: IndexBlockHeader) std.math.Order {
    return ctx.order(self.sid);
}

fn blockHeaderSidLowerBoundOrder(ctx: SID, bh: BlockHeader) std.math.Order {
    return ctx.order(bh.sid);
}

pub fn lessThan(_: void, one: *Table, another: *Table) bool {
    return one.size < another.size;
}

const testing = std.testing;

test "release keeps table unless toRemove is set, then removes table dir" {
    const alloc = testing.allocator;
    const io = testing.io;

    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    const rootPath = try tmp.dir.realPathFileAlloc(io, ".", alloc);
    defer alloc.free(rootPath);
    const tablePath = try std.fs.path.join(alloc, &.{ rootPath, "table-1" });
    defer alloc.free(tablePath);

    const memTable = try MemTable.init(alloc);
    defer memTable.deinit(alloc);

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
        .sid = .{ .id = 1, .tenantID = 1234 },
        .fields = fields1[0..],
    };
    const line2 = Line{
        .timestampNs = 2,
        .sid = .{ .id = 1, .tenantID = 1234 },
        .fields = fields2[0..],
    };

    var lines = [_]Line{ line1, line2 };
    try memTable.addLines(io, alloc, lines[0..]);
    try memTable.storeToDisk(io, alloc, tablePath);

    const table1Path = try alloc.dupe(u8, tablePath);
    const table1 = try Table.open(io, alloc, table1Path);
    table1.release(io);
    try Dir.accessAbsolute(io, tablePath, .{});

    const table2Path = try alloc.dupe(u8, tablePath);
    const table2 = try Table.open(io, alloc, table2Path);
    table2.toRemove.store(true, .release);
    table2.release(io);
    try testing.expectError(error.FileNotFound, Dir.accessAbsolute(io, tablePath, .{}));
}

test "release fromMem does not affect filesystem path" {
    const alloc = testing.allocator;
    const io = testing.io;

    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    const rootPath = try tmp.dir.realPathFileAlloc(io, ".", alloc);
    defer alloc.free(rootPath);
    const sentinelPath = try std.fs.path.join(alloc, &.{ rootPath, "sentinel" });
    defer alloc.free(sentinelPath);
    // create a real directory to verify it remains
    try testing.expectError(error.FileNotFound, Dir.accessAbsolute(io, sentinelPath, .{}));
    try Dir.createDirAbsolute(io, sentinelPath, .default_dir);

    const memTable = try MemTable.init(alloc);

    const table = try Table.fromMem(alloc, memTable);
    // eve if set we expect it to keep the created path when disk == null,
    // so close must not delete it
    table.toRemove.store(true, .release);
    // we expected only second release close cleans the table, otherwise it's a memory leak
    table.retain();

    try Dir.accessAbsolute(io, sentinelPath, .{});
    table.release(io);
    try Dir.accessAbsolute(io, sentinelPath, .{});
    table.release(io);
    try Dir.accessAbsolute(io, sentinelPath, .{});
}

const Field = @import("../lines.zig").Field;

fn deinitQueriedLines(alloc: Allocator, lines: *std.ArrayList(Line)) void {
    for (lines.items) |line| {
        freeFields(alloc, line.fields);
    }
    lines.deinit(alloc);
}

test "fromMem creates proper table from mem table with populated data" {
    const alloc = testing.allocator;
    const io = testing.io;
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
        .sid = .{ .id = 1, .tenantID = 1234 },
        .fields = fields1[0..],
    };
    const line2 = Line{
        .timestampNs = 2,
        .sid = .{ .id = 1, .tenantID = 1234 },
        .fields = fields2[0..],
    };

    var lines = [_]Line{ line1, line2 };

    try memTable.addLines(io, alloc, lines[0..]);

    const table = try Table.fromMem(alloc, memTable);
    defer table.release(io);

    var indexBuf: [128]u8 = undefined;
    const indexN = try table.readIndex(io, &indexBuf, 0);
    var columnsHeaderIndexBuf: [128]u8 = undefined;
    const columnsHeaderIndexN = try table.readColumnsHeaderIndex(io, &columnsHeaderIndexBuf, 0);
    var columnsHeaderBuf: [128]u8 = undefined;
    const columnsHeaderN = try table.readColumnsHeader(io, &columnsHeaderBuf, 0);
    var timestampsBuf: [128]u8 = undefined;
    const timestampsN = try table.readTimestamps(io, &timestampsBuf, 0);

    try testing.expect(indexBuf[0..indexN].len > 0);
    try testing.expect(columnsHeaderIndexBuf[0..columnsHeaderIndexN].len > 0);
    try testing.expect(columnsHeaderBuf[0..columnsHeaderN].len > 0);
    try testing.expect(timestampsBuf[0..timestampsN].len > 0);

    // if "info" or "seq" is added, the bloom values should be generated
    try testing.expect(table.inner.mem.bloomValuesBuf.items.len > 0);

    try testing.expect(table.columnIdxs.count() > 0);
    try testing.expect(table.columnIDGen.keyIDs.count() > 0);

    try testing.expectEqual(memTable.indexBuf.items.len, indexBuf[0..indexN].len);
    try testing.expectEqual(memTable.columnsHeaderIndexBuf.items.len, columnsHeaderIndexBuf[0..columnsHeaderIndexN].len);
    try testing.expectEqual(memTable.columnsHeaderBuf.items.len, columnsHeaderBuf[0..columnsHeaderN].len);
    try testing.expectEqual(memTable.timestampsBuf.items.len, timestampsBuf[0..timestampsN].len);
}

test "open reads table from disk" {
    const alloc = testing.allocator;
    const io = testing.io;

    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    const rootPath = try tmp.dir.realPathFileAlloc(io, ".", alloc);
    defer alloc.free(rootPath);
    const tablePath = try std.fs.path.join(alloc, &.{ rootPath, "table-1" });
    defer alloc.free(tablePath);

    const memTable = try MemTable.init(alloc);
    defer memTable.deinit(alloc);

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
        .sid = .{ .id = 1, .tenantID = 1234 },
        .fields = fields1[0..],
    };
    const line2 = Line{
        .timestampNs = 2,
        .sid = .{ .id = 1, .tenantID = 1234 },
        .fields = fields2[0..],
    };

    var lines = [_]Line{ line1, line2 };
    try memTable.addLines(io, alloc, lines[0..]);
    try memTable.storeToDisk(io, alloc, tablePath);

    const tablePathOwned = try alloc.dupe(u8, tablePath);
    const table = try Table.open(io, alloc, tablePathOwned);
    defer table.release(io);

    try testing.expect(table.inner == .disk);

    var buf: [128]u8 = undefined;
    var n = try table.readIndex(io, buf[0..memTable.indexBuf.items.len], 0);
    try testing.expectEqual(memTable.indexBuf.items.len, n);
    try testing.expectEqualSlices(u8, memTable.indexBuf.items, buf[0..n]);

    n = try table.readColumnsHeaderIndex(io, buf[0..memTable.columnsHeaderIndexBuf.items.len], 0);
    try testing.expectEqual(memTable.columnsHeaderIndexBuf.items.len, n);
    try testing.expectEqualSlices(u8, memTable.columnsHeaderIndexBuf.items, buf[0..n]);

    n = try table.readColumnsHeader(io, buf[0..memTable.columnsHeaderBuf.items.len], 0);
    try testing.expectEqual(memTable.columnsHeaderBuf.items.len, n);
    try testing.expectEqualSlices(u8, memTable.columnsHeaderBuf.items, buf[0..n]);

    n = try table.readTimestamps(io, buf[0..memTable.timestampsBuf.items.len], 0);
    try testing.expectEqual(memTable.timestampsBuf.items.len, n);
    try testing.expectEqualSlices(u8, memTable.timestampsBuf.items, buf[0..n]);

    n = try table.readMessageBloomTokens(io, buf[0..memTable.messageBloomTokensBuf.items.len], 0);
    try testing.expectEqual(memTable.messageBloomTokensBuf.items.len, n);
    try testing.expectEqualSlices(u8, memTable.messageBloomTokensBuf.items, buf[0..n]);

    n = try table.readMessageBloomValues(io, buf[0..memTable.messageBloomValuesBuf.items.len], 0);
    try testing.expectEqual(memTable.messageBloomValuesBuf.items.len, n);
    try testing.expectEqualSlices(u8, memTable.messageBloomValuesBuf.items, buf[0..n]);

    try testing.expectEqual(
        memTable.tableHeader.bloomValuesBuffersAmount,
        table.tableHeader().bloomValuesBuffersAmount,
    );
    try testing.expectEqual(@as(usize, 1), table.tableHeader().bloomValuesBuffersAmount);

    n = try table.readBloomTokens(io, buf[0..memTable.bloomTokensBuf.items.len], 0, 0);
    try testing.expectEqual(memTable.bloomTokensBuf.items.len, n);
    try testing.expectEqualSlices(u8, memTable.bloomTokensBuf.items, buf[0..n]);

    n = try table.readBloomValues(io, buf[0..memTable.bloomValuesBuf.items.len], 0, 0);
    try testing.expectEqual(memTable.bloomValuesBuf.items.len, n);
    try testing.expectEqualSlices(u8, memTable.bloomValuesBuf.items, buf[0..n]);

    try testing.expect(table.columnIDGen.keyIDs.count() > 0);
    try testing.expect(table.columnIdxs.count() > 0);

    const expectedHeaders = try IndexBlockHeader.readIndexBlockHeaders(
        alloc,
        memTable.metaIndexBuf.items,
    );
    defer if (expectedHeaders.len > 0) alloc.free(expectedHeaders);
    try testing.expectEqual(expectedHeaders.len, table.indexBlockHeaders.len);
    for (expectedHeaders, table.indexBlockHeaders) |expected, actual| {
        try testing.expectEqualDeep(expected, actual);
    }
}

test "queryLines" {
    const alloc = testing.allocator;
    const io = testing.io;
    const ExpectedLine = struct {
        timestampNs: u64,
        sid: SID,
    };
    const Case = struct {
        requestedSIDs: []const SID,
        query: Query,
        expected: []const ExpectedLine,
    };

    const sidBlock = SID{ .id = 10, .tenantID = 1234 };
    const sid1 = SID{ .id = 1, .tenantID = 1234 };
    const sid2 = SID{ .id = 2, .tenantID = 1234 };
    const sid3 = SID{ .id = 3, .tenantID = 1234 };
    const sid5 = SID{ .id = 5, .tenantID = 1234 };
    const sidMissing = SID{ .id = 4, .tenantID = 1234 };
    const sidTenantA = SID{ .id = 1, .tenantID = 1111 };
    const sidTenantB = SID{ .id = 1, .tenantID = 2222 };

    var seqInfo = [_]Field{ .{ .key = "app", .value = "seq" }, .{ .key = "level", .value = "info" } };
    var seqWarn = [_]Field{ .{ .key = "app", .value = "seq" }, .{ .key = "level", .value = "warn" } };
    var seqError = [_]Field{ .{ .key = "app", .value = "seq" }, .{ .key = "level", .value = "error" } };
    var apiInfo = [_]Field{ .{ .key = "app", .value = "api" }, .{ .key = "level", .value = "info" } };
    var apiErrorA = [_]Field{ .{ .key = "app", .value = "api" }, .{ .key = "level", .value = "error" } };
    var apiErrorB = [_]Field{ .{ .key = "app", .value = "api" }, .{ .key = "level", .value = "error" } };
    var workerInfo = [_]Field{ .{ .key = "app", .value = "worker" }, .{ .key = "level", .value = "info" } };
    var tenantAFields = [_]Field{ .{ .key = "app", .value = "api-a" }, .{ .key = "level", .value = "info" } };
    var tenantBFields = [_]Field{ .{ .key = "app", .value = "api-b" }, .{ .key = "level", .value = "info" } };

    var lines = [_]Line{
        .{ .timestampNs = 1, .sid = sidBlock, .fields = seqInfo[0..] },
        .{ .timestampNs = 2, .sid = sidBlock, .fields = seqWarn[0..] },
        .{ .timestampNs = 3, .sid = sidBlock, .fields = seqError[0..] },
        .{ .timestampNs = 1, .sid = sid1, .fields = seqInfo[0..] },
        .{ .timestampNs = 2, .sid = sid1, .fields = seqError[0..] },
        .{ .timestampNs = 3, .sid = sid2, .fields = apiErrorA[0..] },
        .{ .timestampNs = 6, .sid = sid2, .fields = apiErrorB[0..] },
        .{ .timestampNs = 3, .sid = sid3, .fields = apiInfo[0..] },
        .{ .timestampNs = 10, .sid = sid5, .fields = workerInfo[0..] },
        .{ .timestampNs = 1, .sid = sidTenantA, .fields = tenantAFields[0..] },
        .{ .timestampNs = 2, .sid = sidTenantB, .fields = tenantBFields[0..] },
    };

    const memTable = try MemTable.init(alloc);
    try memTable.addLines(io, alloc, lines[0..]);

    const table = try Table.fromMem(alloc, memTable);
    defer table.release(io);

    const noTagsExpr: Query.FilterExpression = .{ .predicate = .{ .key = "", .value = "", .op = .equal } };
    const warnExpr: Query.FilterExpression = .{ .predicate = .{ .key = "level", .value = "warn", .op = .equal } };
    const fatalExpr: Query.FilterExpression = .{ .predicate = .{ .key = "level", .value = "fatal", .op = .equal } };
    const errorExpr: Query.FilterExpression = .{ .predicate = .{ .key = "level", .value = "error", .op = .equal } };

    const cases = [_]Case{
        .{
            .requestedSIDs = &.{sidBlock},
            .query = .{ .start = 1, .end = 3, .tagsExpr = &noTagsExpr, .fieldsExpr = null },
            .expected = &.{
                .{ .timestampNs = 1, .sid = sidBlock },
                .{ .timestampNs = 2, .sid = sidBlock },
                .{ .timestampNs = 3, .sid = sidBlock },
            },
        },
        .{
            .requestedSIDs = &.{sidBlock},
            .query = .{ .start = 1, .end = 3, .tagsExpr = &noTagsExpr, .fieldsExpr = &warnExpr },
            .expected = &.{.{ .timestampNs = 2, .sid = sidBlock }},
        },
        .{
            .requestedSIDs = &.{sidBlock},
            .query = .{ .start = 1, .end = 3, .tagsExpr = &noTagsExpr, .fieldsExpr = &fatalExpr },
            .expected = &.{},
        },
        .{
            .requestedSIDs = &.{sidBlock},
            .query = .{ .start = 10, .end = 20, .tagsExpr = &noTagsExpr, .fieldsExpr = null },
            .expected = &.{},
        },
        .{
            .requestedSIDs = &.{ sidMissing, sid5 },
            .query = .{ .start = 0, .end = 20, .tagsExpr = &noTagsExpr, .fieldsExpr = null },
            .expected = &.{.{ .timestampNs = 10, .sid = sid5 }},
        },
        .{
            .requestedSIDs = &.{ sid1, sid2 },
            .query = .{ .start = 2, .end = 5, .tagsExpr = &noTagsExpr, .fieldsExpr = &errorExpr },
            .expected = &.{
                .{ .timestampNs = 2, .sid = sid1 },
                .{ .timestampNs = 3, .sid = sid2 },
            },
        },
        .{
            .requestedSIDs = &.{ sid1, sid1 },
            .query = .{ .start = 0, .end = 10, .tagsExpr = &noTagsExpr, .fieldsExpr = null },
            .expected = &.{
                .{ .timestampNs = 1, .sid = sid1 },
                .{ .timestampNs = 2, .sid = sid1 },
            },
        },
        .{
            .requestedSIDs = &.{sidTenantB},
            .query = .{ .start = 0, .end = 10, .tagsExpr = &noTagsExpr, .fieldsExpr = null },
            .expected = &.{.{ .timestampNs = 2, .sid = sidTenantB }},
        },
        .{
            .requestedSIDs = &.{ sidMissing, sid5 },
            .query = .{ .start = 0, .end = 9, .tagsExpr = &noTagsExpr, .fieldsExpr = null },
            .expected = &.{},
        },
        .{
            .requestedSIDs = &.{},
            .query = .{ .start = 0, .end = 10, .tagsExpr = &noTagsExpr, .fieldsExpr = null },
            .expected = &.{},
        },
    };

    var queried = std.ArrayList(Line).empty;
    defer deinitQueriedLines(alloc, &queried);

    for (cases) |case| {
        for (queried.items) |line| {
            freeFields(alloc, line.fields);
        }
        queried.clearRetainingCapacity();

        const requested = try alloc.dupe(SID, case.requestedSIDs);
        defer alloc.free(requested);

        try table.queryLines(io, alloc, &queried, requested, case.query);
        try testing.expectEqual(case.expected.len, queried.items.len);

        for (case.expected, 0..) |expected, i| {
            try testing.expectEqual(expected.timestampNs, queried.items[i].timestampNs);
            try testing.expect(queried.items[i].sid.eql(&expected.sid));
        }
    }
}

// TODO: test flushed mem table is the same as an opened one,
// kinda round trippness property,
// and do the same with index tables (probably already done)

test "queryLinesReproducerWhenMixedEmptyKeyAndNonEmptyKey" {
    const alloc = std.testing.allocator;
    const io: Io = std.testing.io;

    const sid = SID{ .id = 1, .tenantID = 1234 };

    var fields1 = [_]Field{
        .{ .key = "", .value = "message-1" },
        .{ .key = "level", .value = "info" },
    };
    var fields2 = [_]Field{
        .{ .key = "", .value = "message-2" },
        .{ .key = "level", .value = "warn" },
    };
    var lines = [_]Line{
        .{ .timestampNs = 1, .sid = sid, .fields = fields1[0..] },
        .{ .timestampNs = 2, .sid = sid, .fields = fields2[0..] },
    };

    const memTable = try MemTable.init(alloc);
    try memTable.addLines(io, alloc, lines[0..]);

    const table = try Table.fromMem(alloc, memTable);
    defer table.release(io);

    var queried = std.ArrayList(Line).empty;
    defer {
        for (queried.items) |line| {
            freeFields(alloc, line.fields);
        }
        queried.deinit(alloc);
    }

    const noTagsExpr: Query.FilterExpression = .{ .predicate = .{ .key = "", .value = "", .op = .equal } };
    const query = Query{ .start = 0, .end = 10, .tagsExpr = &noTagsExpr, .fieldsExpr = null };
    var requested = [_]SID{sid};

    try table.queryLines(io, alloc, &queried, requested[0..], query);
}

test "queryLines reads disk table fields after open" {
    const alloc = testing.allocator;
    const io = testing.io;

    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    const rootPath = try tmp.dir.realPathFileAlloc(io, ".", alloc);
    defer alloc.free(rootPath);
    const tablePath = try std.fs.path.join(alloc, &.{ rootPath, "table" });
    defer alloc.free(tablePath);

    const sid = SID{ .id = 1, .tenantID = 11 };
    var fields1 = [_]Field{
        .{ .key = "", .value = "GET /api/health 200 3ms" },
        .{ .key = "id", .value = "web-001" },
        .{ .key = "status", .value = "200" },
        .{ .key = "path", .value = "/api/health" },
    };
    var fields2 = [_]Field{
        .{ .key = "", .value = "POST /api/orders 500 timeout" },
        .{ .key = "id", .value = "web-002" },
        .{ .key = "status", .value = "500" },
        .{ .key = "path", .value = "/api/orders" },
    };
    var lines = [_]Line{
        .{ .timestampNs = 1, .sid = sid, .fields = fields1[0..] },
        .{ .timestampNs = 2, .sid = sid, .fields = fields2[0..] },
    };

    const memTable = try MemTable.init(alloc);
    defer memTable.deinit(alloc);
    try memTable.addLines(io, alloc, lines[0..]);
    try memTable.storeToDisk(io, alloc, tablePath);

    const tablePathOwned = try alloc.dupe(u8, tablePath);
    const table = try Table.open(io, alloc, tablePathOwned);
    defer table.release(io);

    var queried = std.ArrayList(Line).empty;
    defer deinitQueriedLines(alloc, &queried);

    const noTagsExpr: Query.FilterExpression = .{ .predicate = .{ .key = "", .value = "", .op = .equal } };
    const statusExpr: Query.FilterExpression = .{ .predicate = .{ .key = "status", .value = "200", .op = .equal } };
    const query = Query{ .start = 0, .end = 10, .tagsExpr = &noTagsExpr, .fieldsExpr = &statusExpr };
    var requested = [_]SID{sid};

    try table.queryLines(io, alloc, &queried, requested[0..], query);
    try testing.expectEqual(@as(usize, 1), queried.items.len);
    try testing.expectEqual(@as(u64, 1), queried.items[0].timestampNs);
    try testing.expectEqualSlices(u8, "id", queried.items[0].fields[1].key);
    try testing.expectEqualSlices(u8, "web-001", queried.items[0].fields[1].value);
}
