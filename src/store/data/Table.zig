const std = @import("std");
const Allocator = std.mem.Allocator;
const Io = std.Io;
const Dir = Io.Dir;

const Logger = @import("logging");
const filenames = @import("../../filenames.zig");
const fs = @import("../../fs.zig");
const MemTable = @import("../data/MemTable.zig");
const DiskTable = @import("DiskTable.zig");
const IndexBlockHeader = @import("../data/IndexBlockHeader.zig");
const BlockHeader = @import("../data/BlockHeader.zig");
const TableHeader = @import("../data/TableHeader.zig");
const ColumnIDGen = @import("../data/ColumnIDGen.zig");
const BlockData = @import("../data/BlockData.zig").BlockData;
const Block = @import("../data/Block.zig");
const Unpacker = @import("../data/Unpacker.zig").Unpacker;
const ValuesDecoder = @import("../data/ValuesDecoder.zig");
const TableReader = @import("../data/TableReader.zig");
const TimestampsEncoder = @import("../data/TimestampsEncoder.zig");
const DecompressionPool = @import("../compression/DecompressionPool.zig");

const Line = @import("../lines.zig").Line;
const Field = @import("../lines.zig").Field;
const msgKey = @import("../lines.zig").msgKey;
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
// TODO: define an Arc object to move all the ref counting to it's type
refCounter: std.atomic.Value(u32),

// TODO: investigate how we could make a checksum and validate it on opening a table
pub fn openAll(
    io: Io,
    parentAlloc: Allocator,
    path: []const u8,
    decompressionPool: *DecompressionPool,
) !std.ArrayList(*Table) {
    Dir.createDirAbsolute(io, path, .default_dir) catch |err| switch (err) {
        // TODO: if the folder already exists we must read it's content and log an error
        // in case the tables on the disk are missing in the tables list
        Dir.CreateDirError.PathAlreadyExists => {},
        else => |e| return e,
    };

    var fba = std.heap.stackFallback(2048, parentAlloc);
    const alloc = fba.get();

    // read table names,
    // they are given either from a file or listed directories in the path
    var tablesFilePathBuf: [std.fs.max_path_bytes]u8 = undefined;
    var tablesFilePathWriter = std.Io.Writer.fixed(&tablesFilePathBuf);
    try std.fs.path.fmtJoin(&.{ path, filenames.tables }).format(&tablesFilePathWriter);
    var tableNames = try catalog.readNames(io, alloc, tablesFilePathWriter.buffered(), true);
    defer {
        for (tableNames.items) |tableName| alloc.free(tableName);
        tableNames.deinit(alloc);
    }

    // syncing tables with a json, make sure all the listed dirs exist
    try catalog.validateTablesExist(io, path, tableNames.items);

    // syncing tables with the given names remove all the not listed dirs
    try catalog.removeUnusedTables(io, path, tableNames.items);

    // open tables
    var tables = try std.ArrayList(*Table).initCapacity(parentAlloc, tableNames.items.len);
    errdefer {
        for (tables.items) |table| table.close(io);
        tables.deinit(parentAlloc);
    }
    for (tableNames.items) |tableName| {
        // don't clean tablePath, Table owns it
        const tablePath = try std.fs.path.join(parentAlloc, &.{ path, tableName });
        errdefer parentAlloc.free(tablePath);
        const table = try Table.open(io, parentAlloc, tablePath, decompressionPool);
        tables.appendAssumeCapacity(table);
    }

    // fsync after opening tables because it creates the files
    try fs.syncPathAndParentDir(io, path);

    return tables;
}

pub fn open(io: Io, alloc: Allocator, path: []const u8, decompressionPool: *DecompressionPool) !*Table {
    var pathBuf: [std.fs.max_path_bytes]u8 = undefined;
    var pathWriter = std.Io.Writer.fixed(&pathBuf);

    const header = try TableHeader.readFile(io, path);

    try std.fs.path.fmtJoin(&.{ path, filenames.columnKeys }).format(&pathWriter);
    var columnIDGen = try ColumnIDGen.decodeFile(io, alloc, decompressionPool, pathWriter.buffered());
    errdefer columnIDGen.deinit(alloc);
    pathWriter.end = 0;

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
        indexBlockHeaders = try IndexBlockHeader.readIndexBlockHeaders(io, alloc, decompressionPool, metaindexContent);
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
        const bloomTokensFile = try std.Io.Dir.openFileAbsolute(io, bloomTokensPath, .{});
        errdefer bloomTokensFile.close(io);

        const bloomValuesPath = try filenames.writeBloomFilePath(
            &pathBuf,
            path,
            filenames.bloomValues,
            @intCast(shardIdx),
        );
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

pub fn fromMem(io: Io, alloc: Allocator, memTable: *MemTable, decompressionPool: *DecompressionPool) !*Table {
    std.debug.assert(memTable.size() == memTable.tableHeader.compressedSize);

    // TODO: move ownership of the original meta index to the table, not only the buffers,
    // but it requires index collecting during ingestion
    var indexBlockHeaders: []IndexBlockHeader = &.{};
    const metaIndexBuf = memTable.metaIndexBuf.items;
    if (metaIndexBuf.len > 0) {
        indexBlockHeaders = try IndexBlockHeader.readIndexBlockHeaders(io, alloc, decompressionPool, metaIndexBuf);
    }
    errdefer if (indexBlockHeaders.len > 0) alloc.free(indexBlockHeaders);

    // TODO: avoid decoding column ids, we can simply assign what we have from the stream writer
    const columnIDGen = blk: {
        if (memTable.columnKeysBuf.items.len > 0) {
            break :blk try ColumnIDGen.decode(io, alloc, decompressionPool, memTable.columnKeysBuf.items);
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

    var tablesFilePathBuf: [std.fs.max_path_bytes]u8 = undefined;
    var tablesFilePathWriter = std.Io.Writer.fixed(&tablesFilePathBuf);
    try std.fs.path.fmtJoin(&.{ path, filenames.tables }).format(&tablesFilePathWriter);

    try fs.writeBufferToFileAtomic(io, tablesFilePathWriter.buffered(), data, true);
}

pub fn retain(self: *Table) void {
    _ = self.refCounter.fetchAdd(1, .monotonic);
}

// Table is a managed object, so it does not accept an allocator,
// because the allocator is in a read path is (arena) not the same as in a write path which
// which created that table
// TODO: find how we can explicitly carry an allocator
pub fn release(self: *Table, io: Io) void {
    // TODO: on older intel CPUs it must be more efficient, benchmark:
    // ~ObjectPtr() {
    //   if (1 == p->count.load(std::memory_order_acquire) ||
    //       1 == p->count.fetch_sub(1, std::memory_order_acq_rel)) {
    //     delete p;
    //   }
    // }
    // or even better implementation: https://github.com/gcc-mirror/gcc/commit/dbf8bd3c2f2cd2d27ca4f0fe379bd9490273c6d7
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

pub fn queryLines(
    self: *Table,
    io: Io,
    alloc: Allocator,
    comptime leakyUnpacking: bool,
    timestampsEncoders: *TimestampsEncoder.TimestampsEncoderPool,
    decompressionPool: *DecompressionPool,
    dst: *std.ArrayList(Line),
    sids: []SID,
    query: Query,
) !void {
    if (sids.len == 0 and query.tagsExpr == null and query.streamIDs == null) {
        return self.queryLinesAllBlocks(io, alloc, leakyUnpacking, timestampsEncoders, decompressionPool, dst, query);
    }

    var indexBlockHeaders = self.indexBlockHeaders;
    var sidsToFind = sids;

    while (indexBlockHeaders.len > 0 and sidsToFind.len > 0) {
        var sid = sidsToFind[0];
        var indexBlockHeader = indexBlockHeaders[0];

        if (sid.lessThan(indexBlockHeader.sid)) {
            // minimum value in the block is higher, so we bump it to the first given in the block
            sid = indexBlockHeader.sid;
            const n = std.sort.lowerBound(SID, sidsToFind, indexBlockHeader.sid, SID.order);
            if (n == sidsToFind.len) {
                sidsToFind = sidsToFind[0..0];
                break;
            }

            sid = sidsToFind[n];
            sidsToFind = sidsToFind[n..];
        }

        var n: usize = 0;
        if (indexBlockHeaders[0].sid.lessThan(sid)) {
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
        const indexBuffer = try alloc.alloc(u8, indexBlockHeader.size);
        defer alloc.free(indexBuffer);
        n = try self.readIndex(io, indexBuffer, indexBlockHeader.offset);
        std.debug.assert(indexBuffer.len == n);
        try BlockHeader.decodeIndexWindow(io, alloc, decompressionPool, &blockHeaders, indexBuffer, indexBlockHeader);

        var blockHeadersToRead = blockHeaders.items;
        while (blockHeadersToRead.len > 0) {
            n = std.sort.lowerBound(BlockHeader, blockHeadersToRead, sid, blockHeaderSidLowerBoundOrder);
            blockHeadersToRead = blockHeadersToRead[n..];

            while (blockHeadersToRead.len > 0 and blockHeadersToRead[0].sid.eql(sid)) {
                const blockHeader = blockHeadersToRead[0];
                blockHeadersToRead = blockHeadersToRead[1..];

                if (query.start > blockHeader.timestampsHeader.max or query.end < blockHeader.timestampsHeader.min) {
                    // block doesn't contain the requested range
                    continue;
                }

                try self.queryBlock(
                    io,
                    alloc,
                    leakyUnpacking,
                    timestampsEncoders,
                    decompressionPool,
                    dst,
                    blockHeader,
                    query,
                );
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

fn queryLinesAllBlocks(
    self: *Table,
    io: Io,
    alloc: Allocator,
    comptime leakyUnpacking: bool,
    timestampsEncoders: *TimestampsEncoder.TimestampsEncoderPool,
    decompressionPool: *DecompressionPool,
    dst: *std.ArrayList(Line),
    query: Query,
) !void {
    for (self.indexBlockHeaders) |indexBlockHeader| {
        if (query.start > indexBlockHeader.maxTs or query.end < indexBlockHeader.minTs) {
            continue;
        }

        var blockHeaders = std.ArrayList(BlockHeader).empty;
        defer blockHeaders.deinit(alloc);
        const indexBuffer = try alloc.alloc(u8, indexBlockHeader.size);
        defer alloc.free(indexBuffer);
        const n = try self.readIndex(io, indexBuffer, indexBlockHeader.offset);
        std.debug.assert(indexBuffer.len == n);
        try BlockHeader.decodeIndexWindow(io, alloc, decompressionPool, &blockHeaders, indexBuffer, indexBlockHeader);

        // TODO: research apache fusion query pushdown for almost sorted data,
        // it's our case when the blocks may intersect, but the sorting is "Inexact":
        // https://datafusion.apache.org/blog/2026/07/20/sort-pushdown/
        for (blockHeaders.items) |blockHeader| {
            if (query.start > blockHeader.timestampsHeader.max or query.end < blockHeader.timestampsHeader.min) {
                continue;
            }

            _ = @import("../query/BlockQuery.zig");

            try self.queryBlock(
                io,
                alloc,
                leakyUnpacking,
                timestampsEncoders,
                decompressionPool,
                dst,
                blockHeader,
                query,
            );
        }
    }
}

fn queryBlock(
    self: *const Table,
    io: Io,
    alloc: Allocator,
    comptime leakyUnpacking: bool,
    timestampsEncoders: *TimestampsEncoder.TimestampsEncoderPool,
    decompressionPool: *DecompressionPool,
    dst: *std.ArrayList(Line),
    blockHeader: BlockHeader,
    query: Query,
) !void {
    errdefer |err| {
        Logger.log(.err, "failed to query a block", .{
            .err = err,
            .table = self.path,
            .columnsHeaderOffset = blockHeader.columnsHeaderOffset,
            .columnsHeaderSize = blockHeader.columnsHeaderSize,
            .columnsHeaderIndexOffset = blockHeader.columnsHeaderIndexOffset,
            .columnsHeaderIndexSize = blockHeader.columnsHeaderIndexSize,
            .timestampsOffset = blockHeader.timestampsHeader.offset,
            .timestampsSize = blockHeader.timestampsHeader.size,
        });
    }

    var colIdx = std.AutoHashMap(u16, u16).init(alloc);
    defer colIdx.deinit();

    try colIdx.ensureTotalCapacity(self.columnIdxs.count());
    var idxIt = self.columnIdxs.iterator();
    while (idxIt.next()) |entry| {
        const colID = self.columnIDGen.keyIDs.get(entry.key_ptr.*) orelse continue;
        colIdx.putAssumeCapacity(colID, entry.value_ptr.*);
    }

    const tableReader: *TableReader = try .init(io, alloc, self, decompressionPool);
    defer tableReader.deinit(alloc);

    var blockData = BlockData.initEmpty();
    try blockData.readFrom(io, alloc, &blockHeader, tableReader);
    defer blockData.deinit(alloc);

    // rely on request arena
    var unpacker = Unpacker(leakyUnpacking).init(decompressionPool);
    defer unpacker.deinit(alloc);
    var decoder: ValuesDecoder = .{};
    defer decoder.deinit(alloc);

    var block = try Block.initFromData(io, alloc, timestampsEncoders, &blockData, leakyUnpacking, &unpacker, &decoder);
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
    for (fields) |f| {
        // TODO: this is a demo of a broken ingestion design;
        // initially ingestion was designed around Loki API,
        // it has a <log line> and structured fields,
        // as a result <lig line> is threated as a special empty key: "",
        // to serve the the data as a json we made up its key as msgKey,
        // but then to filter and query such keys they clients query it as msgKey
        // and now we have to mutate it in place,
        if (std.mem.eql(u8, f.key, p.key) or (f.key.len == 0 and std.mem.eql(u8, msgKey, p.key))) {
            const res = switch (p.op) {
                .equal => std.mem.eql(u8, f.value, p.value),
                .notEqual => !std.mem.eql(u8, f.value, p.value),
                else => return error.QueryMatchOperationNotImplemented,
            };
            return res;
        }
    }

    return false;
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

test {
    _ = @import("Table_test.zig");
}
