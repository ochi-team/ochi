const std = @import("std");
const Allocator = std.mem.Allocator;
const Io = std.Io;
const Dir = Io.Dir;

const ValuesEncoder = @import("ValuesEncoder.zig");

const Block = @import("Block.zig");
const BlockData = @import("BlockData.zig").BlockData;
const TimestampsData = @import("BlockData.zig").TimestampsData;
const ColumnData = @import("BlockData.zig").ColumnData;
const maxTimestampsBlockSize = @import("BlockData.zig").maxTimestampsBlockSize;
const maxValuesBlockSize = @import("BlockData.zig").maxValuesBlockSize;
const maxBloomTokensBlockSize = @import("BlockData.zig").maxBloomTokensBlockSize;
const Column = @import("Column.zig");
const MemTable = @import("MemTable.zig");
const Table = @import("../data/Table.zig");
const filenames = @import("../../filenames.zig");
const fs = @import("../../fs.zig");
const BlockHeader = @import("BlockHeader.zig");
const TimestampsHeader = BlockHeader.TimestampsHeader;
const ColumnsHeader = @import("ColumnsHeader.zig");
const ColumnHeader = @import("ColumnHeader.zig");
const Packer = @import("Packer.zig");
const ColumnsHeaderIndex = @import("ColumnsHeaderIndex.zig");
const ColumnIDGen = @import("ColumnIDGen.zig");
const TimestampsEncoder = @import("TimestampsEncoder.zig");
const StreamDestination = @import("StreamDestination.zig").StreamDestination;
const HashTokenizer = @import("bloom.zig").HashTokenizer;
const encodeBloomHashes = @import("bloom.zig").encodeBloomHashes;
const encoding = @import("encoding");
const Encoder = encoding.Encoder;

const maxPackedValuesSize = 8 * 1024 * 1024;
const bloomValuesMaxShardsCount: u16 = 128;

pub const Error = error{
    EmptyTimestamps,
};

const TableWriter = @This();

timestampsDst: StreamDestination,
indexDst: StreamDestination,
metaindexDst: StreamDestination,

columnsHeaderDst: StreamDestination,
columnsHeaderIndexDst: StreamDestination,

columnKeysDst: StreamDestination,
// TODO: conider to get rid of that since we have the keys ordered
columnIdxsDst: StreamDestination,

messageBloomValuesDst: StreamDestination,
messageBloomTokensDst: StreamDestination,
bloomValuesList: std.ArrayList(StreamDestination),
bloomTokensList: std.ArrayList(StreamDestination),

columnIDGen: *ColumnIDGen,
columnKeyCopies: std.ArrayList([]u8),
colIdx: std.AutoHashMap(u16, u16),
nextColI: u16,
maxColI: u16,

timestampsEncoder: *TimestampsEncoder,

path: []const u8,
destinationBuffer: ?*std.ArrayList(u8) = null,

pub fn initMem(allocator: Allocator, memTable: *MemTable) !*TableWriter {
    const columnIDGen = try ColumnIDGen.init(allocator);
    errdefer columnIDGen.deinit(allocator);
    var colIdx = std.AutoHashMap(u16, u16).init(allocator);
    errdefer colIdx.deinit();

    const timestampsEncoder = try TimestampsEncoder.init(allocator);
    errdefer timestampsEncoder.deinit(allocator);

    const maxCols = 1;

    var bloomValuesList = try std.ArrayList(StreamDestination).initCapacity(allocator, maxCols);
    errdefer bloomValuesList.deinit(allocator);
    var bloomTokensList = try std.ArrayList(StreamDestination).initCapacity(allocator, maxCols);
    errdefer bloomTokensList.deinit(allocator);

    // mem table holds a single item in the list, so we append it in advance
    bloomValuesList.appendAssumeCapacity(StreamDestination.initBuffer(&memTable.bloomValuesBuf));
    bloomTokensList.appendAssumeCapacity(StreamDestination.initBuffer(&memTable.bloomTokensBuf));

    const w = try allocator.create(TableWriter);
    errdefer allocator.destroy(w);

    w.* = TableWriter{
        // it uses StreamDestination instead of Io.Writer since we need to close files in the end,
        // so we stil have to hold a link to file which creates a leaky abstraction,
        // therefore makes no sense to hold both File and Writer with a runtime cost
        .timestampsDst = StreamDestination.initBuffer(&memTable.timestampsBuf),
        .indexDst = StreamDestination.initBuffer(&memTable.indexBuf),
        .metaindexDst = StreamDestination.initBuffer(&memTable.metaIndexBuf),

        .columnsHeaderDst = StreamDestination.initBuffer(&memTable.columnsHeaderBuf),
        .columnsHeaderIndexDst = StreamDestination.initBuffer(&memTable.columnsHeaderIndexBuf),
        .columnKeysDst = StreamDestination.initBuffer(&memTable.columnKeysBuf),
        .columnIdxsDst = StreamDestination.initBuffer(&memTable.columnIdxsBuf),

        .messageBloomTokensDst = StreamDestination.initBuffer(&memTable.messageBloomTokensBuf),
        .messageBloomValuesDst = StreamDestination.initBuffer(&memTable.messageBloomValuesBuf),
        .bloomValuesList = bloomValuesList,
        .bloomTokensList = bloomTokensList,
        .columnIDGen = columnIDGen,
        .columnKeyCopies = .empty,
        .colIdx = colIdx,
        .nextColI = 0,
        .maxColI = maxCols,

        .timestampsEncoder = timestampsEncoder,
        // path is empty for mem table
        .path = "",
    };
    return w;
}

pub fn initDisk(io: Io, alloc: Allocator, path: []const u8, fitsInCache: bool) !*TableWriter {
    std.debug.assert(path.len != 0);

    // TODO: implement page cache support
    _ = fitsInCache;

    try fs.createDirAssert(io, path);

    // TODO: open files in parallel

    var pathBuf: [std.fs.max_path_bytes]u8 = undefined;
    var pathWriter = std.Io.Writer.fixed(&pathBuf);

    const destinationBuffer = try alloc.create(std.ArrayList(u8));
    destinationBuffer.* = .empty;
    errdefer {
        destinationBuffer.deinit(alloc);
        alloc.destroy(destinationBuffer);
    }

    try std.fs.path.fmtJoin(&.{ path, filenames.columnKeys }).format(&pathWriter);
    var columnKeysFile = try Dir.createFileAbsolute(io, pathWriter.buffered(), .{ .truncate = true, .read = true });
    errdefer columnKeysFile.close(io);
    const columnKeysDst = try StreamDestination.initFile(io, columnKeysFile, destinationBuffer);
    pathWriter.end = 0;

    try std.fs.path.fmtJoin(&.{ path, filenames.columnIdxs }).format(&pathWriter);
    var columnIdxsFile = try Dir.createFileAbsolute(io, pathWriter.buffered(), .{ .truncate = true, .read = true });
    errdefer columnIdxsFile.close(io);
    const columnIdxsDst = try StreamDestination.initFile(io, columnIdxsFile, destinationBuffer);
    pathWriter.end = 0;

    try std.fs.path.fmtJoin(&.{ path, filenames.metaindex }).format(&pathWriter);
    var metaindexFile = try Dir.createFileAbsolute(io, pathWriter.buffered(), .{ .truncate = true, .read = true });
    errdefer metaindexFile.close(io);
    const metaindexDst = try StreamDestination.initFile(io, metaindexFile, destinationBuffer);
    pathWriter.end = 0;

    try std.fs.path.fmtJoin(&.{ path, filenames.index }).format(&pathWriter);
    var indexFile = try Dir.createFileAbsolute(io, pathWriter.buffered(), .{ .truncate = true, .read = true });
    errdefer indexFile.close(io);
    const indexDst = try StreamDestination.initFile(io, indexFile, destinationBuffer);
    pathWriter.end = 0;

    try std.fs.path.fmtJoin(&.{ path, filenames.columnsHeaderIndex }).format(&pathWriter);
    var columnsHeaderIndexFile = try Dir.createFileAbsolute(io, pathWriter.buffered(), .{ .truncate = true, .read = true });
    errdefer columnsHeaderIndexFile.close(io);
    const columnsHeaderIndexDst = try StreamDestination.initFile(io, columnsHeaderIndexFile, destinationBuffer);
    pathWriter.end = 0;

    try std.fs.path.fmtJoin(&.{ path, filenames.columnsHeader }).format(&pathWriter);
    var columnsHeaderFile = try Dir.createFileAbsolute(io, pathWriter.buffered(), .{ .truncate = true, .read = true });
    errdefer columnsHeaderFile.close(io);
    const columnsHeaderDst = try StreamDestination.initFile(io, columnsHeaderFile, destinationBuffer);
    pathWriter.end = 0;

    try std.fs.path.fmtJoin(&.{ path, filenames.timestamps }).format(&pathWriter);
    var timestampsFile = try Dir.createFileAbsolute(io, pathWriter.buffered(), .{ .truncate = true, .read = true });
    errdefer timestampsFile.close(io);
    const timestampsDst = try StreamDestination.initFile(io, timestampsFile, destinationBuffer);
    pathWriter.end = 0;

    try std.fs.path.fmtJoin(&.{ path, filenames.messageTokens }).format(&pathWriter);
    var messageBloomTokensFile = try Dir.createFileAbsolute(io, pathWriter.buffered(), .{ .truncate = true, .read = true });
    errdefer messageBloomTokensFile.close(io);
    const messageBloomTokensDst = try StreamDestination.initFile(io, messageBloomTokensFile, destinationBuffer);
    pathWriter.end = 0;

    try std.fs.path.fmtJoin(&.{ path, filenames.messageValues }).format(&pathWriter);
    var messageBloomValuesFile = try Dir.createFileAbsolute(io, pathWriter.buffered(), .{ .truncate = true, .read = true });
    errdefer messageBloomValuesFile.close(io);
    const messageBloomValuesDst = try StreamDestination.initFile(io, messageBloomValuesFile, destinationBuffer);
    pathWriter.end = 0;

    var bloomValuesList = try std.ArrayList(StreamDestination).initCapacity(alloc, bloomValuesMaxShardsCount);
    errdefer bloomValuesList.deinit(alloc);
    var bloomTokensList = try std.ArrayList(StreamDestination).initCapacity(alloc, bloomValuesMaxShardsCount);
    errdefer bloomTokensList.deinit(alloc);

    const columnIDGen = try ColumnIDGen.init(alloc);
    errdefer columnIDGen.deinit(alloc);
    var colIdx = std.AutoHashMap(u16, u16).init(alloc);
    errdefer colIdx.deinit();

    const timestampsEncoder = try TimestampsEncoder.init(alloc);
    errdefer timestampsEncoder.deinit(alloc);

    const w = try alloc.create(TableWriter);
    errdefer alloc.destroy(w);

    w.* = TableWriter{
        .timestampsDst = timestampsDst,
        .indexDst = indexDst,
        .metaindexDst = metaindexDst,

        .columnsHeaderDst = columnsHeaderDst,
        .columnsHeaderIndexDst = columnsHeaderIndexDst,

        .columnKeysDst = columnKeysDst,
        .columnIdxsDst = columnIdxsDst,

        .messageBloomValuesDst = messageBloomValuesDst,
        .messageBloomTokensDst = messageBloomTokensDst,
        .bloomValuesList = bloomValuesList,
        .bloomTokensList = bloomTokensList,

        .columnIDGen = columnIDGen,
        .columnKeyCopies = .empty,
        .colIdx = colIdx,
        .nextColI = 0,
        .maxColI = bloomValuesMaxShardsCount,

        .timestampsEncoder = timestampsEncoder,
        .path = path,
        .destinationBuffer = destinationBuffer,
    };
    return w;
}

pub fn deinit(self: *TableWriter, allocator: Allocator) void {
    self.bloomValuesList.deinit(allocator);
    self.bloomTokensList.deinit(allocator);

    self.columnIDGen.deinit(allocator);
    for (self.columnKeyCopies.items) |key| {
        allocator.free(key);
    }
    self.columnKeyCopies.deinit(allocator);
    self.colIdx.deinit();

    if (self.destinationBuffer) |buf| {
        buf.deinit(allocator);
        allocator.destroy(buf);
    }

    self.timestampsEncoder.deinit(allocator);

    allocator.destroy(self);
}

/// size gives the amount of all the buffers bytes,
/// the content of the buffers is compressed
pub fn size(self: *TableWriter) u32 {
    var res: usize = self.timestampsDst.len();
    res += self.indexDst.len();
    res += self.metaindexDst.len();
    res += self.columnsHeaderDst.len();
    res += self.columnsHeaderIndexDst.len();
    res += self.columnKeysDst.len();
    res += self.columnIdxsDst.len();

    res += self.messageBloomValuesDst.len();
    res += self.messageBloomTokensDst.len();
    for (self.bloomValuesList.items, self.bloomTokensList.items) |bloomValuesBuf, bloomTokensBuf| {
        res += bloomValuesBuf.len();
        res += bloomTokensBuf.len();
    }

    return @intCast(res);
}

pub fn writeColumnKeys(self: *TableWriter, io: Io, allocator: Allocator) !void {
    const encodingBound = try self.columnIDGen.bound();
    const slice = try self.columnKeysDst.allocSlice(allocator, encodingBound);
    const offset = try self.columnIDGen.encode(allocator, slice);
    try self.columnKeysDst.appendAllocated(io, slice, offset);
}

pub fn writeColumnIndexes(self: *TableWriter, io: Io, allocator: Allocator) !void {
    const count = self.colIdx.count();

    var bound = Encoder.varIntBound(count);
    var it = self.colIdx.iterator();
    while (it.next()) |entry| {
        bound += Encoder.varIntBound(entry.key_ptr.*);
        bound += Encoder.varIntBound(entry.value_ptr.*);
    }

    const slice = try self.columnIdxsDst.allocSlice(allocator, bound);

    var enc = Encoder.init(slice);
    enc.writeVarInt(count);
    it = self.colIdx.iterator();
    while (it.next()) |entry| {
        enc.writeVarInt(entry.key_ptr.*);
        enc.writeVarInt(entry.value_ptr.*);
    }
    try self.columnIdxsDst.appendAllocated(io, slice, enc.offset);
}

pub fn writeBlock(
    self: *TableWriter,
    io: Io,
    allocator: Allocator,
    block: *Block,
    blockHeader: *BlockHeader,
) !void {
    // TODO: assert block
    try self.writeTimestamps(io, allocator, &blockHeader.timestampsHeader, block.timestamps);

    const columnsHeader = try ColumnsHeader.initFromBlock(allocator, block);
    defer columnsHeader.deinit(allocator);
    const columns = block.getColumns();

    try self.columnIDGen.keyIDs.ensureUnusedCapacity(allocator, columns.len);
    try self.colIdx.ensureUnusedCapacity(@intCast(columns.len));
    try self.bloomValuesList.ensureUnusedCapacity(allocator, columns.len);
    try self.bloomTokensList.ensureUnusedCapacity(allocator, columns.len);

    for (columns, 0..) |col, i| {
        try self.writeColumn(io, allocator, col, &columnsHeader.headers[i]);
    }

    try self.writeColumnsHeader(io, allocator, columnsHeader, blockHeader);
}

pub fn writeData(
    self: *TableWriter,
    io: Io,
    alloc: Allocator,
    blockHeader: *BlockHeader,
    data: *BlockData,
) !void {
    try self.writeTimestampsData(io, alloc, &blockHeader.timestampsHeader, data.timestampsData);

    const columnsHeader = try ColumnsHeader.initFromData(alloc, data);
    defer columnsHeader.deinit(alloc);
    const columns = data.columnsData.items;

    try self.columnIDGen.keyIDs.ensureUnusedCapacity(alloc, columns.len);
    try self.colIdx.ensureUnusedCapacity(@intCast(columns.len));
    try self.bloomValuesList.ensureUnusedCapacity(alloc, columns.len);
    try self.bloomTokensList.ensureUnusedCapacity(alloc, columns.len);

    for (columns, 0..) |col, i| {
        try self.writeColumnData(io, alloc, col, &columnsHeader.headers[i]);
    }

    try self.writeColumnsHeader(io, alloc, columnsHeader, blockHeader);
}

fn writeTimestamps(
    self: *TableWriter,
    io: Io,
    allocator: Allocator,
    tsHeader: *TimestampsHeader,
    timestamps: []u64,
) !void {
    if (timestamps.len == 0) {
        return Error.EmptyTimestamps;
    }

    var fba = std.heap.stackFallback(2048, allocator);
    var fbaAlloc = fba.get();
    const encodedTimestamps = try self.timestampsEncoder.encode(fbaAlloc, timestamps);
    defer fbaAlloc.free(encodedTimestamps.buf);
    const encodedTimestampsBuf = encodedTimestamps.buf[0..encodedTimestamps.offset];

    tsHeader.* = .{
        .min = timestamps[0],
        .max = timestamps[timestamps.len - 1],
        .offset = self.timestampsDst.len(),
        .size = encodedTimestampsBuf.len,
        .encodingType = encodedTimestamps.encodingType,
    };

    try self.timestampsDst.appendSlice(io, allocator, encodedTimestampsBuf);
}

// TODO: audit the entire codebase on stateful buffers management
// currently lots of writing to the buffers goes out of order,
// so if a nerror occurs the state may stuck and corrupt the data for the next writes,
// instead we must:
// 1. allocate the necessary space in the buffers
// 2. define cleaners on top of such writes
// 3. execute the most error prone operations at the beginning
// TODO: we might experiment with indexing every N lines (8192 let's say)
// We can start from timestamp.
// To speedup the search we can write not only timestamps itself,
// but also values between the intervals in order to specify the range.
// It could be especially useful for large files that contains 100k+ lines,
// it must help speed up queries with a narrow time range for stale data
fn writeTimestampsData(
    self: *TableWriter,
    io: Io,
    alloc: Allocator,
    tsHeader: *TimestampsHeader,
    timestampsData: TimestampsData,
) !void {
    std.debug.assert(timestampsData.data.len <= maxTimestampsBlockSize);

    tsHeader.* = .{
        .min = timestampsData.minTimestamp,
        .max = timestampsData.maxTimestamp,
        .offset = self.timestampsDst.len(),
        .size = timestampsData.data.len,
        .encodingType = timestampsData.encodingType,
    };
    try self.timestampsDst.appendSlice(io, alloc, timestampsData.data);
}

fn writeColumn(self: *TableWriter, io: Io, allocator: Allocator, col: Column, ch: *ColumnHeader) !void {
    ch.key = col.key;

    const valuesEncoder = try ValuesEncoder.init(allocator);
    defer valuesEncoder.deinit();
    const valueType = try valuesEncoder.encode(col.values, &ch.dict);
    ch.type = valueType.type;
    ch.min = valueType.min;
    ch.max = valueType.max;
    const packer = try Packer.init(allocator);
    defer packer.deinit();
    const packedValues = try packer.packValues(valuesEncoder.values.items);
    defer allocator.free(packedValues);
    std.debug.assert(packedValues.len <= maxPackedValuesSize);

    const bloomBufI = self.getBloomBufferIndex(io, allocator, ch.key);
    const bloomValuesBuf = if (bloomBufI) |i| &self.bloomValuesList.items[i] else |err| switch (err) {
        error.MessageBloomMustBeUsed => &self.messageBloomValuesDst,
        else => return err,
    };
    const bloomTokensBuf = if (bloomBufI) |i| &self.bloomTokensList.items[i] else |err| switch (err) {
        error.MessageBloomMustBeUsed => &self.messageBloomTokensDst,
        else => return err,
    };

    ch.size = packedValues.len;
    ch.offset = bloomValuesBuf.len();
    try bloomValuesBuf.appendSlice(io, allocator, packedValues);

    const bloomHash = if (valueType.type == .dict) &[_]u8{} else blk: {
        const tokenizer = try HashTokenizer.init(allocator);
        defer tokenizer.deinit(allocator);

        var hashes = try tokenizer.tokenizeValues(allocator, col.values);
        defer hashes.deinit(allocator);

        const hashed = try encodeBloomHashes(allocator, hashes.items);
        break :blk hashed;
    };
    defer {
        if (valueType.type != .dict) {
            allocator.free(bloomHash);
        }
    }
    ch.bloomFilterSize = bloomHash.len;
    ch.bloomFilterOffset = bloomTokensBuf.len();
    try bloomTokensBuf.appendSlice(io, allocator, bloomHash);
}

fn writeColumnData(self: *TableWriter, io: Io, alloc: Allocator, col: ColumnData, ch: *ColumnHeader) !void {
    const dataLen = col.bloomValues.len;
    std.debug.assert(dataLen <= maxValuesBlockSize);

    ch.key = col.key;
    ch.type = col.type;

    ch.min = col.min;
    ch.max = col.max;

    // move the dict ownership to ch in order to avoid double free
    std.mem.swap(std.ArrayList([]const u8), &ch.dict.values, &col.dict.values);
    ch.size = dataLen;

    const bloomBufI = self.getBloomBufferIndex(io, alloc, ch.key);
    const bloomValuesBuf = if (bloomBufI) |i| &self.bloomValuesList.items[i] else |err| switch (err) {
        error.MessageBloomMustBeUsed => &self.messageBloomValuesDst,
        else => return err,
    };
    const bloomTokensBuf = if (bloomBufI) |i| &self.bloomTokensList.items[i] else |err| switch (err) {
        error.MessageBloomMustBeUsed => &self.messageBloomTokensDst,
        else => return err,
    };

    ch.offset = bloomValuesBuf.len();
    try bloomValuesBuf.appendSlice(io, alloc, col.bloomValues);

    const bloomFilterSize = bloomTokensBuf.len();
    std.debug.assert(bloomFilterSize <= maxBloomTokensBlockSize);
    ch.bloomFilterSize = if (col.bloomTokens) |d| d.len else 0;
    ch.bloomFilterOffset = bloomTokensBuf.len();
    if (col.bloomTokens) |d| try bloomTokensBuf.appendSlice(io, alloc, d);
}

fn getBloomBufferIndex(self: *TableWriter, io: Io, alloc: Allocator, key: []const u8) !u16 {
    if (key.len == 0) {
        // TODO: this is a low quality return value, we could
        // 1. make msg bloom as a 0 item of the list
        // 2. or make a better branching outside
        return error.MessageBloomMustBeUsed;
    }

    if (self.columnIDGen.keyIDs.get(key) == null) {
        try self.ensureColumnKeyOwned(alloc, key);
    }
    const colID = self.columnIDGen.keyIDs.get(key).?;
    const maybeColI = self.colIdx.get(colID);
    if (maybeColI) |colI| {
        return colI;
    }

    // TODO: we can get rid of colIdx, because:
    // 1. the keys are stored in order of appearance
    // 2. the max amount of blooms are known in advance,
    // so we can calculate colI dynamically without storing them,
    // but requires validation the max blooms is a known value
    const colI = self.nextColI % self.maxColI;
    self.nextColI += 1;
    self.colIdx.putAssumeCapacity(colID, colI);

    if (colI >= self.bloomValuesList.items.len) {
        std.debug.assert(colI == self.bloomValuesList.items.len);
        // path if empty for mem table, we collect bloom buffers in init,
        // this branch is only for a disk table
        std.debug.assert(self.path.len != 0);
        var pathBuf: [std.fs.max_path_bytes]u8 = undefined;
        const scratchBuf = switch (self.messageBloomValuesDst) {
            .file => |f| f.buf,
            .buffer => std.debug.panic("buffer blooms must have been pre-created in advance", .{}),
        };
        var valuesDst = try createBloomValuesFile(io, &pathBuf, self.path, scratchBuf, colI);
        errdefer valuesDst.deinit(io, alloc);
        const tokensDst = try createBloomTokensValues(io, &pathBuf, self.path, scratchBuf, colI);
        errdefer tokensDst.deinit(io, alloc);
        self.bloomValuesList.appendAssumeCapacity(valuesDst);
        self.bloomTokensList.appendAssumeCapacity(tokensDst);
    }

    return colI;
}

fn ensureColumnKeyOwned(self: *TableWriter, alloc: Allocator, key: []const u8) !void {
    if (self.columnIDGen.keyIDs.get(key) != null) {
        return;
    }

    try self.columnIDGen.keyIDs.ensureUnusedCapacity(alloc, 1);
    // TODO: find why we can't borrow a key
    const ownedKey = try alloc.dupe(u8, key);
    errdefer alloc.free(ownedKey);

    _ = self.columnIDGen.genIDAssumeCapacity(ownedKey);
    try self.columnKeyCopies.append(alloc, ownedKey);
}

fn createBloomValuesFile(io: Io, pathBuf: []u8, tablePath: []const u8, buf: *std.ArrayList(u8), i: usize) !StreamDestination {
    const path = try filenames.writeBloomFilePath(pathBuf, tablePath, filenames.bloomValues, i);
    const file = try std.Io.Dir.cwd().createFile(io, path, .{});
    errdefer file.close(io);
    return StreamDestination.initFile(io, file, buf);
}

fn createBloomTokensValues(io: Io, pathBuf: []u8, tablePath: []const u8, buf: *std.ArrayList(u8), i: usize) !StreamDestination {
    const path = try filenames.writeBloomFilePath(pathBuf, tablePath, filenames.bloomTokens, i);
    const file = try std.Io.Dir.cwd().createFile(io, path, .{});
    errdefer file.close(io);
    return StreamDestination.initFile(io, file, buf);
}

fn writeColumnsHeader(
    self: *TableWriter,
    io: Io,
    allocator: Allocator,
    csh: *ColumnsHeader,
    bh: *BlockHeader,
) !void {
    std.debug.assert(csh.headers.len + csh.celledColumns.len <= Block.maxColumns);

    var columnDescs: [Block.maxColumns]ColumnsHeaderIndex.ColumnDesc = undefined;
    var celledColumnDescs: [Block.maxColumns]ColumnsHeaderIndex.ColumnDesc = undefined;
    var cshIdx = ColumnsHeaderIndex.initBuffer(&columnDescs, &celledColumnDescs);

    for (csh.headers) |header| {
        try self.ensureColumnKeyOwned(allocator, header.key);
    }
    for (csh.celledColumns) |column| {
        try self.ensureColumnKeyOwned(allocator, column.key);
    }

    try self.columnIDGen.keyIDs.ensureUnusedCapacity(allocator, csh.celledColumns.len);

    const dstSize = csh.encodeBound();
    const dst = try self.columnsHeaderDst.allocSlice(allocator, dstSize);

    const cshOffset = csh.encode(dst, &cshIdx, self.columnIDGen);

    bh.columnsHeaderOffset = self.columnsHeaderDst.len();
    bh.columnsHeaderSize = cshOffset;
    try self.columnsHeaderDst.appendAllocated(io, dst, cshOffset);

    const dstIdxSize = cshIdx.encodeBound();
    const dstIdx = try self.columnsHeaderIndexDst.allocSlice(allocator, dstIdxSize);
    const cshIdxOffset = cshIdx.encode(dstIdx);

    bh.columnsHeaderIndexOffset = self.columnsHeaderIndexDst.len();
    bh.columnsHeaderIndexSize = cshIdxOffset;
    try self.columnsHeaderIndexDst.appendAllocated(io, dstIdx, cshIdxOffset);
}

const testing = std.testing;
const Line = @import("../lines.zig").Line;
const Field = @import("../lines.zig").Field;
const SID = @import("../lines.zig").SID;
const TableReader = @import("TableReader.zig");

test "writeBlock and writeData produce identical buffer output" {
    const alloc = testing.allocator;
    const io = std.testing.io;

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
    const line1 = Line{ .timestampNs = 1, .sid = sid, .fields = &fields1 };
    const line2 = Line{ .timestampNs = 2, .sid = sid, .fields = &fields2 };
    const line3 = Line{ .timestampNs = 3, .sid = sid, .fields = &fields3 };
    var lines = [_]Line{ line1, line2, line3 };

    // Writer 1: encode via writeBlock
    const memTable1 = try MemTable.init(alloc);
    const table1 = try Table.fromMem(alloc, memTable1);
    defer table1.close(io);

    const writer1 = try TableWriter.initMem(alloc, memTable1);
    defer writer1.deinit(alloc);

    const block = try Block.initFromLines(alloc, &lines);
    defer block.deinit(alloc);

    var bh1 = BlockHeader.initFromBlock(block, sid);
    try writer1.writeBlock(io, alloc, block, &bh1);

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
    const writer2 = try TableWriter.initMem(alloc, memTable2);
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
    const io = std.testing.io;

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
    const line = Line{ .timestampNs = 1, .sid = sid, .fields = &fields };
    var lines = [_]Line{line};

    const memTable = try MemTable.init(alloc);
    defer memTable.deinit(alloc);
    const writer = try TableWriter.initMem(alloc, memTable);
    defer writer.deinit(alloc);

    const block = try Block.initFromLines(alloc, &lines);
    defer block.deinit(alloc);

    var bh = BlockHeader.initFromBlock(block, sid);
    try writer.writeBlock(io, alloc, block, &bh);
}
