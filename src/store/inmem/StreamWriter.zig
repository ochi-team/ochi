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
const filenames = @import("../../filenames.zig");
const fs = @import("../../fs.zig");
const BlockHeader = @import("block_header.zig").BlockHeader;
const ColumnsHeader = @import("block_header.zig").ColumnsHeader;
const ColumnHeader = @import("block_header.zig").ColumnHeader;
const TimestampsHeader = @import("block_header.zig").TimestampsHeader;
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

// TODO: consider a better name:
// - MemTableWriter
// - LinesWriter
// - ColumnsWriter
// - TypedWriter
// or find a better one,
// then rename Self, StreamMerger and StreamReader accordingly
const Self = @This();

const tsBufferSize = 2 * 1024;
const indexBufferSize = 2 * 1024;
const metaIndexBufferSize = 2 * 1024;
const columnsHeaderBufferSize = 2 * 1024;
const columnsHeaderIndexBufferSize = 2 * 1024;
const messageBloomValuesSize = 2 * 1024;
const messageBloomTokensSize = 2 * 1024;
const columnKeysBufferSize = 512;
const columnIndexesBufferSize = 128;

// TODO: expose metrics on len/cap relations
timestampsDst: StreamDestination,
indexDst: StreamDestination,
metaIndexDst: StreamDestination,

columnsHeaderDst: StreamDestination,
columnsHeaderIndexDst: StreamDestination,

columnKeysBuf: StreamDestination,
// TODO: conider to get rid of that since we have the keys ordered
columnIdxsBuf: StreamDestination,

messageBloomValuesDst: StreamDestination,
messageBloomTokensDst: StreamDestination,
bloomValuesList: std.ArrayList(StreamDestination),
bloomTokensList: std.ArrayList(StreamDestination),

columnIDGen: *ColumnIDGen,
colIdx: std.AutoHashMap(u16, u16),
nextColI: u16,
maxColI: u16,

timestampsEncoder: *TimestampsEncoder,

path: []const u8,

pub fn initMem(io: Io, allocator: Allocator, maxColI: u16) !*Self {
    var timestampsDst = try StreamDestination.initBuffer(allocator, tsBufferSize);
    errdefer timestampsDst.deinit(io, allocator);
    var indexDst = try StreamDestination.initBuffer(allocator, indexBufferSize);
    errdefer indexDst.deinit(io, allocator);
    var metaIndexDst = try StreamDestination.initBuffer(allocator, metaIndexBufferSize);
    errdefer metaIndexDst.deinit(io, allocator);

    var columnsHeaderDst = try StreamDestination.initBuffer(allocator, columnsHeaderBufferSize);
    errdefer columnsHeaderDst.deinit(io, allocator);
    var columnsHeaderIndexDst = try StreamDestination.initBuffer(allocator, columnsHeaderIndexBufferSize);
    errdefer columnsHeaderIndexDst.deinit(io, allocator);

    var columnKeysBuf = try StreamDestination.initBuffer(allocator, columnKeysBufferSize);
    errdefer columnKeysBuf.deinit(io, allocator);
    var columnIdxsBuf = try StreamDestination.initBuffer(allocator, columnIndexesBufferSize);
    errdefer columnIdxsBuf.deinit(io, allocator);

    var msgBloomValuesDst = try StreamDestination.initBuffer(allocator, messageBloomValuesSize);
    errdefer msgBloomValuesDst.deinit(io, allocator);
    var msgBloomTokensDst = try StreamDestination.initBuffer(allocator, messageBloomTokensSize);
    errdefer msgBloomTokensDst.deinit(io, allocator);
    var bloomValuesList = try std.ArrayList(StreamDestination).initCapacity(allocator, maxColI);
    errdefer bloomValuesList.deinit(allocator);
    var bloomTokensList = try std.ArrayList(StreamDestination).initCapacity(allocator, maxColI);
    errdefer bloomTokensList.deinit(allocator);

    const columnIDGen = try ColumnIDGen.init(allocator);
    errdefer columnIDGen.deinit(allocator);
    var colIdx = std.AutoHashMap(u16, u16).init(allocator);
    errdefer colIdx.deinit();

    const timestampsEncoder = try TimestampsEncoder.init(allocator);
    errdefer timestampsEncoder.deinit(allocator);

    const w = try allocator.create(Self);
    errdefer allocator.destroy(w);

    w.* = Self{
        .timestampsDst = timestampsDst,
        .indexDst = indexDst,
        .metaIndexDst = metaIndexDst,

        .columnsHeaderDst = columnsHeaderDst,
        .columnsHeaderIndexDst = columnsHeaderIndexDst,

        .messageBloomValuesDst = msgBloomValuesDst,
        .messageBloomTokensDst = msgBloomTokensDst,
        .bloomValuesList = bloomValuesList,
        .bloomTokensList = bloomTokensList,

        .columnIDGen = columnIDGen,
        .colIdx = colIdx,
        .nextColI = 0,
        .maxColI = maxColI,

        .columnKeysBuf = columnKeysBuf,
        .columnIdxsBuf = columnIdxsBuf,

        .timestampsEncoder = timestampsEncoder,
        // path is empty for mem table
        .path = "",
    };
    return w;
}

pub fn initDisk(io: Io, alloc: Allocator, path: []const u8, fitsInCache: bool) !*Self {
    std.debug.assert(path.len != 0);

    // TODO: implement page cache support
    _ = fitsInCache;

    fs.createDirAssert(io, path);

    var stack = std.heap.stackFallback(2048, alloc);
    const fba = stack.get();

    // TODO: open files in parallel

    // TODO: banch of openings are duplicated across index and data file,
    // it's better to have them all together as DataBuffers, DataFiles, etc.
    const columnKeysPath = try std.fs.path.join(fba, &.{ path, filenames.columnKeys });
    defer fba.free(columnKeysPath);
    const columnIdxsPath = try std.fs.path.join(fba, &.{ path, filenames.columnIdxs });
    defer fba.free(columnIdxsPath);
    const metaindexPath = try std.fs.path.join(fba, &.{ path, filenames.metaindex });
    defer fba.free(metaindexPath);
    const indexPath = try std.fs.path.join(fba, &.{ path, filenames.index });
    defer fba.free(indexPath);
    const columnsHeaderIndexPath = try std.fs.path.join(fba, &.{ path, filenames.columnsHeaderIndex });
    defer fba.free(columnsHeaderIndexPath);
    const columnsHeaderPath = try std.fs.path.join(fba, &.{ path, filenames.columnsHeader });
    defer fba.free(columnsHeaderPath);
    const timestampsPath = try std.fs.path.join(fba, &.{ path, filenames.timestamps });
    defer fba.free(timestampsPath);
    const messageBloomTokensPath = try std.fs.path.join(fba, &.{ path, filenames.messageTokens });
    defer fba.free(messageBloomTokensPath);
    const messageBloomValuesPath = try std.fs.path.join(fba, &.{ path, filenames.messageValues });
    defer fba.free(messageBloomValuesPath);

    var columnKeysFile = try Dir.createFileAbsolute(io, columnKeysPath, .{ .truncate = true, .read = true });
    errdefer columnKeysFile.close(io);
    var columnKeysBuf = try StreamDestination.initFile(io, columnKeysFile);
    errdefer columnKeysBuf.deinit(io, alloc);

    var columnIdxsFile = try Dir.createFileAbsolute(io, columnIdxsPath, .{ .truncate = true, .read = true });
    errdefer columnIdxsFile.close(io);
    var columnIdxsBuf = try StreamDestination.initFile(io, columnIdxsFile);
    errdefer columnIdxsBuf.deinit(io, alloc);

    var metaindexFile = try Dir.createFileAbsolute(io, metaindexPath, .{ .truncate = true, .read = true });
    errdefer metaindexFile.close(io);
    var metaIndexDst = try StreamDestination.initFile(io, metaindexFile);
    errdefer metaIndexDst.deinit(io, alloc);

    var indexFile = try Dir.createFileAbsolute(io, indexPath, .{ .truncate = true, .read = true });
    errdefer indexFile.close(io);
    var indexDst = try StreamDestination.initFile(io, indexFile);
    errdefer indexDst.deinit(io, alloc);

    var columnsHeaderIndexFile = try Dir.createFileAbsolute(io, columnsHeaderIndexPath, .{ .truncate = true, .read = true });
    errdefer columnsHeaderIndexFile.close(io);
    var columnsHeaderIndexDst = try StreamDestination.initFile(io, columnsHeaderIndexFile);
    errdefer columnsHeaderIndexDst.deinit(io, alloc);

    var columnsHeaderFile = try Dir.createFileAbsolute(io, columnsHeaderPath, .{ .truncate = true, .read = true });
    errdefer columnsHeaderFile.close(io);
    var columnsHeaderDst = try StreamDestination.initFile(io, columnsHeaderFile);
    errdefer columnsHeaderDst.deinit(io, alloc);

    var timestampsFile = try Dir.createFileAbsolute(io, timestampsPath, .{ .truncate = true, .read = true });
    errdefer timestampsFile.close(io);
    var timestampsDst = try StreamDestination.initFile(io, timestampsFile);
    errdefer timestampsDst.deinit(io, alloc);

    var messageBloomTokensFile = try Dir.createFileAbsolute(io, messageBloomTokensPath, .{ .truncate = true, .read = true });
    errdefer messageBloomTokensFile.close(io);
    var msgBloomTokensDst = try StreamDestination.initFile(io, messageBloomTokensFile);
    errdefer msgBloomTokensDst.deinit(io, alloc);

    var messageBloomValuesFile = try Dir.createFileAbsolute(io, messageBloomValuesPath, .{ .truncate = true, .read = true });
    errdefer messageBloomValuesFile.close(io);
    var msgBloomValuesDst = try StreamDestination.initFile(io, messageBloomValuesFile);
    errdefer msgBloomValuesDst.deinit(io, alloc);
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

    const w = try alloc.create(Self);
    errdefer alloc.destroy(w);

    w.* = Self{
        .timestampsDst = timestampsDst,
        .indexDst = indexDst,
        .metaIndexDst = metaIndexDst,

        .columnsHeaderDst = columnsHeaderDst,
        .columnsHeaderIndexDst = columnsHeaderIndexDst,

        .messageBloomValuesDst = msgBloomValuesDst,
        .messageBloomTokensDst = msgBloomTokensDst,
        .bloomValuesList = bloomValuesList,
        .bloomTokensList = bloomTokensList,

        .columnIDGen = columnIDGen,
        .colIdx = colIdx,
        .nextColI = 0,
        .maxColI = bloomValuesMaxShardsCount,

        .columnKeysBuf = columnKeysBuf,
        .columnIdxsBuf = columnIdxsBuf,

        .timestampsEncoder = timestampsEncoder,
        .path = path,
    };
    return w;
}

pub fn deinit(self: *Self, io: Io, allocator: Allocator) void {
    self.timestampsDst.deinit(io, allocator);
    self.indexDst.deinit(io, allocator);
    self.metaIndexDst.deinit(io, allocator);

    self.columnsHeaderDst.deinit(io, allocator);
    self.columnsHeaderIndexDst.deinit(io, allocator);

    self.messageBloomValuesDst.deinit(io, allocator);
    self.messageBloomTokensDst.deinit(io, allocator);
    for (self.bloomValuesList.items) |*bv| {
        bv.deinit(io, allocator);
    }
    self.bloomValuesList.deinit(allocator);
    for (self.bloomTokensList.items) |*bv| {
        bv.deinit(io, allocator);
    }
    self.bloomTokensList.deinit(allocator);

    self.columnIDGen.deinit(allocator);
    self.colIdx.deinit();

    self.columnKeysBuf.deinit(io, allocator);
    self.columnIdxsBuf.deinit(io, allocator);

    self.timestampsEncoder.deinit(allocator);

    allocator.destroy(self);
}

/// size gives the amount of all the buffers bytes,
/// the content of the buffers is compressed
pub fn size(self: *Self) u32 {
    var res: usize = self.timestampsDst.len();
    res += self.indexDst.len();
    res += self.metaIndexDst.len();
    res += self.columnsHeaderDst.len();
    res += self.columnsHeaderIndexDst.len();
    res += self.columnKeysBuf.len();
    res += self.columnIdxsBuf.len();

    res += self.messageBloomValuesDst.len();
    res += self.messageBloomTokensDst.len();
    for (self.bloomValuesList.items, self.bloomTokensList.items) |bloomValuesBuf, bloomTokensBuf| {
        res += bloomValuesBuf.len();
        res += bloomTokensBuf.len();
    }

    return @intCast(res);
}

pub fn writeColumnKeys(self: *Self, io: Io, allocator: Allocator) !void {
    const encodingBound = try self.columnIDGen.bound();
    const slice = try self.columnKeysBuf.allocSlice(allocator, encodingBound);
    const offset = try self.columnIDGen.encode(allocator, slice);
    try self.columnKeysBuf.appendAllocated(io, slice, offset);
}

pub fn writeColumnIndexes(self: *Self, io: Io, allocator: Allocator) !void {
    const count = self.colIdx.count();

    var bound = Encoder.varIntBound(count);
    var it = self.colIdx.iterator();
    while (it.next()) |entry| {
        bound += Encoder.varIntBound(entry.key_ptr.*);
        bound += Encoder.varIntBound(entry.value_ptr.*);
    }

    const slice = try self.columnIdxsBuf.allocSlice(allocator, bound);

    var enc = Encoder.init(slice);
    enc.writeVarInt(count);
    it = self.colIdx.iterator();
    while (it.next()) |entry| {
        enc.writeVarInt(entry.key_ptr.*);
        enc.writeVarInt(entry.value_ptr.*);
    }
    try self.columnIdxsBuf.appendAllocated(io, slice, enc.offset);
}

pub fn writeBlock(
    self: *Self,
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
    self: *Self,
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
    self: *Self,
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

fn writeTimestampsData(
    self: *Self,
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

fn writeColumn(self: *Self, io: Io, allocator: Allocator, col: Column, ch: *ColumnHeader) !void {
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

fn writeColumnData(self: *Self, io: Io, alloc: Allocator, col: ColumnData, ch: *ColumnHeader) !void {
    const dataLen = col.data.len;
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
    try bloomValuesBuf.appendSlice(io, alloc, col.data);

    const bloomFilterSize = bloomTokensBuf.len();
    std.debug.assert(bloomFilterSize <= maxBloomTokensBlockSize);
    ch.bloomFilterSize = if (col.bloomFilterData) |d| d.len else 0;
    ch.bloomFilterOffset = bloomTokensBuf.len();
    if (col.bloomFilterData) |d| try bloomTokensBuf.appendSlice(io, alloc, d);
}

fn getBloomBufferIndex(self: *Self, io: Io, alloc: Allocator, key: []const u8) !u16 {
    if (key.len == 0) {
        return error.MessageBloomMustBeUsed;
    }

    const colID = self.columnIDGen.genIDAssumeCapacity(key);
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
        // path if empty for mem table
        if (self.path.len == 0) {
            var valuesBuf = try createBloomBuf(alloc);
            errdefer valuesBuf.deinit(io, alloc);
            const tokensBuf = try createBloomBuf(alloc);
            self.bloomValuesList.appendAssumeCapacity(valuesBuf);
            self.bloomTokensList.appendAssumeCapacity(tokensBuf);
        } else {
            var valuesDst = try createBloomValuesFile(io, alloc, self.path, colI);
            errdefer valuesDst.deinit(io, alloc);
            const tokensDst = try createBloomTokensValues(io, alloc, self.path, colI);
            self.bloomValuesList.appendAssumeCapacity(valuesDst);
            self.bloomTokensList.appendAssumeCapacity(tokensDst);
        }
    }

    return colI;
}

fn createBloomBuf(alloc: Allocator) !StreamDestination {
    return StreamDestination.initBuffer(alloc, messageBloomValuesSize);
}

fn createBloomValuesFile(io: Io, alloc: Allocator, tablePath: []const u8, i: usize) !StreamDestination {
    var stackFba = std.heap.stackFallback(128, alloc);
    const fba = stackFba.get();

    const path = try MemTable.getBloomValuesFilePath(fba, tablePath, i);
    defer fba.free(path);
    const file = try std.Io.Dir.cwd().createFile(io, path, .{});
    errdefer file.close(io);
    return StreamDestination.initFile(io, file);
}

fn createBloomTokensValues(io: Io, alloc: Allocator, tablePath: []const u8, i: usize) !StreamDestination {
    var stackFba = std.heap.stackFallback(128, alloc);
    const fba = stackFba.get();

    const path = try MemTable.getBloomTokensFilePath(fba, tablePath, i);
    defer fba.free(path);
    const file = try std.Io.Dir.cwd().createFile(io, path, .{});
    errdefer file.close(io);
    return StreamDestination.initFile(io, file);
}

fn writeColumnsHeader(
    self: *Self,
    io: Io,
    allocator: Allocator,
    csh: *ColumnsHeader,
    bh: *BlockHeader,
) !void {
    var cshIdx = try ColumnsHeaderIndex.init(allocator);
    defer cshIdx.deinit(allocator);

    const dstSize = csh.encodeBound();
    const dstIdxSize = cshIdx.encodeBound();
    const dst = try allocator.alloc(u8, dstSize + dstIdxSize);
    defer allocator.free(dst);

    try cshIdx.columns.ensureUnusedCapacity(allocator, csh.headers.len);
    try cshIdx.celledColumns.ensureUnusedCapacity(allocator, csh.celledColumns.len);
    try self.columnIDGen.keyIDs.ensureUnusedCapacity(allocator, csh.celledColumns.len);
    const cshOffset = csh.encode(dst, cshIdx, self.columnIDGen);
    const cshIdxOffset = cshIdx.encode(dst[cshOffset..]);

    bh.columnsHeaderOffset = self.columnsHeaderDst.len();
    bh.columnsHeaderSize = cshOffset;
    try self.columnsHeaderDst.appendSlice(io, allocator, dst[0..cshOffset]);

    bh.columnsHeaderIndexOffset = self.columnsHeaderIndexDst.len();
    bh.columnsHeaderIndexSize = cshIdxOffset;
    try self.columnsHeaderIndexDst.appendSlice(io, allocator, dst[cshOffset .. cshOffset + cshIdxOffset]);
}

const testing = std.testing;
const Line = @import("../lines.zig").Line;
const Field = @import("../lines.zig").Field;
const SID = @import("../lines.zig").SID;
const StreamReader = @import("reader.zig").StreamReader;

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
    const sid = SID{ .id = 1, .tenantID = "1111" };
    const line1 = Line{ .timestampNs = 1, .sid = sid, .fields = &fields1 };
    const line2 = Line{ .timestampNs = 2, .sid = sid, .fields = &fields2 };
    const line3 = Line{ .timestampNs = 3, .sid = sid, .fields = &fields3 };
    var lines = [_]Line{ line1, line2, line3 };

    // Writer 1: encode via writeBlock
    const maxColI = 128;
    const writer1 = try Self.initMem(io, alloc, maxColI);
    defer writer1.deinit(alloc);

    const block = try Block.initFromLines(alloc, &lines);
    defer block.deinit(alloc);

    var bh1 = BlockHeader.initFromBlock(block, sid);
    try writer1.writeBlock(io, alloc, block, &bh1);

    // Build StreamReader from writer1's buffers to populate BlockData
    var bloomValuesList = try std.ArrayList([]const u8).initCapacity(alloc, writer1.bloomValuesList.items.len);
    defer bloomValuesList.deinit(alloc);
    for (writer1.bloomValuesList.items) |buf| bloomValuesList.appendAssumeCapacity(buf.asSliceAssumeBuffer());

    var bloomTokensList = try std.ArrayList([]const u8).initCapacity(alloc, writer1.bloomTokensList.items.len);
    defer bloomTokensList.deinit(alloc);
    for (writer1.bloomTokensList.items) |buf| bloomTokensList.appendAssumeCapacity(buf.asSliceAssumeBuffer());

    const sr = StreamReader{
        .timestampsBuf = writer1.timestampsDst.asSliceAssumeBuffer(),
        .indexBuf = writer1.indexDst.asSliceAssumeBuffer(),
        .metaIndexBuf = writer1.metaIndexDst.asSliceAssumeBuffer(),
        .columnsHeaderBuf = writer1.columnsHeaderDst.asSliceAssumeBuffer(),
        .columnsHeaderIndexBuf = writer1.columnsHeaderIndexDst.asSliceAssumeBuffer(),
        .columnsKeysBuf = writer1.columnKeysBuf.asSliceAssumeBuffer(),
        .columnIdxsBuf = writer1.columnIdxsBuf.asSliceAssumeBuffer(),
        .messageBloomValuesBuf = writer1.messageBloomValuesDst.asSliceAssumeBuffer(),
        .messageBloomTokensBuf = writer1.messageBloomTokensDst.asSliceAssumeBuffer(),
        .bloomValuesList = bloomValuesList,
        .bloomTokensList = bloomTokensList,
        .columnIDGen = writer1.columnIDGen,
        .colIdx = &writer1.colIdx,
    };

    var bd = BlockData.initEmpty();
    defer bd.deinit(alloc);
    bd.sid = sid;
    try bd.readFrom(alloc, &bh1, &sr);

    // Writer 2: re-encode the same data via writeData
    const writer2 = try Self.initMem(io, alloc, maxColI);
    defer writer2.deinit(alloc);

    var bh2 = BlockHeader.initFromData(&bd, sid);
    try writer2.writeData(io, alloc, &bh2, &bd);

    // Finalize both writers
    try writer1.writeColumnKeys(io, alloc);
    try writer1.writeColumnIndexes(io, alloc);
    try writer2.writeColumnKeys(io, alloc);
    try writer2.writeColumnIndexes(io, alloc);

    // Compare all data buffers
    try testing.expectEqualSlices(u8, writer1.timestampsDst.asSliceAssumeBuffer(), writer2.timestampsDst.asSliceAssumeBuffer());
    try testing.expectEqualSlices(u8, writer1.columnsHeaderDst.asSliceAssumeBuffer(), writer2.columnsHeaderDst.asSliceAssumeBuffer());
    try testing.expectEqualSlices(u8, writer1.columnsHeaderIndexDst.asSliceAssumeBuffer(), writer2.columnsHeaderIndexDst.asSliceAssumeBuffer());
    try testing.expectEqualSlices(u8, writer1.messageBloomValuesDst.asSliceAssumeBuffer(), writer2.messageBloomValuesDst.asSliceAssumeBuffer());
    try testing.expectEqualSlices(u8, writer1.messageBloomTokensDst.asSliceAssumeBuffer(), writer2.messageBloomTokensDst.asSliceAssumeBuffer());
    try testing.expectEqual(writer1.bloomValuesList.items.len, writer2.bloomValuesList.items.len);
    for (writer1.bloomValuesList.items, writer2.bloomValuesList.items) |b1, b2| {
        try testing.expectEqualSlices(u8, b1.asSliceAssumeBuffer(), b2.asSliceAssumeBuffer());
    }
    try testing.expectEqual(writer1.bloomTokensList.items.len, writer2.bloomTokensList.items.len);
    for (writer1.bloomTokensList.items, writer2.bloomTokensList.items) |b1, b2| {
        try testing.expectEqualSlices(u8, b1.asSliceAssumeBuffer(), b2.asSliceAssumeBuffer());
    }
    try testing.expectEqualSlices(u8, writer1.columnKeysBuf.asSliceAssumeBuffer(), writer2.columnKeysBuf.asSliceAssumeBuffer());
    try testing.expectEqualSlices(u8, writer1.columnIdxsBuf.asSliceAssumeBuffer(), writer2.columnIdxsBuf.asSliceAssumeBuffer());
}
