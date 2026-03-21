const std = @import("std");
const Allocator = std.mem.Allocator;

const ValuesEncoder = @import("ValuesEncoder.zig");

const Block = @import("Block.zig");
const Column = @import("Column.zig");
const MemTable = @import("MemTable.zig");
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

pub const Error = error{
    EmptyTimestamps,
};

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
// TODO: move the buffers ownership to MemTable and pass the pointers to the writer
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

pub fn initMem(allocator: Allocator, maxColI: u16) !*Self {
    var timestampsDst = try StreamDestination.initBuffer(allocator, tsBufferSize);
    errdefer timestampsDst.deinit(allocator);
    var indexDst = try StreamDestination.initBuffer(allocator, indexBufferSize);
    errdefer indexDst.deinit(allocator);
    var metaIndexDst = try StreamDestination.initBuffer(allocator, metaIndexBufferSize);
    errdefer metaIndexDst.deinit(allocator);

    var columnsHeaderDst = try StreamDestination.initBuffer(allocator, columnsHeaderBufferSize);
    errdefer columnsHeaderDst.deinit(allocator);
    var columnsHeaderIndexDst = try StreamDestination.initBuffer(allocator, columnsHeaderIndexBufferSize);
    errdefer columnsHeaderIndexDst.deinit(allocator);

    var columnKeysBuf = try StreamDestination.initBuffer(allocator, columnKeysBufferSize);
    errdefer columnKeysBuf.deinit(allocator);
    var columnIdxsBuf = try StreamDestination.initBuffer(allocator, columnIndexesBufferSize);
    errdefer columnIdxsBuf.deinit(allocator);

    var msgBloomValuesDst = try StreamDestination.initBuffer(allocator, messageBloomValuesSize);
    errdefer msgBloomValuesDst.deinit(allocator);
    var msgBloomTokensDst = try StreamDestination.initBuffer(allocator, messageBloomTokensSize);
    errdefer msgBloomTokensDst.deinit(allocator);
    var bloomValuesList = try std.ArrayList(StreamDestination).initCapacity(allocator, maxColI);
    errdefer bloomValuesList.deinit(allocator);
    var bloomTokensList = try std.ArrayList(StreamDestination).initCapacity(allocator, maxColI);
    errdefer bloomTokensList.deinit(allocator);

    const columnIDGen = try ColumnIDGen.init(allocator);
    errdefer columnIDGen.deinit(allocator);
    const colIdx = std.AutoHashMap(u16, u16).init(allocator);

    const timestampsEncoder = try TimestampsEncoder.init(allocator);
    errdefer timestampsEncoder.deinit(allocator);

    const w = try allocator.create(Self);
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

pub fn deinit(self: *Self, allocator: Allocator) void {
    self.timestampsDst.deinit(allocator);
    self.indexDst.deinit(allocator);
    self.metaIndexDst.deinit(allocator);

    self.columnsHeaderDst.deinit(allocator);
    self.columnsHeaderIndexDst.deinit(allocator);

    self.messageBloomValuesDst.deinit(allocator);
    self.messageBloomTokensDst.deinit(allocator);
    for (self.bloomValuesList.items) |*bv| {
        bv.deinit(allocator);
    }
    self.bloomValuesList.deinit(allocator);
    for (self.bloomTokensList.items) |*bv| {
        bv.deinit(allocator);
    }
    self.bloomTokensList.deinit(allocator);

    self.columnIDGen.deinit(allocator);
    self.colIdx.deinit();

    self.columnKeysBuf.deinit(allocator);
    self.columnIdxsBuf.deinit(allocator);

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

pub fn writeColumnKeys(self: *Self, allocator: Allocator) !void {
    const encodingBound = try self.columnIDGen.bound();
    const slice = try self.columnKeysBuf.allocSlice(allocator, encodingBound);
    const offset = try self.columnIDGen.encode(allocator, slice);
    try self.columnKeysBuf.appendAllocated(allocator, slice, offset);
}

// [10:len][20 * len:key value pair]
pub fn writeColumnIndexes(self: *Self, allocator: Allocator) !void {
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
    try self.columnIdxsBuf.appendAllocated(allocator, slice, enc.offset);
}

pub fn writeBlock(
    self: *Self,
    allocator: Allocator,
    block: *Block,
    blockHeader: *BlockHeader,
) !void {
    try self.writeTimestamps(allocator, &blockHeader.timestampsHeader, block.timestamps);

    const columnsHeader = try ColumnsHeader.init(allocator, block);
    defer columnsHeader.deinit(allocator);
    const columns = block.getColumns();
    try self.columnIDGen.keyIDs.ensureUnusedCapacity(columns.len);
    try self.colIdx.ensureUnusedCapacity(@intCast(columns.len));
    try self.bloomValuesList.ensureUnusedCapacity(allocator, columns.len);
    try self.bloomTokensList.ensureUnusedCapacity(allocator, columns.len);

    for (columns, 0..) |col, i| {
        try self.writeColumnHeader(allocator, col, &columnsHeader.headers[i]);
    }

    try self.writeColumnsHeader(allocator, columnsHeader, blockHeader);
}

fn writeTimestamps(
    self: *Self,
    allocator: Allocator,
    tsHeader: *TimestampsHeader,
    timestamps: []u64,
) !void {
    if (timestamps.len == 0) {
        return Error.EmptyTimestamps;
    }

    var fba = std.heap.stackFallback(2048, allocator);
    var staticAllocator = fba.get();
    const encodedTimestamps = try self.timestampsEncoder.encode(staticAllocator, timestamps);
    defer staticAllocator.free(encodedTimestamps.buf);
    const encodedTimestampsBuf = encodedTimestamps.buf[0..encodedTimestamps.offset];

    tsHeader.min = timestamps[0];
    tsHeader.max = timestamps[timestamps.len - 1];
    tsHeader.offset = self.timestampsDst.len();
    tsHeader.size = encodedTimestampsBuf.len;
    tsHeader.encodingType = encodedTimestamps.encodingType;

    try self.timestampsDst.appendSlice(allocator, encodedTimestampsBuf);
}

fn writeColumnHeader(self: *Self, allocator: Allocator, col: Column, ch: *ColumnHeader) !void {
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

    const bloomBufI = self.getBloomBufferIndex(allocator, ch.key);
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
    try bloomValuesBuf.appendSlice(allocator, packedValues);

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
    try bloomTokensBuf.appendSlice(allocator, bloomHash);
}

fn getBloomBufferIndex(self: *Self, alloc: Allocator, key: []const u8) !u16 {
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
            const valuesBuf = try createBloomBuf(alloc);
            const tokensBuf = try createBloomBuf(alloc);
            self.bloomValuesList.appendAssumeCapacity(valuesBuf);
            self.bloomTokensList.appendAssumeCapacity(tokensBuf);
        } else {
            const valuesDst = try createBloomValuesFile(alloc, self.path, colI);
            const tokensDst = try createBloomTokensValues(alloc, self.path, colI);
            self.bloomValuesList.appendAssumeCapacity(valuesDst);
            self.bloomTokensList.appendAssumeCapacity(tokensDst);
        }
    }

    return colI;
}

fn createBloomBuf(alloc: Allocator) !StreamDestination {
    return StreamDestination.initBuffer(alloc, messageBloomValuesSize);
}

fn createBloomValuesFile(alloc: Allocator, tablePath: []const u8, i: usize) !StreamDestination {
    const path = try MemTable.getBloomValuesFilePath(alloc, tablePath, i);
    const file = try std.fs.cwd().createFile(path, .{});
    return StreamDestination.initFile(file);
}

fn createBloomTokensValues(alloc: Allocator, tablePath: []const u8, i: usize) !StreamDestination {
    const path = try MemTable.getBloomTokensFilePath(alloc, tablePath, i);
    const file = try std.fs.cwd().createFile(path, .{});
    return StreamDestination.initFile(file);
}

fn writeColumnsHeader(
    self: *Self,
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
    try self.columnIDGen.keyIDs.ensureUnusedCapacity(csh.celledColumns.len);
    const cshOffset = csh.encode(dst, cshIdx, self.columnIDGen);
    const cshIdxOffset = cshIdx.encode(dst[cshOffset..]);

    bh.columnsHeaderOffset = self.columnsHeaderDst.len();
    bh.columnsHeaderSize = cshOffset;
    try self.columnsHeaderDst.appendSlice(allocator, dst[0..cshOffset]);

    bh.columnsHeaderIndexOffset = self.columnsHeaderIndexDst.len();
    bh.columnsHeaderIndexSize = cshIdxOffset;
    try self.columnsHeaderIndexDst.appendSlice(allocator, dst[cshOffset .. cshOffset + cshIdxOffset]);
}
