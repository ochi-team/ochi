const std = @import("std");

const ValuesEncoder = @import("ValuesEncoder.zig");

const Block = @import("block.zig").Block;
const Column = @import("block.zig").Column;
const BlockHeader = @import("block_header.zig").BlockHeader;
const ColumnsHeader = @import("block_header.zig").ColumnsHeader;
const ColumnHeader = @import("block_header.zig").ColumnHeader;
const TimestampsHeader = @import("block_header.zig").TimestampsHeader;
const Packer = @import("Packer.zig");
const ColumnsHeaderIndex = @import("ColumnsHeaderIndex.zig");
const ColumnIDGen = @import("ColumnIDGen.zig");
const TimestampsEncoder = @import("TimestampsEncoder.zig").TimestampsEncoder;
const HashTokenizer = @import("bloom.zig").HashTokenizer;
const encodeBloomHashes = @import("bloom.zig").encodeBloomHashes;
const encoding = @import("encoding");

const maxPackedValuesSize = 8 * 1024 * 1024;

pub const Error = error{
    EmptyTimestamps,
};

pub const StreamWriter = struct {
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
    timestampsBuf: std.ArrayList(u8),
    indexBuf: std.ArrayList(u8),
    metaIndexBuf: std.ArrayList(u8),

    columnsHeaderBuf: std.ArrayList(u8),
    columnsHeaderIndexBuf: std.ArrayList(u8),

    messageBloomValuesBuf: std.ArrayList(u8),
    messageBloomTokensBuf: std.ArrayList(u8),
    bloomValuesList: std.ArrayList(std.ArrayList(u8)),
    bloomTokensList: std.ArrayList(std.ArrayList(u8)),

    columnIDGen: *ColumnIDGen,
    colIdx: std.AutoHashMap(u16, u16),
    nextColI: u16,
    maxColI: u16,

    columnKeysBuf: std.ArrayList(u8),
    columnIdxsBuf: std.ArrayList(u8),

    timestampsEncoder: *TimestampsEncoder(u64),

    pub fn init(allocator: std.mem.Allocator, maxColI: u16) !*StreamWriter {
        var timestampsBuffer = try std.ArrayList(u8).initCapacity(allocator, tsBufferSize);
        errdefer timestampsBuffer.deinit(allocator);
        var indexBuffer = try std.ArrayList(u8).initCapacity(allocator, indexBufferSize);
        errdefer indexBuffer.deinit(allocator);
        var metaIndexBuf = try std.ArrayList(u8).initCapacity(allocator, metaIndexBufferSize);
        errdefer metaIndexBuf.deinit(allocator);

        var columnsHeaderBuf = try std.ArrayList(u8).initCapacity(allocator, columnsHeaderBufferSize);
        errdefer columnsHeaderBuf.deinit(allocator);
        var columnsHeaderIndexBuf = try std.ArrayList(u8).initCapacity(allocator, columnsHeaderIndexBufferSize);
        errdefer columnsHeaderIndexBuf.deinit(allocator);

        var msgBloomValuesBuf = try std.ArrayList(u8).initCapacity(allocator, messageBloomValuesSize);
        errdefer msgBloomValuesBuf.deinit(allocator);
        var msgBloomTokensBuf = try std.ArrayList(u8).initCapacity(allocator, messageBloomTokensSize);
        errdefer msgBloomTokensBuf.deinit(allocator);
        var bloomValuesList = try std.ArrayList(std.ArrayList(u8)).initCapacity(allocator, maxColI);
        errdefer bloomValuesList.deinit(allocator);
        var bloomTokensList = try std.ArrayList(std.ArrayList(u8)).initCapacity(allocator, maxColI);
        errdefer bloomTokensList.deinit(allocator);

        const columnIDGen = try ColumnIDGen.init(allocator);
        errdefer columnIDGen.deinit(allocator);
        const colIdx = std.AutoHashMap(u16, u16).init(allocator);

        var columnKeysBuf = try std.ArrayList(u8).initCapacity(allocator, columnKeysBufferSize);
        errdefer columnKeysBuf.deinit(allocator);
        var columnIdxsBuf = try std.ArrayList(u8).initCapacity(allocator, columnIndexesBufferSize);
        errdefer columnIdxsBuf.deinit(allocator);

        const timestampsEncoder = try TimestampsEncoder(u64).init(allocator);
        errdefer timestampsEncoder.deinit(allocator);

        const w = try allocator.create(StreamWriter);
        w.* = StreamWriter{
            .timestampsBuf = timestampsBuffer,
            .indexBuf = indexBuffer,
            .metaIndexBuf = metaIndexBuf,

            .columnsHeaderBuf = columnsHeaderBuf,
            .columnsHeaderIndexBuf = columnsHeaderIndexBuf,

            .messageBloomValuesBuf = msgBloomValuesBuf,
            .messageBloomTokensBuf = msgBloomTokensBuf,
            .bloomValuesList = bloomValuesList,
            .bloomTokensList = bloomTokensList,

            .columnIDGen = columnIDGen,
            .colIdx = colIdx,
            .nextColI = 0,
            .maxColI = maxColI,

            .columnKeysBuf = columnKeysBuf,
            .columnIdxsBuf = columnIdxsBuf,

            .timestampsEncoder = timestampsEncoder,
        };
        return w;
    }

    pub fn deinit(self: *StreamWriter, allocator: std.mem.Allocator) void {
        self.timestampsBuf.deinit(allocator);
        self.indexBuf.deinit(allocator);
        self.metaIndexBuf.deinit(allocator);

        self.columnsHeaderBuf.deinit(allocator);
        self.columnsHeaderIndexBuf.deinit(allocator);

        self.messageBloomValuesBuf.deinit(allocator);
        self.messageBloomTokensBuf.deinit(allocator);
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

    pub fn size(self: *StreamWriter) u32 {
        var res: usize = self.timestampsBuf.items.len;
        res += self.indexBuf.items.len;
        res += self.metaIndexBuf.items.len;
        res += self.columnsHeaderBuf.items.len;
        res += self.columnsHeaderIndexBuf.items.len;
        res += self.columnKeysBuf.items.len;
        res += self.columnIdxsBuf.items.len;

        res += self.messageBloomValuesBuf.items.len;
        res += self.messageBloomTokensBuf.items.len;
        for (self.bloomValuesList.items, self.bloomTokensList.items) |bloomValuesBuf, bloomTokensBuf| {
            res += bloomValuesBuf.items.len;
            res += bloomTokensBuf.items.len;
        }

        return @intCast(res);
    }

    pub fn writeColumnKeys(self: *StreamWriter, allocator: std.mem.Allocator) !void {
        const encodingBound = self.columnIDGen.bound();
        const buf = try allocator.alloc(u8, encodingBound);
        defer allocator.free(buf);

        const encodingOffset = self.columnIDGen.encode(buf);

        const compressBound = try encoding.compressBound(encodingOffset);
        try self.columnKeysBuf.ensureUnusedCapacity(allocator, compressBound);
        const slice = self.columnKeysBuf.unusedCapacitySlice()[0..compressBound];
        const offset = try encoding.compressAuto(slice, buf[0..encodingOffset]);
        self.columnKeysBuf.items.len += offset;
    }

    // [10:len][20 * len:key value pair]
    pub fn writeColumnIndexes(self: *StreamWriter, allocator: std.mem.Allocator) !void {
        const count = self.colIdx.count();
        try self.columnIdxsBuf.ensureUnusedCapacity(allocator, 10 + 20 * count);
        const slice = self.columnIdxsBuf.unusedCapacitySlice();

        var enc = encoding.Encoder.init(slice);
        enc.writeVarInt(count);
        var it = self.colIdx.iterator();
        while (it.next()) |entry| {
            enc.writeVarInt(entry.key_ptr.*);
            enc.writeVarInt(entry.value_ptr.*);
        }
        self.columnIdxsBuf.items.len += enc.offset;
    }

    pub fn writeBlock(
        self: *StreamWriter,
        allocator: std.mem.Allocator,
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
        self: *StreamWriter,
        allocator: std.mem.Allocator,
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
        tsHeader.offset = self.timestampsBuf.items.len;
        tsHeader.size = encodedTimestampsBuf.len;
        tsHeader.encodingType = encodedTimestamps.encodingType;

        try self.timestampsBuf.appendSlice(allocator, encodedTimestampsBuf);
    }

    fn writeColumnHeader(self: *StreamWriter, allocator: std.mem.Allocator, col: Column, ch: *ColumnHeader) !void {
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

        const bloomBufI = self.getBloomBufferIndex(ch.key);
        const bloomValuesBuf = if (bloomBufI) |i| &self.bloomValuesList.items[i] else |err| switch (err) {
            error.MessageBloomMustBeUsed => &self.messageBloomValuesBuf,
            else => return err,
        };
        const bloomTokensBuf = if (bloomBufI) |i| &self.bloomTokensList.items[i] else |err| switch (err) {
            error.MessageBloomMustBeUsed => &self.messageBloomTokensBuf,
            else => return err,
        };

        ch.size = packedValues.len;
        ch.offset = bloomValuesBuf.items.len;
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
        ch.bloomFilterOffset = bloomTokensBuf.items.len;
        try bloomTokensBuf.appendSlice(allocator, bloomHash);
    }

    fn getBloomBufferIndex(self: *StreamWriter, key: []const u8) error{MessageBloomMustBeUsed}!u16 {
        if (key.len == 0) {
            return error.MessageBloomMustBeUsed;
        }

        const colID = self.columnIDGen.genIDAssumeCapacity(key);
        const maybeColI = self.colIdx.get(colID);
        if (maybeColI) |colI| {
            return colI;
        }

        // at the moment implemented only for an in mem table, so we assume max col i is ever 1
        const colI = self.nextColI % self.maxColI;
        self.nextColI += 1;
        self.colIdx.putAssumeCapacity(colID, colI);

        if (colI >= self.bloomValuesList.items.len) {
            self.bloomValuesList.appendAssumeCapacity(std.ArrayList(u8).empty);
            self.bloomTokensList.appendAssumeCapacity(std.ArrayList(u8).empty);
        }

        return colI;
    }

    fn writeColumnsHeader(
        self: *StreamWriter,
        allocator: std.mem.Allocator,
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

        bh.columnsHeaderOffset = self.columnsHeaderBuf.items.len;
        bh.columnsHeaderSize = cshOffset;
        try self.columnsHeaderBuf.appendSlice(allocator, dst[0..cshOffset]);

        bh.columnsHeaderIndexOffset = self.columnsHeaderIndexBuf.items.len;
        bh.columnsHeaderIndexSize = cshIdxOffset;
        try self.columnsHeaderIndexBuf.appendSlice(allocator, dst[cshOffset .. cshOffset + cshIdxOffset]);
    }
};
