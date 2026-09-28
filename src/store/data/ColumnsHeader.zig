const std = @import("std");
const Allocator = std.mem.Allocator;

const encoding = @import("encoding");
const Encoder = encoding.Encoder;
const Decoder = encoding.Decoder;

const ColumnHeader = @import("ColumnHeader.zig");
const Column = @import("Column.zig");
const Block = @import("Block.zig");
const BlockData = @import("BlockData.zig").BlockData;
const ColumnDict = @import("ColumnDict.zig");
const ColumnsHeaderIndex = @import("ColumnsHeaderIndex.zig");
const ColumnIDGen = @import("ColumnIDGen.zig");

pub const ColumnsHeader = @This();
headers: []ColumnHeader,
invariantColumns: []Column,
/// When true, deinit owns and frees invariantColumns and each column's values (decode path).
/// TODO: find a workaround for clear ownership instead of a flag
ownsInvariantColumns: bool = false,

pub fn initFromBlock(alloc: Allocator, block: *const Block) !*ColumnsHeader {
    return init(alloc, block.getColumns().len, block.getInvariantColumns());
}

pub fn initFromData(alloc: Allocator, data: *const BlockData) !*ColumnsHeader {
    return init(alloc, data.columnsData.items.len, data.invariantColumns orelse &[_]Column{});
}

fn init(alloc: Allocator, colsLen: usize, invariants: []Column) !*ColumnsHeader {
    const headers = try alloc.alloc(ColumnHeader, colsLen);
    errdefer alloc.free(headers);
    var inited: u16 = 0;
    errdefer {
        for (0..inited) |i| {
            headers[i].dict.deinit(alloc);
        }
    }
    {
        for (0..headers.len) |i| {
            headers[i].dict = try ColumnDict.init(alloc);
            inited += 1;
        }
    }

    const ch = try alloc.create(ColumnsHeader);
    ch.* = .{
        .headers = headers,
        .invariantColumns = invariants,
    };

    return ch;
}

pub fn deinit(self: *ColumnsHeader, allocator: Allocator) void {
    for (0..self.headers.len) |i| {
        self.headers[i].dict.deinit(allocator);
    }
    allocator.free(self.headers);
    if (self.ownsInvariantColumns) {
        for (self.invariantColumns) |*column| allocator.free(column.values);
        allocator.free(self.invariantColumns);
    }
    allocator.destroy(self);
}

// [headers len][headers][columns len][columns]
pub fn encodeBound(self: *const ColumnsHeader) usize {
    var size: usize = 0;

    var headersSize: usize = 0;
    for (self.headers) |*header| {
        headersSize += header.encodeBound();
    }
    // Headers length varint
    size += Encoder.varIntBound(headersSize);
    // Sum of all header bounds
    size += headersSize;

    var invariantSize: usize = 0;
    for (self.invariantColumns) |*col| {
        invariantSize += col.invariantBound(false);
    }
    // invariant columns length varint
    size += Encoder.varIntBound(invariantSize);
    // Sum of all invariant column bounds
    size += invariantSize;

    return size;
}
pub fn encode(
    self: *ColumnsHeader,
    dst: []u8,
    cshIdx: *ColumnsHeaderIndex,
    columnIDGen: *ColumnIDGen,
) usize {
    var enc = Encoder.init(dst);
    enc.writeVarInt(self.headers.len);
    var offset = enc.offset;

    for (self.headers) |*header| {
        const colID = columnIDGen.genIDAssumeCapacity(header.key);
        header.encode(&enc);
        cshIdx.appendColumnAssumeCapacity(.{ .id = colID, .offset = @intCast(offset) });
        offset = enc.offset;
    }

    enc.writeVarInt(self.invariantColumns.len);
    offset = enc.offset;

    for (self.invariantColumns) |*invariantCol| {
        const colID = columnIDGen.genIDAssumeCapacity(invariantCol.key);
        invariantCol.encodeAsInvariant(&enc, false);
        cshIdx.appendInvariantColumnAssumeCapacity(.{ .id = colID, .offset = @intCast(offset) });
        offset = enc.offset;
    }

    return enc.offset;
}

pub fn decode(
    allocator: Allocator,
    buf: []const u8,
    cshIdx: *const ColumnsHeaderIndex,
    columnIDGen: *const ColumnIDGen,
) !*ColumnsHeader {
    var dec = Decoder.init(buf);

    const headersLen = dec.readVarInt();
    const headers = try allocator.alloc(ColumnHeader, headersLen);
    var headersDecoded: usize = 0;
    errdefer {
        for (headers[0..headersDecoded]) |*header| header.dict.deinit(allocator);
        allocator.free(headers);
    }

    for (0..headersLen) |i| {
        const colID = cshIdx.columnID(i);
        const key = columnIDGen.keyIDs.keys()[colID];
        headers[i] = try ColumnHeader.decode(&dec, key, allocator);
        headersDecoded += 1;
    }

    const invariantLen = dec.readVarInt();
    const invariantColumns = try allocator.alloc(Column, invariantLen);
    var invariantDecoded: usize = 0;
    errdefer {
        for (invariantColumns[0..invariantDecoded]) |*column| allocator.free(column.values);
        allocator.free(invariantColumns);
    }

    for (0..invariantLen) |i| {
        const colID = cshIdx.invariantColumnID(i);
        invariantColumns[i] = try Column.decodeAsInvariant(&dec, allocator, false);
        invariantColumns[i].key = columnIDGen.keyIDs.keys()[colID];
        invariantDecoded = i + 1;
    }

    const ch = try allocator.create(ColumnsHeader);
    ch.* = .{
        .headers = headers,
        .invariantColumns = invariantColumns,
        .ownsInvariantColumns = true,
    };

    return ch;
}

test {
    _ = @import("ColumnsHeader_test.zig");
}
