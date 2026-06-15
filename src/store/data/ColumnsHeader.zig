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

pub fn initFromBlock(alloc: Allocator, block: *Block) !*ColumnsHeader {
    return init(alloc, block.getColumns().len, block.getInvariantColumns());
}

pub fn initFromData(alloc: Allocator, data: *BlockData) !*ColumnsHeader {
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

test "ColumnsHeaderEncode" {
    const alloc = std.testing.allocator;

    // Create ColumnIDGen and populate it with test keys
    const columnIDGen = try ColumnIDGen.init(alloc);
    defer columnIDGen.deinit(alloc);
    try columnIDGen.keyIDs.ensureUnusedCapacity(alloc, 5);
    _ = columnIDGen.genIDAssumeCapacity("col_string");
    _ = columnIDGen.genIDAssumeCapacity("col_dict");
    _ = columnIDGen.genIDAssumeCapacity("col_uint32");
    _ = columnIDGen.genIDAssumeCapacity("invariant_col1");
    _ = columnIDGen.genIDAssumeCapacity("invariant_col2");

    // Create test ColumnsHeader
    const headers = try alloc.alloc(ColumnHeader, 3);
    defer alloc.free(headers);

    // String column header (non-dict type, so dict is empty)
    headers[0] = .{
        .key = "col_string",
        .dict = ColumnDict{ .values = std.ArrayList([]const u8).empty },
        .type = .string,
        .min = 0,
        .max = 0,
        .size = 100,
        .offset = 1000,
        .bloomFilterSize = 50,
        .bloomFilterOffset = 2000,
    };

    // Dict column header (dict type, so dict has capacity)
    headers[1] = .{
        .key = "col_dict",
        .dict = try ColumnDict.init(alloc),
        .type = .dict,
        .min = 0,
        .max = 0,
        .size = 200,
        .offset = 1100,
        .bloomFilterSize = 0,
        .bloomFilterOffset = 0,
    };
    headers[1].dict.values.appendAssumeCapacity("value1");
    headers[1].dict.values.appendAssumeCapacity("value2");
    defer headers[1].dict.deinit(alloc);

    // Uint32 column header (non-dict type, so dict is empty)
    headers[2] = .{
        .key = "col_uint32",
        .dict = ColumnDict{ .values = std.ArrayList([]const u8).empty },
        .type = .uint32,
        .min = 10,
        .max = 1000,
        .size = 150,
        .offset = 1200,
        .bloomFilterSize = 60,
        .bloomFilterOffset = 2100,
    };

    // Create test invariant columns
    const invariantColumns = try alloc.alloc(Column, 2);
    defer alloc.free(invariantColumns);

    const invariantValues1 = try alloc.alloc([]const u8, 1);
    invariantValues1[0] = "invariant_value_1";
    defer alloc.free(invariantValues1);

    const invariantValues2 = try alloc.alloc([]const u8, 1);
    invariantValues2[0] = "invariant_value_2";
    defer alloc.free(invariantValues2);

    invariantColumns[0] = .{
        .key = "invariant_col1",
        .values = invariantValues1,
    };

    invariantColumns[1] = .{
        .key = "invariant_col2",
        .values = invariantValues2,
    };

    var columnsHeader = ColumnsHeader{
        .headers = headers,
        .invariantColumns = invariantColumns,
    };

    // Create ColumnsHeaderIndex
    var columnIDs: [6]u16 = undefined;
    var columnOffsets: [6]u32 = undefined;
    var cshIdx = ColumnsHeaderIndex.initBufferKnown(&columnIDs, &columnOffsets, 3);

    // Encode
    const encodeBoundSize = columnsHeader.encodeBound();
    const encodeBuf = try alloc.alloc(u8, encodeBoundSize);
    defer alloc.free(encodeBuf);

    const encodedSize = columnsHeader.encode(encodeBuf, &cshIdx, columnIDGen);

    // Decode
    const decodedHeader = try ColumnsHeader.decode(
        alloc,
        encodeBuf[0..encodedSize],
        &cshIdx,
        columnIDGen,
    );
    defer decodedHeader.deinit(alloc);

    // Verify using deep comparison
    try std.testing.expectEqual(headers.len, decodedHeader.headers.len);
    for (headers, decodedHeader.headers) |orig, decoded| {
        try std.testing.expectEqualDeep(orig, decoded);
    }

    try std.testing.expectEqual(invariantColumns.len, decodedHeader.invariantColumns.len);
    for (invariantColumns, decodedHeader.invariantColumns) |orig, decoded| {
        try std.testing.expectEqualDeep(orig, decoded);
    }
}
