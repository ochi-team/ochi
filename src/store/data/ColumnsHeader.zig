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
celledColumns: []Column,
/// When true, deinit owns and frees celledColumns and each column's values (decode path).
/// TODO: find a workaround for clear ownership instead of a flag
owns_celled_columns: bool = false,

pub fn initFromBlock(alloc: Allocator, block: *Block) !*ColumnsHeader {
    return init(alloc, block.getColumns().len, block.getCelledColumns());
}

pub fn initFromData(alloc: Allocator, data: *BlockData) !*ColumnsHeader {
    return init(alloc, data.columnsData.items.len, data.celledColumns orelse &[_]Column{});
}

fn init(alloc: Allocator, colsLen: usize, cells: []Column) !*ColumnsHeader {
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
        .celledColumns = cells,
    };

    return ch;
}

pub fn deinit(self: *ColumnsHeader, allocator: Allocator) void {
    for (0..self.headers.len) |i| {
        self.headers[i].dict.deinit(allocator);
    }
    allocator.free(self.headers);
    if (self.owns_celled_columns) {
        for (self.celledColumns) |*column| allocator.free(column.values);
        allocator.free(self.celledColumns);
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

    var celledsize: usize = 0;
    for (self.celledColumns) |*col| {
        celledsize += col.celledBound(false);
    }
    // Celled columns length varint
    size += Encoder.varIntBound(celledsize);
    // Sum of all celled column bounds
    size += celledsize;

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
        cshIdx.columns.appendAssumeCapacity(.{
            .columndID = colID,
            .offset = offset,
        });
        offset = enc.offset;
    }

    enc.writeVarInt(self.celledColumns.len);
    offset = enc.offset;

    for (self.celledColumns) |*celledCol| {
        const colID = columnIDGen.genIDAssumeCapacity(celledCol.key);
        celledCol.encodeAsCelled(&enc, false);
        cshIdx.celledColumns.appendAssumeCapacity(.{
            .columndID = colID,
            .offset = offset,
        });
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
        const colID = cshIdx.columns.items[i].columndID;
        const key = columnIDGen.keyIDs.keys()[colID];
        headers[i] = try ColumnHeader.decode(&dec, key, allocator);
        headersDecoded += 1;
    }

    const celledLen = dec.readVarInt();
    const celledColumns = try allocator.alloc(Column, celledLen);
    var celledDecoded: usize = 0;
    errdefer {
        for (celledColumns[0..celledDecoded]) |*column| allocator.free(column.values);
        allocator.free(celledColumns);
    }

    for (0..celledLen) |i| {
        const colID = cshIdx.celledColumns.items[i].columndID;
        celledColumns[i] = try Column.decodeAsCelled(&dec, allocator, false);
        celledColumns[i].key = columnIDGen.keyIDs.keys()[colID];
        celledDecoded = i + 1;
    }

    const ch = try allocator.create(ColumnsHeader);
    ch.* = .{
        .headers = headers,
        .celledColumns = celledColumns,
        .owns_celled_columns = true,
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
    _ = columnIDGen.genIDAssumeCapacity("celled_col1");
    _ = columnIDGen.genIDAssumeCapacity("celled_col2");

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

    // Create test celled columns
    const celledColumns = try alloc.alloc(Column, 2);
    defer alloc.free(celledColumns);

    const celledValues1 = try alloc.alloc([]const u8, 1);
    celledValues1[0] = "constant_value_1";
    defer alloc.free(celledValues1);

    const celledValues2 = try alloc.alloc([]const u8, 1);
    celledValues2[0] = "constant_value_2";
    defer alloc.free(celledValues2);

    celledColumns[0] = .{
        .key = "celled_col1",
        .values = celledValues1,
    };

    celledColumns[1] = .{
        .key = "celled_col2",
        .values = celledValues2,
    };

    var columnsHeader = ColumnsHeader{
        .headers = headers,
        .celledColumns = celledColumns,
    };

    // Create ColumnsHeaderIndex
    var columnDescs: [3]ColumnsHeaderIndex.ColumnDesc = undefined;
    var celledColumnDescs: [3]ColumnsHeaderIndex.ColumnDesc = undefined;
    var cshIdx = ColumnsHeaderIndex.initBuffer(&columnDescs, &celledColumnDescs);

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

    try std.testing.expectEqual(celledColumns.len, decodedHeader.celledColumns.len);
    for (celledColumns, decodedHeader.celledColumns) |orig, decoded| {
        try std.testing.expectEqualDeep(orig, decoded);
    }
}
