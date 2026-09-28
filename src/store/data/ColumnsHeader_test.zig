const std = @import("std");

const ColumnHeader = @import("ColumnHeader.zig");
const Column = @import("Column.zig");
const ColumnDict = @import("ColumnDict.zig");
const ColumnsHeaderIndex = @import("ColumnsHeaderIndex.zig");
const ColumnIDGen = @import("ColumnIDGen.zig");

pub const ColumnsHeader = @import("ColumnsHeader.zig");

const testing = std.testing;

test "ColumnsHeaderEncode" {
    const alloc = testing.allocator;

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
    try testing.expectEqual(headers.len, decodedHeader.headers.len);
    for (headers, decodedHeader.headers) |orig, decoded| {
        try testing.expectEqualDeep(orig, decoded);
    }

    try testing.expectEqual(invariantColumns.len, decodedHeader.invariantColumns.len);
    for (invariantColumns, decodedHeader.invariantColumns) |orig, decoded| {
        try testing.expectEqualDeep(orig, decoded);
    }
}
