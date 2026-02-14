const std = @import("std");

const SID = @import("../lines.zig").SID;
const Column = @import("Column.zig");
const BlockHeader = @import("block_header.zig").BlockHeader;
const TimestampsHeader = @import("block_header.zig").TimestampsHeader;
const ColumnHeader = @import("block_header.zig").ColumnHeader;
const ColumnsHeader = @import("block_header.zig").ColumnsHeader;
const ColumnsHeaderIndex = @import("ColumnsHeaderIndex.zig");
const ColumnDict = @import("ColumnDict.zig");
const ColumnType = @import("block_header.zig").ColumnType;
const EncodingType = @import("TimestampsEncoder.zig").EncodingType;
const StreamReader = @import("reader.zig").StreamReader;

// TODO: make it gloabal, potentially it can be used as a global constant by others
const maxTimestampsBlockSize = 8 * 1024 * 1024;
const maxValuesBlockSize = 8 * 1024 * 1024;
const maxColumnsHeaderSize = 8 * 1024 * 1024;
const maxColumnsHeaderIndexSize = 8 * 1024 * 1024;

pub const BlockData = struct {
    sid: SID = undefined,
    uncompressedSizeBytes: u64 = 0,
    rowsCount: u32 = 0,

    timestampsData: TimestampsData,
    columnsHeader: ?*ColumnsHeader = null,
    columnsData: std.ArrayList(ColumnData),
    celledColumns: ?[]Column = null,

    pub fn initEmpty() BlockData {
        return .{ .columnsData = std.ArrayList(ColumnData).empty, .timestampsData = .{} };
    }

    pub fn reset(self: *BlockData, allocator: std.mem.Allocator) void {
        self.sid = undefined;
        self.uncompressedSizeBytes = 0;
        self.rowsCount = 0;

        self.timestampsData = .{};
        self.columnsData.clearRetainingCapacity();
        self.celledColumns = null;

        if (self.columnsHeader) |ch| {
            ch.deinit(allocator);
            self.columnsHeader = null;
        }
    }

    pub fn deinit(self: *BlockData, allocator: std.mem.Allocator) void {
        self.columnsData.deinit(allocator);
        if (self.columnsHeader) |ch| {
            ch.deinit(allocator);
        }
    }

    pub fn readFrom(
        self: *BlockData,
        allocator: std.mem.Allocator,
        bh: *const BlockHeader,
        sr: *StreamReader,
    ) !void {
        self.reset(allocator);

        self.sid = bh.sid;
        self.uncompressedSizeBytes = bh.size;
        self.rowsCount = bh.len;

        self.timestampsData = try TimestampsData.readFrom(&bh.timestampsHeader, sr);

        const columnsHeaderSize = bh.columnsHeaderSize;
        std.debug.assert(columnsHeaderSize <= maxColumnsHeaderSize);

        const columnsHeaderBuf = sr.columnsHeaderBuf[bh.columnsHeaderOffset..][0..columnsHeaderSize];

        // --- index ---
        const columnsHeaderIndexSize = bh.columnsHeaderIndexSize;
        std.debug.assert(columnsHeaderIndexSize <= maxColumnsHeaderIndexSize);

        const columnsHeaderIndexBuf = sr.columnsHeaderIndexBuf[bh.columnsHeaderIndexOffset..][0..columnsHeaderIndexSize];

        const cshIdx = try ColumnsHeaderIndex.decode(
            allocator,
            columnsHeaderIndexBuf,
        );
        defer cshIdx.deinit(allocator);

        self.columnsHeader = try ColumnsHeader.decode(
            allocator,
            columnsHeaderBuf,
            cshIdx,
            sr.columnIDGen,
        );

        const columnsHeader = self.columnsHeader.?;

        try self.columnsData.ensureTotalCapacity(allocator, columnsHeader.headers.len);

        for (columnsHeader.headers) |*ch| {
            const col = try ColumnData.readFrom(ch, sr);
            self.columnsData.appendAssumeCapacity(col);
        }

        self.celledColumns = columnsHeader.celledColumns;
    }
};

pub const TimestampsData = struct {
    data: []const u8 = undefined,

    encodingType: EncodingType = .Undefined,

    minTimestamp: u64 = 0,

    maxTimestamp: u64 = 0,

    pub fn readFrom(
        th: *const TimestampsHeader,
        sr: *const StreamReader,
    ) !TimestampsData {
        const timestampsBlockSize = th.size;
        std.debug.assert(timestampsBlockSize <= maxTimestampsBlockSize);

        return .{
            .data = sr.timestampsBuf[th.offset..][0..timestampsBlockSize],
            .encodingType = th.encodingType,
            .minTimestamp = th.min,
            .maxTimestamp = th.max,
        };
    }
};

pub const ColumnData = struct {
    name: []const u8,
    valueType: ColumnType,

    minValue: u64,
    maxValue: u64,

    valuesDict: *const ColumnDict,
    valuesData: []const u8,

    bloomFilterData: ?[]const u8,

    pub fn readFrom(
        ch: *const ColumnHeader,
        sr: *const StreamReader,
    ) !ColumnData {
        const colID = sr.columnIDGen.keyIDs.get(ch.key).?;
        const bloomBufI = sr.colIdx.get(colID).?;

        const valuesSize = ch.size;
        std.debug.assert(valuesSize <= maxValuesBlockSize);

        const bloomValuesBuf = sr.bloomValuesList.items[bloomBufI];
        const valuesData = bloomValuesBuf.items[ch.offset..][0..valuesSize];

        var bloomFilterData: ?[]const u8 = null;

        if (ch.type != .dict) {
            const bloomTokensBuf = sr.bloomTokensList.items[bloomBufI];
            bloomFilterData = bloomTokensBuf.items[ch.bloomFilterOffset..][0..ch.bloomFilterSize];
        }

        return .{
            .name = ch.key,
            .valueType = ch.type,

            .minValue = ch.min,
            .maxValue = ch.max,

            .valuesDict = &ch.dict,
            .valuesData = valuesData,

            .bloomFilterData = bloomFilterData,
        };
    }
};

const Line = @import("../lines.zig").Line;
const Field = @import("../lines.zig").Field;
const TableMem = @import("TableMem.zig");
const BlockReader = @import("reader.zig").BlockReader;

test "BlockData initEmpty and deinit without header" {
    var bd = BlockData.initEmpty();
    try std.testing.expectEqual(@as(?*ColumnsHeader, null), bd.columnsHeader);
    try std.testing.expectEqual(@as(?[]Column, null), bd.celledColumns);

    // Should not crash when deinit is called with no decoded data.
    bd.deinit(std.testing.allocator);
}

const SampleLines = struct {
    fields1: [2]Field,
    fields2: [2]Field,
    fields3: [2]Field,
    lines: [3]Line,
};

fn populateSampleLines(sample: *SampleLines) void {
    sample.fields1 = .{
        .{ .key = "level", .value = "info" },
        .{ .key = "app", .value = "seq" },
    };
    sample.fields2 = .{
        .{ .key = "level", .value = "warn" },
        .{ .key = "app", .value = "seq" },
    };
    sample.fields3 = .{
        .{ .key = "level", .value = "warn" },
        .{ .key = "app", .value = "seq" },
    };
    sample.lines = .{
        .{
            .timestampNs = 1,
            .sid = .{ .id = 2, .tenantID = "2222" },
            .fields = sample.fields1[0..],
        },
        .{
            .timestampNs = 2,
            .sid = .{ .id = 1, .tenantID = "1111" },
            .fields = sample.fields2[0..],
        },
        .{
            .timestampNs = 3,
            .sid = .{ .id = 1, .tenantID = "1111" },
            .fields = sample.fields3[0..],
        },
    };
}

test "BlockData readFrom populates columnsData and celledColumns" {
    const allocator = std.testing.allocator;

    var sample: SampleLines = .{
        .fields1 = undefined,
        .fields2 = undefined,
        .fields3 = undefined,
        .lines = undefined,
    };
    populateSampleLines(&sample);

    var lines = [3]*const Line{
        &sample.lines[0],
        &sample.lines[1],
        &sample.lines[2],
    };

    const memTable = try TableMem.init(allocator);
    defer memTable.deinit(allocator);
    try memTable.addLines(allocator, lines[0..]);

    const blockReader = try BlockReader.initFromTableMem(allocator, memTable);
    defer blockReader.deinit(allocator);

    // Read first block, which should populate BlockData.
    try std.testing.expect(try blockReader.nextBlock(allocator));

    const bd = &blockReader.blockData;
    try std.testing.expect(bd.columnsHeader != null);
    const ch = bd.columnsHeader.?;

    // BlockData must mirror the number of column headers.
    try std.testing.expectEqual(ch.headers.len, bd.columnsData.items.len);

    // When there are any column headers, each ColumnData should correspond to its ColumnHeader.
    for (ch.headers, bd.columnsData.items) |*header, col| {
        try std.testing.expectEqualStrings(header.key, col.name);
        try std.testing.expectEqual(header.type, col.valueType);
        try std.testing.expectEqual(header.size, col.valuesData.len);
        try std.testing.expectEqual(&header.dict, col.valuesDict);
    }

    // Second call to nextBlock exercises BlockData reuse path (columnsHeader deinit + re-decode).
    _ = try blockReader.nextBlock(allocator);
}
