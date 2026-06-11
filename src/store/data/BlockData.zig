// TODO: find a better name
const std = @import("std");
const Io = std.Io;
const Allocator = std.mem.Allocator;

const SID = @import("../lines.zig").SID;
const Column = @import("Column.zig");
const Block = @import("Block.zig");
const BlockHeader = @import("BlockHeader.zig");
const TimestampsHeader = BlockHeader.TimestampsHeader;
const ColumnHeader = @import("ColumnHeader.zig");
const ColumnsHeader = @import("ColumnsHeader.zig");
const ColumnsHeaderIndex = @import("ColumnsHeaderIndex.zig");
const ColumnDict = @import("ColumnDict.zig");
const ColumnType = ColumnHeader.ColumnType;
const EncodingType = @import("TimestampsEncoder.zig").EncodingType;
const TableReader = @import("TableReader.zig");

// TODO: make it gloabal, potentially it can be used as a global constant by others
// TODO: perhaps we should apply equal limits to every file type and name it like maxBlockSegmentSize
// meaning it's a segment of a block we plan to store in its file
pub const maxTimestampsBlockSize = 8 * 1024 * 1024;
pub const maxValuesBlockSize = 8 * 1024 * 1024;
pub const maxBloomTokensBlockSize = 8 * 1024 * 1024;
pub const maxColumnsHeaderSize = 8 * 1024 * 1024;
pub const maxColumnsHeaderIndexSize = 8 * 1024 * 1024;

// TODO: move data segments to its file in the /data package
pub const BlockData = struct {
    sid: SID = undefined,
    // TODO: audit in the codebase the usage of compressed and uncompressed sizes,
    // find a better name for both to refleect the data lifecycle (e.g. content size and data size,
    // when a content is given request from the ingestor, data is what we write to the tables)
    uncompressedSizeBytes: u64 = 0,
    len: u32 = 0,

    timestampsData: TimestampsData,
    // holds read buffer ownership, coupled to columnsHeader lifetime
    // TODO: this holds ownership of merge read, either document it's ownership
    // or remove if we migrate ot file read / mmap
    columnsHeaderBuf: []const u8 = "",
    // TODO: try making it non nullable
    columnsHeader: ?*ColumnsHeader = null,
    columnsData: std.ArrayList(ColumnData),
    // TODO: consider making it as a Field,
    // it might make ingestion more copies, but reading is lighter
    celledColumns: ?[]Column = null,

    pub fn initEmpty() BlockData {
        return .{ .columnsData = std.ArrayList(ColumnData).empty, .timestampsData = .{} };
    }

    pub fn reset(self: *BlockData, allocator: Allocator) void {
        self.sid = .{ .tenantID = 0, .id = 0 };
        self.uncompressedSizeBytes = 0;
        self.len = 0;

        self.timestampsData.deinit(allocator);
        for (self.columnsData.items) |*col| {
            col.deinit(allocator);
        }
        self.columnsData.clearRetainingCapacity();
        self.celledColumns = null;

        if (self.columnsHeader) |ch| {
            // TODO: make it reset instead
            ch.deinit(allocator);
            self.columnsHeader = null;
        }
        if (self.columnsHeaderBuf.len > 0) {
            allocator.free(self.columnsHeaderBuf);
            self.columnsHeaderBuf = "";
        }
    }

    pub fn deinit(self: *BlockData, alloc: Allocator) void {
        for (self.columnsData.items) |*col| {
            col.deinit(alloc);
        }
        self.columnsData.deinit(alloc);
        if (self.columnsHeader) |ch| {
            ch.deinit(alloc);
        }
        if (self.columnsHeaderBuf.len > 0) {
            alloc.free(self.columnsHeaderBuf);
        }
        self.timestampsData.deinit(alloc);
    }

    // TODO: assign this API to a reader instead so we could have data as a pure data models
    pub fn readFrom(
        self: *BlockData,
        io: Io,
        alloc: Allocator,
        bh: *const BlockHeader,
        sr: *const TableReader,
    ) !void {
        self.reset(alloc);

        self.sid = bh.sid;
        self.uncompressedSizeBytes = bh.size;
        self.len = bh.len;

        const timestampsBuf = try alloc.alloc(u8, bh.timestampsHeader.size);
        // move immediately to timestamps to being able to deinit on error
        self.timestampsData.data = timestampsBuf;
        errdefer self.timestampsData.deinit(alloc);
        self.timestampsData = try TimestampsData.readFrom(io, timestampsBuf, &bh.timestampsHeader, sr);

        const columnsHeaderSize = bh.columnsHeaderSize;
        std.debug.assert(columnsHeaderSize <= maxColumnsHeaderSize);

        const columnsHeaderBuf = try alloc.alloc(u8, columnsHeaderSize);
        self.columnsHeaderBuf = columnsHeaderBuf;
        const columnsHeaderN = try sr.readColumnsHeader(io, columnsHeaderBuf, bh.columnsHeaderOffset);
        std.debug.assert(columnsHeaderN == columnsHeaderBuf.len);

        const columnsHeaderIndexSize = bh.columnsHeaderIndexSize;
        std.debug.assert(columnsHeaderIndexSize <= maxColumnsHeaderIndexSize);

        const columnsHeaderIndexBuf = try alloc.alloc(u8, columnsHeaderIndexSize);
        defer alloc.free(columnsHeaderIndexBuf);
        const columnsHeaderIndexN = try sr.readColumnsHeaderIndex(
            io,
            columnsHeaderIndexBuf,
            bh.columnsHeaderIndexOffset,
        );
        std.debug.assert(columnsHeaderIndexN == columnsHeaderIndexBuf.len);

        var columnIDs: [Block.maxColumns]u16 = undefined;
        var columnOffsets: [Block.maxColumns]u32 = undefined;
        var cshIdx = ColumnsHeaderIndex.initBufferUnknown(&columnIDs, &columnOffsets);
        cshIdx.decode(columnsHeaderIndexBuf);

        self.columnsHeader = try ColumnsHeader.decode(
            alloc,
            columnsHeaderBuf,
            &cshIdx,
            sr.columnIDGen,
        );

        const columnsHeader = self.columnsHeader.?;

        try self.columnsData.ensureTotalCapacity(alloc, columnsHeader.headers.len);

        for (columnsHeader.headers) |*ch| {
            const col = try ColumnData.readFrom(io, alloc, ch, sr);
            self.columnsData.appendAssumeCapacity(col);
        }

        self.celledColumns = columnsHeader.celledColumns;
    }
};

// TODO: move TimestampsData and ColumnData inside BlockData
pub const TimestampsData = struct {
    data: []const u8 = "",

    encodingType: EncodingType = .Undefined,

    minTimestamp: u64 = 0,
    maxTimestamp: u64 = 0,

    pub fn readFrom(
        io: Io,
        buf: []u8,
        th: *const TimestampsHeader,
        sr: *const TableReader,
    ) !TimestampsData {
        std.debug.assert(buf.len <= maxTimestampsBlockSize);

        const n = try sr.readTimestamps(io, buf, th.offset);
        std.debug.assert(n == buf.len);

        return .{
            .data = buf,
            .encodingType = th.encodingType,
            .minTimestamp = th.min,
            .maxTimestamp = th.max,
        };
    }

    pub fn deinit(self: *TimestampsData, alloc: Allocator) void {
        if (self.data.len > 0) alloc.free(self.data);
        self.* = .{};
    }
};

pub const ColumnData = struct {
    key: []const u8,
    type: ColumnType,

    min: u64,
    max: u64,

    // TODO: try making it a value, it stores a single array and used mostly as a value in the headers,
    // or the other way around,
    // it's important to note writeColumnData uses *ColumnDict in order to move ownership,
    // therefore it may require passing  a ColumndData as a pointer
    dict: *ColumnDict,
    // TODO: this holds ownership of merge read, either document it's ownership
    // or remove if we migrate ot file read / mmap
    bloomValues: []const u8,

    // TODO: try making it non optional, default as an empty string
    bloomTokens: ?[]const u8,

    pub fn readFrom(
        io: Io,
        alloc: Allocator,
        ch: *ColumnHeader,
        tableReader: *const TableReader,
    ) !ColumnData {
        const valuesSize = ch.size;
        std.debug.assert(valuesSize <= maxValuesBlockSize);

        const valuesData = try alloc.alloc(u8, valuesSize);
        errdefer alloc.free(valuesData);
        const valuesN = try tableReader.readBloomValues(io, valuesData, ch.key, ch.offset);
        std.debug.assert(valuesN == valuesData.len);

        var tokensData: ?[]const u8 = null;
        if (ch.type != .dict) {
            const tokensBuf = try alloc.alloc(u8, ch.bloomFilterSize);
            errdefer alloc.free(tokensBuf);
            const tokensN = try tableReader.readBloomTokens(
                io,
                tokensBuf,
                ch.key,
                ch.bloomFilterOffset,
            );
            std.debug.assert(tokensN == tokensBuf.len);
            tokensData = tokensBuf;
        }

        return .{
            .key = ch.key,
            .type = ch.type,

            .min = ch.min,
            .max = ch.max,

            .dict = &ch.dict,
            .bloomValues = valuesData,

            .bloomTokens = tokensData,
        };
    }

    pub fn deinit(self: *ColumnData, alloc: Allocator) void {
        if (self.bloomValues.len > 0) {
            alloc.free(self.bloomValues);
        }
        if (self.bloomTokens) |bloomTokens| {
            if (bloomTokens.len > 0) {
                alloc.free(bloomTokens);
            }
        }
        self.* = undefined;
    }
};

const Line = @import("../lines.zig").Line;
const Field = @import("../lines.zig").Field;
const MemTable = @import("MemTable.zig");
const BlockReader = @import("BlockReader.zig");
const Table = @import("../data/Table.zig");

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
            .sid = .{ .id = 2, .tenantID = 2222 },
            .fields = sample.fields1[0..],
        },
        .{
            .timestampNs = 2,
            .sid = .{ .id = 1, .tenantID = 1111 },
            .fields = sample.fields2[0..],
        },
        .{
            .timestampNs = 3,
            .sid = .{ .id = 1, .tenantID = 1111 },
            .fields = sample.fields3[0..],
        },
    };
}

test "BlockData readFrom populates columnsData and celledColumns" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var sample: SampleLines = .{
        .fields1 = undefined,
        .fields2 = undefined,
        .fields3 = undefined,
        .lines = undefined,
    };
    populateSampleLines(&sample);

    var lines = [3]Line{
        sample.lines[0],
        sample.lines[1],
        sample.lines[2],
    };

    const memTable = try MemTable.init(allocator);
    const table = try Table.fromMem(allocator, memTable);
    defer table.close(io);
    try memTable.addLines(io, allocator, lines[0..]);

    const blockReader = try BlockReader.initFromMemTable(allocator, table);
    defer blockReader.deinit(allocator);

    // Read first block, which should populate BlockData.
    try std.testing.expect(try blockReader.nextBlock(io, allocator));

    const bd = &blockReader.blockData;
    try std.testing.expect(bd.columnsHeader != null);
    const ch = bd.columnsHeader.?;

    // BlockData must mirror the number of column headers.
    try std.testing.expectEqual(ch.headers.len, bd.columnsData.items.len);

    // When there are any column headers, each ColumnData should correspond to its ColumnHeader.
    for (ch.headers, bd.columnsData.items) |*header, col| {
        try std.testing.expectEqualStrings(header.key, col.key);
        try std.testing.expectEqual(header.type, col.type);
        try std.testing.expectEqual(header.size, col.bloomValues.len);
        try std.testing.expectEqual(&header.dict, col.dict);
    }

    // Second call to nextBlock exercises BlockData reuse path (columnsHeader deinit + re-decode).
    _ = try blockReader.nextBlock(io, allocator);
}
