// TODO: find a better name,
// e.g. Block -> BlockBuffer, BlockData -> Block, confirm that BlockBuffer is used only in ingestion mem tables,
// if so - perfect fit
const std = @import("std");
const Io = std.Io;
const Allocator = std.mem.Allocator;

const SID = @import("../lines.zig").SID;
const Column = @import("Column.zig");
const Block = @import("Block.zig");
const BlockHeader = @import("BlockHeader.zig");
const ColumnsHeader = @import("ColumnsHeader.zig");
const ColumnsHeaderIndex = @import("ColumnsHeaderIndex.zig");
const TableReader = @import("TableReader.zig");

const ColumnData = @import("ColumnData.zig");
const TimestampsData = @import("TimestampsData.zig");

// TODO: make it gloabal, potentially it can be used as a global constant by others
// TODO: perhaps we should apply equal limits to every file type and name it like maxBlockSegmentSize
// meaning it's a segment of a block we plan to store in its file
pub const maxTimestampsBlockSize = 8 * 1024 * 1024;
pub const maxValuesBlockSize = 8 * 1024 * 1024;
pub const maxBloomTokensBlockSize = 8 * 1024 * 1024;
pub const maxColumnsHeaderSize = 8 * 1024 * 1024;
pub const maxColumnsHeaderIndexSize = 8 * 1024 * 1024;

pub const BlockData = @This();
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
// TODO: try making it non nullable or document why it must be so
columnsHeader: ?*ColumnsHeader = null,
columnsData: std.ArrayList(ColumnData),
// TODO: consider making it as a Field,
// it might make ingestion more copies, but reading is lighter
invariantColumns: ?[]Column = null,

pub fn initEmpty() BlockData {
    return .{ .columnsData = std.ArrayList(ColumnData).empty, .timestampsData = .{} };
}

/// resetArena assumes it's owned by an arena allocator,
/// so it doesn't free or clearRetainingCapacity
pub fn resetArena(self: *BlockData) void {
    self.sid = .{ .tenantID = 0, .id = 0 };
    self.uncompressedSizeBytes = 0;
    self.len = 0;

    self.timestampsData = .{};
    self.columnsData = .empty;
    self.invariantColumns = null;
    self.columnsHeader = null;

    self.columnsHeaderBuf = "";
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

pub fn readFrom(
    self: *BlockData,
    io: Io,
    alloc: std.mem.Allocator,
    bh: *const BlockHeader,
    sr: *const TableReader,
) !void {
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

    self.invariantColumns = columnsHeader.invariantColumns;
}
