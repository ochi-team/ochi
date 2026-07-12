const std = @import("std");
const Allocator = std.mem.Allocator;
const Io = std.Io;

const encoding = @import("encoding");

const fs = @import("../../fs.zig");
const filenames = @import("../../filenames.zig");
const Table = @import("../data/Table.zig");
const ColumnIDGen = @import("ColumnIDGen.zig");
const CompressionPool = @import("../compression/CompressionPool.zig");
const DecompressionPool = @import("../compression/DecompressionPool.zig");

// TODO: check maybe i don't need allocator.create
pub const TableReader = @This();
table: *const Table,
metaIndexBuf: []const u8,

columnsKeysBuf: []const u8,
columnIdxsBuf: []const u8,

// TODO: consider making it as a value, not a pointer
// TODO: decode manually when it comes to file reader
columnIDGen: *ColumnIDGen,
colIdx: *std.AutoHashMap(u16, u16),

// TODO: these flags don't look smart,
// there must be a better idea to track ownership
ownsBuffers: bool = false,
ownsMetadata: bool = false,

pub fn init(io: Io, allocator: Allocator, table: *const Table, decompressionPool: *DecompressionPool) !*TableReader {
    switch (table.inner) {
        .mem => return initFromMem(io, allocator, table, decompressionPool),
        .disk => return initFromDisk(io, allocator, table, decompressionPool),
    }
}

pub fn initFromMem(io: Io, allocator: Allocator, table: *const Table, decompressionPool: *DecompressionPool) !*TableReader {
    const memTable = table.inner.mem;
    // TODO: this could be taken from a stream writer theoretically
    const columnIDGen = if (memTable.columnKeysBuf.items.len > 0)
        try ColumnIDGen.decode(io, allocator, decompressionPool, memTable.columnKeysBuf.items)
    else
        try ColumnIDGen.init(allocator);
    errdefer columnIDGen.deinit(allocator);

    const colIdx = try decodeColumnIdxs(allocator, memTable.columnIdxsBuf.items);
    errdefer {
        colIdx.deinit();
        allocator.destroy(colIdx);
    }

    const r = try allocator.create(TableReader);
    r.* = TableReader{
        .table = table,
        .metaIndexBuf = memTable.metaIndexBuf.items,

        .columnIDGen = columnIDGen,
        .colIdx = colIdx,
        .columnsKeysBuf = memTable.columnKeysBuf.items,
        .columnIdxsBuf = memTable.columnIdxsBuf.items,
        .ownsMetadata = true,
    };
    return r;
}

// TODO: instead of reusing the table files open them again due to:
// - keep the query cache clean
// - manage different fadvise for different descriptors
// also benchmark against mmap since it requires only reading
// - based on it reimplement totalBytesRead
fn initFromDisk(io: Io, alloc: Allocator, table: *const Table, decompressionPool: *DecompressionPool) !*TableReader {
    var pathBuf: [std.fs.max_path_bytes]u8 = undefined;
    var pathWriter = std.Io.Writer.fixed(&pathBuf);

    const path = table.path;
    // TODO: open files in parallel to speed up high-latency storage.
    try std.fs.path.fmtJoin(&.{ path, filenames.columnIdxs }).format(&pathWriter);
    const columnIdxsBuf = try fs.readAll(io, alloc, pathWriter.buffered());
    errdefer alloc.free(columnIdxsBuf);

    pathWriter.end = 0;
    try std.fs.path.fmtJoin(&.{ path, filenames.metaindex }).format(&pathWriter);
    const metaIndexBuf = try fs.readAll(io, alloc, pathWriter.buffered());
    errdefer alloc.free(metaIndexBuf);

    pathWriter.end = 0;
    try std.fs.path.fmtJoin(&.{ path, filenames.columnKeys }).format(&pathWriter);
    const columnsKeysBuf = try fs.readAll(io, alloc, pathWriter.buffered());
    errdefer alloc.free(columnsKeysBuf);

    var columnIDGen: *ColumnIDGen = undefined;
    if (columnsKeysBuf.len > 0) {
        columnIDGen = try ColumnIDGen.decode(io, alloc, decompressionPool, columnsKeysBuf);
    } else {
        columnIDGen = try ColumnIDGen.init(alloc);
    }
    errdefer columnIDGen.deinit(alloc);

    const colIdx = try decodeColumnIdxs(alloc, columnIdxsBuf);
    errdefer {
        colIdx.deinit();
        alloc.destroy(colIdx);
    }

    const tableReader = try alloc.create(TableReader);

    tableReader.* = .{
        .table = table,
        .metaIndexBuf = metaIndexBuf,
        .columnIDGen = columnIDGen,
        .colIdx = colIdx,
        .columnsKeysBuf = columnsKeysBuf,
        .columnIdxsBuf = columnIdxsBuf,
        .ownsBuffers = true,
    };
    return tableReader;
}

pub fn deinit(self: *TableReader, allocator: Allocator) void {
    if (!self.ownsBuffers) {
        if (self.ownsMetadata) {
            self.columnIDGen.deinit(allocator);
            self.colIdx.deinit();
            allocator.destroy(self.colIdx);
        }
        allocator.destroy(self);
        return;
    }

    allocator.free(self.metaIndexBuf);
    allocator.free(self.columnsKeysBuf);
    allocator.free(self.columnIdxsBuf);

    self.columnIDGen.deinit(allocator);

    self.colIdx.deinit();
    allocator.destroy(self.colIdx);

    allocator.destroy(self);
}

/// totalBytesRead returns the total number of bytes read from all buffers.
pub fn totalBytesRead(self: *const TableReader) u64 {
    return self.table.tableHeader().compressedSize;
}

pub fn readIndex(
    self: *const TableReader,
    io: Io,
    buf: []u8,
    offset: u64,
) !usize {
    return self.table.readIndex(io, buf, offset);
}

pub fn readTimestamps(
    self: *const TableReader,
    io: Io,
    buf: []u8,
    offset: u64,
) !usize {
    return self.table.readTimestamps(io, buf, offset);
}

pub fn readColumnsHeaderIndex(
    self: *const TableReader,
    io: Io,
    buf: []u8,
    offset: u64,
) !usize {
    return self.table.readColumnsHeaderIndex(io, buf, offset);
}

pub fn readColumnsHeader(
    self: *const TableReader,
    io: Io,
    buf: []u8,
    offset: u64,
) !usize {
    return self.table.readColumnsHeader(io, buf, offset);
}

pub fn readBloomValues(
    self: *const TableReader,
    io: Io,
    buf: []u8,
    key: []const u8,
    offset: u64,
) !usize {
    if (key.len == 0) {
        return self.table.readMessageBloomValues(io, buf, offset);
    }

    const colID = self.columnIDGen.keyIDs.get(key).?;
    const bloomBufI = self.colIdx.get(colID).?;
    return self.table.readBloomValues(io, buf, offset, bloomBufI);
}

pub fn readBloomTokens(
    self: *const TableReader,
    io: Io,
    buf: []u8,
    key: []const u8,
    offset: u64,
) !usize {
    if (key.len == 0) {
        return self.table.readMessageBloomTokens(io, buf, offset);
    }

    const colID = self.columnIDGen.keyIDs.get(key).?;
    const bloomBufI = self.colIdx.get(colID).?;
    return self.table.readBloomTokens(io, buf, offset, bloomBufI);
}

fn decodeColumnIdxs(alloc: Allocator, src: []const u8) !*std.AutoHashMap(u16, u16) {
    const colIdx = try alloc.create(std.AutoHashMap(u16, u16));
    errdefer alloc.destroy(colIdx);

    colIdx.* = std.AutoHashMap(u16, u16).init(alloc);
    errdefer colIdx.deinit();

    if (src.len == 0) {
        return colIdx;
    }

    var dec = encoding.Decoder.init(src);
    const count = dec.readVarInt();
    try colIdx.ensureTotalCapacity(@intCast(count));

    for (0..count) |_| {
        const colID: u16 = @intCast(dec.readVarInt());
        const shardIdx: u16 = @intCast(dec.readVarInt());
        colIdx.putAssumeCapacity(colID, shardIdx);
    }

    std.debug.assert(dec.offset == src.len);
    return colIdx;
}
