const std = @import("std");
const Io = std.Io;
const Dir = Io.Dir;

const tracy = @import("tracy");

const fs = @import("../../fs.zig");
const Field = @import("../lines.zig").Field;
const Line = @import("../lines.zig").Line;
const lineLessThan = @import("../lines.zig").lineLessThan;
const fieldLessThan = @import("../lines.zig").fieldLessThan;
const maxLines = @import("Block.zig").maxLines;
const SID = @import("../lines.zig").SID;
const TimestampsEncoder = @import("TimestampsEncoder.zig");

const TableWriter = @import("TableWriter.zig");
const CompressionPool = @import("../compression/CompressionPool.zig");
const maxCheckpoints = @import("../../DataRecorder.zig").DataShard.maxCheckpoints;
const BlockWriter = @import("BlockWriter.zig");
const TableHeader = @import("TableHeader.zig");
const filenames = @import("../../filenames.zig");

const Consts = @import("../../Consts.zig");

const maxBlockSize = Consts.maxBlockSize;

// TODO: benchmark buffers size if we can build a better size
const tsBufferSize = 1024;
const indexBufferSize = 1024;
const metaIndexBufferSize = 1024;
const columnsHeaderBufferSize = 1024;
const columnsHeaderIndexBufferSize = 1024;
const bloomValuesSize = 1024;
const bloomTokensSize = 1024;
const columnKeysBufferSize = 512;
const columnIndexesBufferSize = 128;

pub const Error = error{
    EmptyLines,
    EmptySids,
};

const MemTable = @This();

// TODO: continuous buffers might be not very efficient on large size,
// 1. it can be a chunked buffer, an array of static buffers
// 2. or reused buffers with a known size
timestampsBuf: std.ArrayList(u8) = .empty,
indexBuf: std.ArrayList(u8) = .empty,
metaIndexBuf: std.ArrayList(u8) = .empty,

columnsHeaderBuf: std.ArrayList(u8) = .empty,
columnsHeaderIndexBuf: std.ArrayList(u8) = .empty,

columnKeysBuf: std.ArrayList(u8) = .empty,
columnIdxsBuf: std.ArrayList(u8) = .empty,

messageBloomValuesBuf: std.ArrayList(u8) = .empty,
messageBloomTokensBuf: std.ArrayList(u8) = .empty,
bloomValuesBuf: std.ArrayList(u8) = .empty,
bloomTokensBuf: std.ArrayList(u8) = .empty,

tableHeader: TableHeader,

flushAtUs: i64 = std.math.maxInt(i64),

pub fn init(allocator: std.mem.Allocator) !*MemTable {
    var timestampsBuf = try std.ArrayList(u8).initCapacity(allocator, tsBufferSize);
    errdefer timestampsBuf.deinit(allocator);
    var indexBuf = try std.ArrayList(u8).initCapacity(allocator, indexBufferSize);
    errdefer indexBuf.deinit(allocator);
    var metaIndexBuf = try std.ArrayList(u8).initCapacity(allocator, metaIndexBufferSize);
    errdefer metaIndexBuf.deinit(allocator);

    var columnsHeaderBuf = try std.ArrayList(u8).initCapacity(allocator, columnsHeaderBufferSize);
    errdefer columnsHeaderBuf.deinit(allocator);
    var columnsHeaderIndexBuf = try std.ArrayList(u8).initCapacity(allocator, columnsHeaderIndexBufferSize);
    errdefer columnsHeaderIndexBuf.deinit(allocator);

    var columnKeysBuf = try std.ArrayList(u8).initCapacity(allocator, columnKeysBufferSize);
    errdefer columnKeysBuf.deinit(allocator);
    var columnIdxsBuf = try std.ArrayList(u8).initCapacity(allocator, columnIndexesBufferSize);
    errdefer columnIdxsBuf.deinit(allocator);

    var msgBloomValuesBuf = try std.ArrayList(u8).initCapacity(allocator, bloomValuesSize);
    errdefer msgBloomValuesBuf.deinit(allocator);
    var msgBloomTokensBuf = try std.ArrayList(u8).initCapacity(allocator, bloomTokensSize);
    errdefer msgBloomTokensBuf.deinit(allocator);
    var bloomValuesBuf = try std.ArrayList(u8).initCapacity(allocator, bloomValuesSize);
    errdefer bloomValuesBuf.deinit(allocator);
    var bloomTokensBuf = try std.ArrayList(u8).initCapacity(allocator, bloomTokensSize);
    errdefer bloomTokensBuf.deinit(allocator);

    const p = try allocator.create(MemTable);
    errdefer allocator.destroy(p);
    p.* = MemTable{
        .tableHeader = .{},
        .timestampsBuf = timestampsBuf,
        .indexBuf = indexBuf,
        .metaIndexBuf = metaIndexBuf,
        .columnsHeaderBuf = columnsHeaderBuf,
        .columnsHeaderIndexBuf = columnsHeaderIndexBuf,
        .columnKeysBuf = columnKeysBuf,
        .columnIdxsBuf = columnIdxsBuf,
        .messageBloomValuesBuf = msgBloomValuesBuf,
        .messageBloomTokensBuf = msgBloomTokensBuf,
        .bloomValuesBuf = bloomValuesBuf,
        .bloomTokensBuf = bloomTokensBuf,
    };

    return p;
}
pub fn deinit(self: *MemTable, allocator: std.mem.Allocator) void {
    self.timestampsBuf.deinit(allocator);
    self.indexBuf.deinit(allocator);
    self.metaIndexBuf.deinit(allocator);

    self.columnsHeaderBuf.deinit(allocator);
    self.columnsHeaderIndexBuf.deinit(allocator);

    self.columnKeysBuf.deinit(allocator);
    self.columnIdxsBuf.deinit(allocator);

    self.messageBloomValuesBuf.deinit(allocator);
    self.messageBloomTokensBuf.deinit(allocator);
    self.bloomValuesBuf.deinit(allocator);
    self.bloomTokensBuf.deinit(allocator);

    allocator.destroy(self);
}

pub fn size(self: *const MemTable) u32 {
    var res: usize = self.timestampsBuf.items.len;
    res += self.indexBuf.items.len;
    res += self.metaIndexBuf.items.len;
    res += self.columnsHeaderBuf.items.len;
    res += self.columnsHeaderIndexBuf.items.len;
    res += self.columnKeysBuf.items.len;
    res += self.columnIdxsBuf.items.len;
    res += self.messageBloomValuesBuf.items.len;
    res += self.messageBloomTokensBuf.items.len;
    res += self.bloomValuesBuf.items.len;
    res += self.bloomTokensBuf.items.len;
    return @intCast(res);
}

pub const LineBySidSortContext = struct {
    sids: []SID,
    linesBySid: [][]Line,

    pub fn lessThan(ctx: @This(), a: usize, b: usize) bool {
        if (ctx.sids[a].lessThan(ctx.sids[b])) {
            return true;
        }
        if (ctx.sids[b].lessThan(ctx.sids[a])) {
            return false;
        }

        return ctx.linesBySid[a][0].timestampNs < ctx.linesBySid[b][0].timestampNs;
    }

    pub fn swap(ctx: @This(), a: usize, b: usize) void {
        std.mem.swap(SID, &ctx.sids[a], &ctx.sids[b]);
        std.mem.swap([]Line, &ctx.linesBySid[a], &ctx.linesBySid[b]);
    }

    pub fn sort(ctx: @This()) void {
        std.sort.pdqContext(0, ctx.sids.len, ctx);
        ctx.sortLineWindows();
    }

    const LineWindowsSortContext = struct {
        linesBySid: [][]Line,
        lineOffsets: []const usize,
        linesLen: usize,

        fn init(linesBySid: [][]Line, lineOffsetsBuf: []usize) @This() {
            var offset: usize = 0;
            for (linesBySid, 0..) |lines, i| {
                offset += lines.len;
                lineOffsetsBuf[i] = offset;
            }

            return .{
                .linesBySid = linesBySid,
                .lineOffsets = lineOffsetsBuf[0..linesBySid.len],
                .linesLen = offset,
            };
        }

        fn len(ctx: @This()) usize {
            return ctx.linesLen;
        }

        pub fn lessThan(ctx: @This(), a: usize, b: usize) bool {
            return lineLessThan({}, ctx.lineAt(a).*, ctx.lineAt(b).*);
        }

        pub fn swap(ctx: @This(), a: usize, b: usize) void {
            std.mem.swap(Line, ctx.lineAt(a), ctx.lineAt(b));
        }

        fn lineAt(ctx: @This(), idx: usize) *Line {
            var low: usize = 0;
            var high = ctx.lineOffsets.len;
            while (low < high) {
                const mid = low + (high - low) / 2;
                if (idx < ctx.lineOffsets[mid]) {
                    high = mid;
                } else {
                    low = mid + 1;
                }
            }

            const startOffset = if (low == 0) 0 else ctx.lineOffsets[low - 1];
            return &ctx.linesBySid[low][idx - startOffset];
        }
    };

    fn sortLineWindows(ctx: @This()) void {
        var start: usize = 0;
        while (start < ctx.sids.len) {
            var end = start + 1;
            while (end < ctx.sids.len and ctx.sids[start].eql(ctx.sids[end])) {
                end += 1;
            }

            if (end - start > 0) {
                const linesBySid = ctx.linesBySid[start..end];
                var lineOffsetsBuf: [maxCheckpoints]usize = undefined;

                const windowsCtx = LineWindowsSortContext.init(linesBySid, &lineOffsetsBuf);
                std.sort.pdqContext(0, windowsCtx.len(), windowsCtx);
            }

            start = end;
        }
    }
};

pub fn addLines(
    self: *MemTable,
    io: Io,
    allocator: std.mem.Allocator,
    timestampsEncoders: *TimestampsEncoder.TimestampsEncoderPool,
    compressionPool: *CompressionPool,
    sids: []SID,
    linesBySid: [][]Line,
) !void {
    const z = tracy.Zone.begin(.{
        .src = @src(),
        .name = "DataShard.flush",
    });
    defer z.end();

    if (sids.len == 0) {
        return Error.EmptySids;
    }
    std.debug.assert(sids.len == linesBySid.len);
    for (linesBySid) |lines| {
        if (lines.len == 0) {
            return Error.EmptyLines;
        }
    }

    const sortContext = LineBySidSortContext{ .sids = sids, .linesBySid = linesBySid };
    sortContext.sort();

    var blockWriter = try BlockWriter.init(allocator);
    defer blockWriter.deinit(allocator);
    const streamWriter = try TableWriter.initMem(
        allocator,
        self,
        timestampsEncoders,
        compressionPool,
    );
    defer streamWriter.deinit(allocator);

    for (0..sids.len) |k| {
        const lines = linesBySid[k];
        const sid = sids[k];

        var streamI: usize = 0;
        var blockSize: u32 = 0;
        for (lines, 0..) |line, i| {
            std.sort.pdq(Field, line.fields, {}, fieldLessThan);

            // TODO: the tables splits blocks by stream ids,
            // we might want to split them by log level as well,
            // or design another approach to split logs by severity
            if (blockSize >= maxBlockSize or lines[streamI..i].len >= maxLines) {
                // TODO: since lines by sids are 2 continuous slices and may relate to the same sid
                // we should rather write them into the same block
                try blockWriter.writeLines(io, allocator, sid, lines[streamI..i], streamWriter);
                blockSize = 0;
                streamI = i;
            }
            blockSize += line.fieldsSize();
        }
        if (streamI != lines.len) {
            try blockWriter.writeLines(io, allocator, sid, lines[streamI..], streamWriter);
        }
    }

    try blockWriter.finish(io, allocator, streamWriter, &self.tableHeader);
}

// TODO: find out if we can use StreamWriter to flush the table to disk
pub fn storeToDisk(self: *MemTable, io: Io, path: []const u8) !void {
    // TODO: make this function parallel when it comes to writing files
    if (Dir.openDirAbsolute(io, path, .{})) |dir| {
        dir.close(io);
        // TODO: audit all error.xxx and use a full error path
        return error.DirAlreadyExists;
    } else |err| switch (err) {
        error.FileNotFound => {
            try Dir.createDirAbsolute(io, path, .default_dir);
        },
        else => return err,
    }

    var pathBuf: [std.fs.max_path_bytes]u8 = undefined;
    var pathWriter = std.Io.Writer.fixed(&pathBuf);

    try std.fs.path.fmtJoin(&.{ path, filenames.columnKeys }).format(&pathWriter);
    try fs.writeBufferValToFile(io, pathWriter.buffered(), self.columnKeysBuf.items);
    pathWriter.end = 0;

    try std.fs.path.fmtJoin(&.{ path, filenames.columnIdxs }).format(&pathWriter);
    try fs.writeBufferValToFile(io, pathWriter.buffered(), self.columnIdxsBuf.items);
    pathWriter.end = 0;

    try std.fs.path.fmtJoin(&.{ path, filenames.metaindex }).format(&pathWriter);
    try fs.writeBufferValToFile(io, pathWriter.buffered(), self.metaIndexBuf.items);
    pathWriter.end = 0;

    try std.fs.path.fmtJoin(&.{ path, filenames.index }).format(&pathWriter);
    try fs.writeBufferValToFile(io, pathWriter.buffered(), self.indexBuf.items);
    pathWriter.end = 0;

    try std.fs.path.fmtJoin(&.{ path, filenames.columnsHeaderIndex }).format(&pathWriter);
    try fs.writeBufferValToFile(io, pathWriter.buffered(), self.columnsHeaderIndexBuf.items);
    pathWriter.end = 0;

    try std.fs.path.fmtJoin(&.{ path, filenames.columnsHeader }).format(&pathWriter);
    try fs.writeBufferValToFile(io, pathWriter.buffered(), self.columnsHeaderBuf.items);
    pathWriter.end = 0;

    try std.fs.path.fmtJoin(&.{ path, filenames.timestamps }).format(&pathWriter);
    try fs.writeBufferValToFile(io, pathWriter.buffered(), self.timestampsBuf.items);
    pathWriter.end = 0;

    try std.fs.path.fmtJoin(&.{ path, filenames.messageTokens }).format(&pathWriter);
    try fs.writeBufferValToFile(
        io,
        pathWriter.buffered(),
        self.messageBloomTokensBuf.items,
    );
    pathWriter.end = 0;

    try std.fs.path.fmtJoin(&.{ path, filenames.messageValues }).format(&pathWriter);
    try fs.writeBufferValToFile(
        io,
        pathWriter.buffered(),
        self.messageBloomValuesBuf.items,
    );

    const bloomTokensPath = try filenames.writeBloomFilePath(&pathBuf, path, filenames.bloomTokens, 0);
    const bloomTokensContent = self.bloomTokensBuf.items;
    try fs.writeBufferValToFile(io, bloomTokensPath, bloomTokensContent);

    const bloomValuesPath = try filenames.writeBloomFilePath(&pathBuf, path, filenames.bloomValues, 0);
    const bloomValuesContent = self.bloomValuesBuf.items;
    try fs.writeBufferValToFile(io, bloomValuesPath, bloomValuesContent);

    try self.tableHeader.writeFile(io, path);

    try fs.syncPathAndParentDir(io, path);
}

pub fn addLinesForSid(
    self: *MemTable,
    io: Io,
    allocator: std.mem.Allocator,
    timestampsEncoders: *TimestampsEncoder.TimestampsEncoderPool,
    compressionPool: *CompressionPool,
    sid: SID,
    lines: []Line,
) !void {
    var sids = [_]SID{sid};
    var linesBySid = [_][]Line{lines};
    return self.addLines(io, allocator, timestampsEncoders, compressionPool, sids[0..], linesBySid[0..]);
}
