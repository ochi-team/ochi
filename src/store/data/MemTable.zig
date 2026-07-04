const std = @import("std");
const Io = std.Io;
const Dir = Io.Dir;
const fs = @import("../../fs.zig");

const Field = @import("../lines.zig").Field;
const Line = @import("../lines.zig").Line;
const lineLessThan = @import("../lines.zig").lineLessThan;
const fieldLessThan = @import("../lines.zig").fieldLessThan;
const SID = @import("../lines.zig").SID;

const TableWriter = @import("TableWriter.zig");
const maxCheckpoints = @import("../../DataRecorder.zig").DataShard.maxCheckpoints;
const BlockWriter = @import("BlockWriter.zig");
const TableHeader = @import("TableHeader.zig");
const filenames = @import("../../filenames.zig");

const Consts = @import("../../Consts.zig");

const maxBlockSize = Consts.maxBlockSize;

// 2mb block size, on merging it takes double amount up to 4mb
// TODO: benchmark whether 2.5-3kb performs better

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

const LineBySidSortContext = struct {
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
    sids: []SID,
    linesBySid: [][]Line,
) !void {
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
    const streamWriter = try TableWriter.initMem(allocator, self);
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
            if (blockSize >= maxBlockSize) {
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
pub fn storeToDisk(self: *MemTable, io: Io, alloc: std.mem.Allocator, path: []const u8) !void {
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

    // for mem table it's expect to have a single bloom filter shard
    var stack = std.heap.stackFallback(2048, alloc);
    const allocator = stack.get();

    const columnKeysPath =
        try std.fs.path.join(allocator, &.{ path, filenames.columnKeys });
    defer allocator.free(columnKeysPath);

    const columnIdxsPath =
        try std.fs.path.join(allocator, &.{ path, filenames.columnIdxs });
    defer allocator.free(columnIdxsPath);

    const metaindexPath =
        try std.fs.path.join(allocator, &.{ path, filenames.metaindex });
    defer allocator.free(metaindexPath);

    const indexPath =
        try std.fs.path.join(allocator, &.{ path, filenames.index });
    defer allocator.free(indexPath);

    const columnsHeaderIndexPath =
        try std.fs.path.join(allocator, &.{ path, filenames.columnsHeaderIndex });
    defer allocator.free(columnsHeaderIndexPath);

    const columnsHeaderPath =
        try std.fs.path.join(allocator, &.{ path, filenames.columnsHeader });
    defer allocator.free(columnsHeaderPath);

    const timestampsPath =
        try std.fs.path.join(allocator, &.{ path, filenames.timestamps });
    defer allocator.free(timestampsPath);

    const messageValuesPath =
        try std.fs.path.join(allocator, &.{ path, filenames.messageValues });
    defer allocator.free(messageValuesPath);

    const messageBloomFilterPath =
        try std.fs.path.join(allocator, &.{ path, filenames.messageTokens });
    defer allocator.free(messageBloomFilterPath);

    try fs.writeBufferValToFile(io, columnKeysPath, self.columnKeysBuf.items);
    try fs.writeBufferValToFile(io, columnIdxsPath, self.columnIdxsBuf.items);
    try fs.writeBufferValToFile(io, metaindexPath, self.metaIndexBuf.items);
    try fs.writeBufferValToFile(io, indexPath, self.indexBuf.items);
    try fs.writeBufferValToFile(io, columnsHeaderIndexPath, self.columnsHeaderIndexBuf.items);
    try fs.writeBufferValToFile(io, columnsHeaderPath, self.columnsHeaderBuf.items);
    try fs.writeBufferValToFile(io, timestampsPath, self.timestampsBuf.items);

    try fs.writeBufferValToFile(
        io,
        messageBloomFilterPath,
        self.messageBloomTokensBuf.items,
    );
    try fs.writeBufferValToFile(
        io,
        messageValuesPath,
        self.messageBloomValuesBuf.items,
    );

    var pathBuf: [std.fs.max_path_bytes]u8 = undefined;

    const bloomTokensPath = try filenames.writeBloomFilePath(&pathBuf, path, filenames.bloomTokens, 0);
    const bloomTokensContent = self.bloomTokensBuf.items;
    try fs.writeBufferValToFile(io, bloomTokensPath, bloomTokensContent);

    const bloomValuesPath = try filenames.writeBloomFilePath(&pathBuf, path, filenames.bloomValues, 0);
    const bloomValuesContent = self.bloomValuesBuf.items;
    try fs.writeBufferValToFile(io, bloomValuesPath, bloomValuesContent);

    try self.tableHeader.writeFile(io, allocator, path);

    try fs.syncPathAndParentDir(io, path);
}

const BlockHeader = @import("BlockHeader.zig");
const IndexBlockHeader = @import("IndexBlockHeader.zig");
const TimestampsEncoder = @import("TimestampsEncoder.zig");
const EncodingType = @import("TimestampsEncoder.zig").EncodingType;
const encoding = @import("encoding");
const Table = @import("Table.zig");
const BlockReader = @import("BlockReader.zig").BlockReader;
const sampleSid = SID{ .tenantID = 1234, .id = 1 };

const SampleLines = struct {
    fields1: [2]Field,
    fields2: [2]Field,
    lines: [2]Line,
};

pub fn addLinesForSid(
    self: *MemTable,
    io: Io,
    allocator: std.mem.Allocator,
    sid: SID,
    lines: []Line,
) !void {
    var sids = [_]SID{sid};
    var linesBySid = [_][]Line{lines};
    return self.addLines(io, allocator, sids[0..], linesBySid[0..]);
}

fn populateSampleLines(sample: *SampleLines) void {
    sample.fields1 = .{
        .{ .key = "level", .value = "info" },
        .{ .key = "app", .value = "seq" },
    };
    sample.fields2 = .{
        .{ .key = "level", .value = "warn" },
        .{ .key = "app", .value = "seq" },
    };
    sample.lines = .{
        .{
            .timestampNs = 2,
            .fields = sample.fields2[0..],
        },
        .{
            .timestampNs = 1,
            .fields = sample.fields1[0..],
        },
    };
}

fn readFileAll(io: Io, allocator: std.mem.Allocator, path: []const u8) ![]u8 {
    var file = try std.Io.Dir.cwd().openFile(io, path, .{});
    defer file.close(io);

    var file_reader = file.reader(io, &.{});
    return file_reader.interface.allocRemaining(allocator, .unlimited);
}

// TODO: test everything using checkAllAllocationFailures
// TODO: test everything using checkAllIoFailures or something
// TODO: make sure we test fallback allocators either as failabable with capacity 1
// and capacity 16k+, audit all of them and perhaps get rid of some
test "addLines" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, testAddLines, .{std.testing.io});
}

fn testAddLines(allocator: std.mem.Allocator, io: Io) !void {
    var sample: SampleLines = SampleLines{
        .fields1 = undefined,
        .fields2 = undefined,
        .lines = undefined,
    };
    populateSampleLines(&sample);

    // unordered timestamps in lines so that it tests its sorting
    var lines = [2]Line{
        sample.lines[0],
        sample.lines[1],
    };

    const memTable = try MemTable.init(allocator);
    defer memTable.deinit(allocator);
    try memTable.addLinesForSid(io, allocator, sampleSid, lines[0..]);

    const timestampsContent = memTable.timestampsBuf.items;
    const indexContent = memTable.indexBuf.items;

    // Validate timestamps
    {
        var dst: [2]u64 = undefined;
        const timestampsEncoder = try TimestampsEncoder.init(allocator);
        defer timestampsEncoder.deinit(allocator);
        try timestampsEncoder.decode(dst[0..], timestampsContent);

        try std.testing.expectEqualSlices(u64, &[_]u64{ 1, 2 }, &dst);
    }

    // Validate block header
    {
        const decompressedSize = try encoding.getFrameContentSize(indexContent);
        const decompressedBuf = try allocator.alloc(u8, decompressedSize);
        defer allocator.free(decompressedBuf);
        _ = try encoding.decompress(decompressedBuf, indexContent);

        const decodedBlockHeader = BlockHeader.decode(decompressedBuf);
        const blockHeader = decodedBlockHeader.header;

        // TODO: compare all the fields in one expect
        try std.testing.expectEqual(1234, blockHeader.sid.tenantID);
        try std.testing.expectEqual(1, blockHeader.sid.id);
        try std.testing.expectEqual(140, blockHeader.size);
        try std.testing.expectEqual(2, blockHeader.len);

        try std.testing.expectEqual(0, blockHeader.timestampsHeader.offset);
        try std.testing.expectEqual(17, blockHeader.timestampsHeader.size);
        try std.testing.expectEqual(1, blockHeader.timestampsHeader.min);
        try std.testing.expectEqual(2, blockHeader.timestampsHeader.max);
        try std.testing.expectEqual(EncodingType.ZDeltapack, blockHeader.timestampsHeader.encodingType);
    }

    // validate meta index
    {
        const metaIndexContent = memTable.metaIndexBuf.items;
        try std.testing.expect(metaIndexContent.len > 0);

        const decompressedSize = try encoding.getFrameContentSize(metaIndexContent);
        const decompressedBuf = try allocator.alloc(u8, decompressedSize);
        defer allocator.free(decompressedBuf);
        _ = try encoding.decompress(decompressedBuf, metaIndexContent);

        const decodedIndexBlockHeader = IndexBlockHeader.decode(decompressedBuf);

        // TODO: compare all the fields in one expect
        try std.testing.expectEqual(1234, decodedIndexBlockHeader.sid.tenantID);
        try std.testing.expectEqual(1, decodedIndexBlockHeader.sid.id);
        try std.testing.expectEqual(1, decodedIndexBlockHeader.minTs);
        try std.testing.expectEqual(2, decodedIndexBlockHeader.maxTs);
        try std.testing.expectEqual(0, decodedIndexBlockHeader.offset);
        try std.testing.expectEqual(@as(u64, @intCast(indexContent.len)), decodedIndexBlockHeader.size);
    }

    // validate bloom filters
    {
        const messageBloomTokensContent = memTable.messageBloomTokensBuf.items;
        const messageBloomValuesContent = memTable.messageBloomValuesBuf.items;

        try std.testing.expectEqual(0, messageBloomTokensContent.len);
        try std.testing.expectEqual(0, messageBloomValuesContent.len);
        try std.testing.expectEqual(0, memTable.bloomTokensBuf.items.len);
        try std.testing.expect(memTable.bloomValuesBuf.items.len > 0);
    }
}

test "addLinesErrorOnEmpty" {
    var lines = [_]Line{};
    const memTable = try MemTable.init(std.testing.allocator);
    defer memTable.deinit(std.testing.allocator);
    const err = memTable.addLinesForSid(std.testing.io, std.testing.allocator, sampleSid, lines[0..]);
    try std.testing.expectError(Error.EmptyLines, err);
}

test "addLines error on empty lines chunk" {
    const alloc = std.testing.allocator;
    const sid = SID{ .tenantID = 1, .id = 1 };
    var fields = [_]Field{.{ .key = "msg", .value = "one" }};
    var lines = [_]Line{.{
        .timestampNs = 1,
        .fields = fields[0..],
    }};
    var emptyLines = [_]Line{};
    var sids = [_]SID{ sid, sid };
    var linesBySid = [_][]Line{ lines[0..], emptyLines[0..] };

    const memTable = try MemTable.init(alloc);
    defer memTable.deinit(alloc);

    const err = memTable.addLines(
        std.testing.io,
        alloc,
        sids[0..],
        linesBySid[0..],
    );
    try std.testing.expectError(Error.EmptyLines, err);
}

test "addLines reorders duplicate SID chunk lines by timestamp" {
    const ExpectedBlock = struct {
        min: u64,
        max: u64,
    };
    const Case = struct {
        chunks: [2][2]u64,
        expectedBlocks: [2]ExpectedBlock,
    };

    const cases = [_]Case{
        .{
            .chunks = .{ .{ 3, 1 }, .{ 2, 4 } },
            .expectedBlocks = .{ .{ .min = 1, .max = 2 }, .{ .min = 3, .max = 4 } },
        },
        .{
            .chunks = .{ .{ 4, 3 }, .{ 2, 1 } },
            .expectedBlocks = .{ .{ .min = 1, .max = 2 }, .{ .min = 3, .max = 4 } },
        },
    };

    const alloc = std.testing.allocator;
    const io = std.testing.io;
    const sid = SID{ .tenantID = 1, .id = 1 };

    for (cases) |case| {
        var fields = [_][1]Field{
            .{.{ .key = "msg", .value = "one" }},
            .{.{ .key = "msg", .value = "two" }},
            .{.{ .key = "msg", .value = "three" }},
            .{.{ .key = "msg", .value = "four" }},
        };
        var firstChunk = [_]Line{
            .{ .timestampNs = case.chunks[0][0], .fields = fields[0][0..] },
            .{ .timestampNs = case.chunks[0][1], .fields = fields[1][0..] },
        };
        var secondChunk = [_]Line{
            .{ .timestampNs = case.chunks[1][0], .fields = fields[2][0..] },
            .{ .timestampNs = case.chunks[1][1], .fields = fields[3][0..] },
        };
        var sids = [_]SID{ sid, sid };
        var linesBySid = [_][]Line{ firstChunk[0..], secondChunk[0..] };

        const memTable = try MemTable.init(alloc);
        var memTableOwned = true;
        defer if (memTableOwned) memTable.deinit(alloc);

        try memTable.addLines(io, alloc, sids[0..], linesBySid[0..]);

        const table = try Table.fromMem(alloc, memTable);
        memTableOwned = false;
        defer table.close(io);

        const blockReader = try BlockReader.initFromMemTable(alloc, table);
        defer blockReader.deinit(alloc);

        var actualBlocks: [2]ExpectedBlock = undefined;
        var actualBlocksLen: usize = 0;
        while (try blockReader.nextBlock(io, alloc)) {
            try std.testing.expect(actualBlocksLen < actualBlocks.len);
            try std.testing.expect(blockReader.blockData.sid.eql(sid));
            actualBlocks[actualBlocksLen] = .{
                .min = blockReader.blockData.timestampsData.minTimestamp,
                .max = blockReader.blockData.timestampsData.maxTimestamp,
            };
            actualBlocksLen += 1;
        }

        try std.testing.expectEqual(case.expectedBlocks.len, actualBlocksLen);
        try std.testing.expectEqualDeep(case.expectedBlocks[0..], actualBlocks[0..actualBlocksLen]);
    }
}

const ExpectedSortedLinesChunk = struct {
    sid: SID,
    timestamps: []const u64,
};

fn testLine(timestampNs: u64) Line {
    return .{
        .timestampNs = timestampNs,
        .fields = undefined,
    };
}

fn expectLineBySidSortResult(
    sids: []const SID,
    linesBySid: [][]Line,
    expected: []const ExpectedSortedLinesChunk,
) !void {
    try std.testing.expectEqual(expected.len, sids.len);
    try std.testing.expectEqual(expected.len, linesBySid.len);

    for (expected, 0..) |chunk, i| {
        try std.testing.expectEqualDeep(chunk.sid, sids[i]);
        try std.testing.expectEqual(chunk.timestamps.len, linesBySid[i].len);
        for (chunk.timestamps, 0..) |timestamp, lineI| {
            try std.testing.expectEqual(timestamp, linesBySid[i][lineI].timestampNs);
        }
    }
}

test "LineBySidSortContext.sort handles no chunks" {
    var sids = [_]SID{};
    var linesBySid = [_][]Line{};

    const sortContext = LineBySidSortContext{
        .sids = sids[0..],
        .linesBySid = linesBySid[0..],
    };
    sortContext.sort();

    try expectLineBySidSortResult(sids[0..], linesBySid[0..], &.{});
}

test "LineBySidSortContext.sort handles single sid with many unordered lines" {
    const sid = SID{ .tenantID = 1, .id = 1 };
    var lines = [_]Line{
        testLine(std.math.maxInt(u64)),
        testLine(3),
        testLine(0),
        testLine(3),
        testLine(1),
    };
    var sids = [_]SID{sid};
    var linesBySid = [_][]Line{lines[0..]};

    const sortContext = LineBySidSortContext{
        .sids = sids[0..],
        .linesBySid = linesBySid[0..],
    };
    sortContext.sort();

    try expectLineBySidSortResult(sids[0..], linesBySid[0..], &.{
        .{ .sid = sid, .timestamps = &.{ 0, 1, 3, 3, std.math.maxInt(u64) } },
    });
}

test "LineBySidSortContext.sort handles max checkpoints with single line chunks" {
    var lines = [_]Line{
        testLine(16),
        testLine(15),
        testLine(14),
        testLine(13),
        testLine(12),
        testLine(11),
        testLine(10),
        testLine(9),
        testLine(8),
        testLine(7),
        testLine(6),
        testLine(5),
        testLine(4),
        testLine(3),
        testLine(2),
        testLine(1),
    };
    var sids = [_]SID{
        .{ .tenantID = 8, .id = 2 },
        .{ .tenantID = 3, .id = 2 },
        .{ .tenantID = 4, .id = 2 },
        .{ .tenantID = 1, .id = 3 },
        .{ .tenantID = 7, .id = 2 },
        .{ .tenantID = 1, .id = 2 },
        .{ .tenantID = 2, .id = 2 },
        .{ .tenantID = 6, .id = 2 },
        .{ .tenantID = 5, .id = 2 },
        .{ .tenantID = 1, .id = 1 },
        .{ .tenantID = 9, .id = 2 },
        .{ .tenantID = 2, .id = 1 },
        .{ .tenantID = 4, .id = 1 },
        .{ .tenantID = 3, .id = 1 },
        .{ .tenantID = 5, .id = 1 },
        .{ .tenantID = 6, .id = 1 },
    };
    var linesBySid = [_][]Line{
        lines[0..1],
        lines[1..2],
        lines[2..3],
        lines[3..4],
        lines[4..5],
        lines[5..6],
        lines[6..7],
        lines[7..8],
        lines[8..9],
        lines[9..10],
        lines[10..11],
        lines[11..12],
        lines[12..13],
        lines[13..14],
        lines[14..15],
        lines[15..16],
    };

    const sortContext = LineBySidSortContext{
        .sids = sids[0..],
        .linesBySid = linesBySid[0..],
    };
    sortContext.sort();

    try expectLineBySidSortResult(sids[0..], linesBySid[0..], &.{
        .{ .sid = .{ .tenantID = 1, .id = 1 }, .timestamps = &.{7} },
        .{ .sid = .{ .tenantID = 1, .id = 2 }, .timestamps = &.{11} },
        .{ .sid = .{ .tenantID = 1, .id = 3 }, .timestamps = &.{13} },
        .{ .sid = .{ .tenantID = 2, .id = 1 }, .timestamps = &.{5} },
        .{ .sid = .{ .tenantID = 2, .id = 2 }, .timestamps = &.{10} },
        .{ .sid = .{ .tenantID = 3, .id = 1 }, .timestamps = &.{3} },
        .{ .sid = .{ .tenantID = 3, .id = 2 }, .timestamps = &.{15} },
        .{ .sid = .{ .tenantID = 4, .id = 1 }, .timestamps = &.{4} },
        .{ .sid = .{ .tenantID = 4, .id = 2 }, .timestamps = &.{14} },
        .{ .sid = .{ .tenantID = 5, .id = 1 }, .timestamps = &.{2} },
        .{ .sid = .{ .tenantID = 5, .id = 2 }, .timestamps = &.{8} },
        .{ .sid = .{ .tenantID = 6, .id = 1 }, .timestamps = &.{1} },
        .{ .sid = .{ .tenantID = 6, .id = 2 }, .timestamps = &.{9} },
        .{ .sid = .{ .tenantID = 7, .id = 2 }, .timestamps = &.{12} },
        .{ .sid = .{ .tenantID = 8, .id = 2 }, .timestamps = &.{16} },
        .{ .sid = .{ .tenantID = 9, .id = 2 }, .timestamps = &.{6} },
    });
}

test "LineBySidSortContext.sort handles many single line chunks for one sid" {
    const sid = SID{ .tenantID = 5, .id = 9 };
    var lines = [_]Line{
        testLine(100),
        testLine(0),
        testLine(50),
        testLine(std.math.maxInt(u64)),
        testLine(1),
        testLine(50),
    };
    var sids = [_]SID{ sid, sid, sid, sid, sid, sid };
    var linesBySid = [_][]Line{
        lines[0..1],
        lines[1..2],
        lines[2..3],
        lines[3..4],
        lines[4..5],
        lines[5..6],
    };

    const sortContext = LineBySidSortContext{
        .sids = sids[0..],
        .linesBySid = linesBySid[0..],
    };
    sortContext.sort();

    try expectLineBySidSortResult(sids[0..], linesBySid[0..], &.{
        .{ .sid = sid, .timestamps = &.{0} },
        .{ .sid = sid, .timestamps = &.{1} },
        .{ .sid = sid, .timestamps = &.{50} },
        .{ .sid = sid, .timestamps = &.{50} },
        .{ .sid = sid, .timestamps = &.{100} },
        .{ .sid = sid, .timestamps = &.{std.math.maxInt(u64)} },
    });
}

test "LineBySidSortContext.sort handles many items per chunk across duplicate sids" {
    const sid = SID{ .tenantID = 2, .id = 7 };
    var linesA = [_]Line{
        testLine(9),
        testLine(1),
        testLine(7),
    };
    var linesB = [_]Line{
        testLine(6),
        testLine(5),
        testLine(4),
        testLine(3),
    };
    var linesC = [_]Line{
        testLine(8),
        testLine(2),
    };
    var sids = [_]SID{ sid, sid, sid };
    var linesBySid = [_][]Line{
        linesA[0..],
        linesB[0..],
        linesC[0..],
    };

    const sortContext = LineBySidSortContext{
        .sids = sids[0..],
        .linesBySid = linesBySid[0..],
    };
    sortContext.sort();

    try expectLineBySidSortResult(sids[0..], linesBySid[0..], &.{
        .{ .sid = sid, .timestamps = &.{ 1, 2, 3, 4 } },
        .{ .sid = sid, .timestamps = &.{ 5, 6 } },
        .{ .sid = sid, .timestamps = &.{ 7, 8, 9 } },
    });
}

test "LineBySidSortContext.sort handles one unordered chunk per sid" {
    const sidA = SID{ .tenantID = 1, .id = 1 };
    const sidB = SID{ .tenantID = 1, .id = 2 };
    const sidC = SID{ .tenantID = 2, .id = 1 };
    var linesA = [_]Line{
        testLine(5),
        testLine(4),
        testLine(6),
    };
    var linesB = [_]Line{
        testLine(3),
        testLine(1),
        testLine(2),
    };
    var linesC = [_]Line{
        testLine(9),
        testLine(7),
        testLine(8),
    };
    var sids = [_]SID{ sidC, sidA, sidB };
    var linesBySid = [_][]Line{
        linesC[0..],
        linesA[0..],
        linesB[0..],
    };

    const sortContext = LineBySidSortContext{
        .sids = sids[0..],
        .linesBySid = linesBySid[0..],
    };
    sortContext.sort();

    try expectLineBySidSortResult(sids[0..], linesBySid[0..], &.{
        .{ .sid = sidA, .timestamps = &.{ 4, 5, 6 } },
        .{ .sid = sidB, .timestamps = &.{ 1, 2, 3 } },
        .{ .sid = sidC, .timestamps = &.{ 7, 8, 9 } },
    });
}

test "LineBySidSortContext.sort handles sid and timestamp extremes" {
    const minSid = SID{ .tenantID = 0, .id = 0 };
    const maxIdSid = SID{ .tenantID = 0, .id = std.math.maxInt(u128) };
    const maxTenantSid = SID{ .tenantID = std.math.maxInt(u64), .id = 0 };
    var minSidFirst = [_]Line{
        testLine(std.math.maxInt(u64)),
        testLine(0),
    };
    var minSidSecond = [_]Line{
        testLine(1),
    };
    var maxIdLines = [_]Line{
        testLine(2),
        testLine(0),
    };
    var maxTenantLines = [_]Line{
        testLine(std.math.maxInt(u64)),
        testLine(std.math.maxInt(u64) - 1),
    };
    var sids = [_]SID{ maxTenantSid, minSid, maxIdSid, minSid };
    var linesBySid = [_][]Line{
        maxTenantLines[0..],
        minSidFirst[0..],
        maxIdLines[0..],
        minSidSecond[0..],
    };

    const sortContext = LineBySidSortContext{
        .sids = sids[0..],
        .linesBySid = linesBySid[0..],
    };
    sortContext.sort();

    try expectLineBySidSortResult(sids[0..], linesBySid[0..], &.{
        .{ .sid = minSid, .timestamps = &.{0} },
        .{ .sid = minSid, .timestamps = &.{ 1, std.math.maxInt(u64) } },
        .{ .sid = maxIdSid, .timestamps = &.{ 0, 2 } },
        .{ .sid = maxTenantSid, .timestamps = &.{ std.math.maxInt(u64) - 1, std.math.maxInt(u64) } },
    });
}

test "flushToDisk writes buffers" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, testFlushToDisk, .{std.testing.io});
}

test "tableHeader timestamp range matches all index blocks" {
    const alloc = std.testing.allocator;
    const io = std.testing.io;

    const memTable = try MemTable.init(alloc);
    defer memTable.deinit(alloc);

    const lineCount = 2200;
    var lines = try alloc.alloc(Line, lineCount);
    defer alloc.free(lines);
    var sids = try alloc.alloc(SID, lineCount);
    defer alloc.free(sids);
    var linesBySid = try alloc.alloc([]Line, lineCount);
    defer alloc.free(linesBySid);

    var fields = [_]Field{.{ .key = "k", .value = "v" }};
    for (0..lineCount) |i| {
        lines[i] = .{
            .timestampNs = @intCast(i + 1),
            .fields = fields[0..],
        };
        sids[i] = .{ .tenantID = 1, .id = @intCast(i + 1) };
        linesBySid[i] = lines[i .. i + 1];
    }

    try memTable.addLines(io, alloc, sids, linesBySid);

    const indexBlockHeaders = try IndexBlockHeader.readIndexBlockHeaders(
        alloc,
        memTable.metaIndexBuf.items,
    );
    defer alloc.free(indexBlockHeaders);

    try std.testing.expect(indexBlockHeaders.len > 1);

    var minTs = indexBlockHeaders[0].minTs;
    var maxTs = indexBlockHeaders[0].maxTs;
    for (indexBlockHeaders[1..]) |header| {
        minTs = @min(minTs, header.minTs);
        maxTs = @max(maxTs, header.maxTs);
    }

    try std.testing.expectEqual(minTs, memTable.tableHeader.minTimestamp);
    try std.testing.expectEqual(maxTs, memTable.tableHeader.maxTimestamp);
}

fn testFlushToDisk(allocator: std.mem.Allocator, io: Io) !void {
    var sample: SampleLines = SampleLines{
        .fields1 = undefined,
        .fields2 = undefined,
        .lines = undefined,
    };
    populateSampleLines(&sample);
    var lines = [2]Line{
        sample.lines[0],
        sample.lines[1],
    };

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const basePath = try tmp.dir.realPathFileAlloc(io, ".", allocator);
    defer allocator.free(basePath);
    const flushPath = try std.fs.path.join(allocator, &.{ basePath, "flush" });
    defer allocator.free(flushPath);

    var pathBuf: [std.fs.max_path_bytes]u8 = undefined;

    const memTable = try MemTable.init(allocator);
    defer memTable.deinit(allocator);

    try memTable.addLinesForSid(io, allocator, sampleSid, lines[0..]);
    try memTable.storeToDisk(io, allocator, flushPath);

    const columnKeysPath = try std.fs.path.join(allocator, &.{ flushPath, filenames.columnKeys });
    defer allocator.free(columnKeysPath);
    const columnIdxsPath = try std.fs.path.join(allocator, &.{ flushPath, filenames.columnIdxs });
    defer allocator.free(columnIdxsPath);
    const metaindexPath = try std.fs.path.join(allocator, &.{ flushPath, filenames.metaindex });
    defer allocator.free(metaindexPath);
    const indexPath = try std.fs.path.join(allocator, &.{ flushPath, filenames.index });
    defer allocator.free(indexPath);
    const columnsHeaderIndexPath = try std.fs.path.join(allocator, &.{ flushPath, filenames.columnsHeaderIndex });
    defer allocator.free(columnsHeaderIndexPath);
    const columnsHeaderPath = try std.fs.path.join(allocator, &.{ flushPath, filenames.columnsHeader });
    defer allocator.free(columnsHeaderPath);
    const timestampsPath = try std.fs.path.join(allocator, &.{ flushPath, filenames.timestamps });
    defer allocator.free(timestampsPath);
    const messageBloomTokensPath = try std.fs.path.join(allocator, &.{ flushPath, filenames.messageTokens });
    defer allocator.free(messageBloomTokensPath);
    const messageBloomValuesPath = try std.fs.path.join(allocator, &.{ flushPath, filenames.messageValues });
    defer allocator.free(messageBloomValuesPath);
    const metadataPath = try std.fs.path.join(allocator, &.{ flushPath, filenames.header });
    defer allocator.free(metadataPath);

    const columnKeysContent = try readFileAll(io, allocator, columnKeysPath);
    defer allocator.free(columnKeysContent);
    try std.testing.expectEqualSlices(u8, memTable.columnKeysBuf.items, columnKeysContent);

    const columnIdxsContent = try readFileAll(io, allocator, columnIdxsPath);
    defer allocator.free(columnIdxsContent);
    try std.testing.expectEqualSlices(u8, memTable.columnIdxsBuf.items, columnIdxsContent);

    const metaindexContent = try readFileAll(io, allocator, metaindexPath);
    defer allocator.free(metaindexContent);
    try std.testing.expectEqualSlices(u8, memTable.metaIndexBuf.items, metaindexContent);

    const indexContent = try readFileAll(io, allocator, indexPath);
    defer allocator.free(indexContent);
    try std.testing.expectEqualSlices(u8, memTable.indexBuf.items, indexContent);

    const columnsHeaderIndexContent = try readFileAll(io, allocator, columnsHeaderIndexPath);
    defer allocator.free(columnsHeaderIndexContent);
    try std.testing.expectEqualSlices(u8, memTable.columnsHeaderIndexBuf.items, columnsHeaderIndexContent);

    const columnsHeaderContent = try readFileAll(io, allocator, columnsHeaderPath);
    defer allocator.free(columnsHeaderContent);
    try std.testing.expectEqualSlices(u8, memTable.columnsHeaderBuf.items, columnsHeaderContent);

    const timestampsContent = try readFileAll(io, allocator, timestampsPath);
    defer allocator.free(timestampsContent);
    try std.testing.expectEqualSlices(u8, memTable.timestampsBuf.items, timestampsContent);

    const msgBloomTokensContent = try readFileAll(io, allocator, messageBloomTokensPath);
    defer allocator.free(msgBloomTokensContent);
    try std.testing.expectEqualSlices(u8, memTable.messageBloomTokensBuf.items, msgBloomTokensContent);

    const msgBloomValuesContent = try readFileAll(io, allocator, messageBloomValuesPath);
    defer allocator.free(msgBloomValuesContent);
    try std.testing.expectEqualSlices(u8, memTable.messageBloomValuesBuf.items, msgBloomValuesContent);

    const bloomTokensPath = try filenames.writeBloomFilePath(&pathBuf, flushPath, filenames.bloomTokens, 0);
    const bloomTokensContent = try readFileAll(io, allocator, bloomTokensPath);
    defer allocator.free(bloomTokensContent);
    try std.testing.expectEqualSlices(u8, memTable.bloomTokensBuf.items, bloomTokensContent);

    const bloomValuesPath = try filenames.writeBloomFilePath(&pathBuf, flushPath, filenames.bloomValues, 0);
    const bloomValuesContent = try readFileAll(io, allocator, bloomValuesPath);
    defer allocator.free(bloomValuesContent);
    try std.testing.expectEqualSlices(u8, memTable.bloomValuesBuf.items, bloomValuesContent);

    const metadataContent = try readFileAll(io, allocator, metadataPath);
    defer allocator.free(metadataContent);
    try std.testing.expect(metadataContent.len > 0);

    try std.testing.expectError(error.DirAlreadyExists, memTable.storeToDisk(io, allocator, flushPath));
}
