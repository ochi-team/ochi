const std = @import("std");
const Io = std.Io;
const Dir = Io.Dir;
const fs = @import("../../fs.zig");

const Field = @import("../lines.zig").Field;
const Line = @import("../lines.zig").Line;
const lineLessThan = @import("../lines.zig").lineLessThan;
const fieldLessThan = @import("../lines.zig").fieldLessThan;
const SID = @import("../lines.zig").SID;

const StreamWriter = @import("StreamWriter.zig");
const BlockWriter = @import("BlockWriter.zig");
const TableHeader = @import("TableHeader.zig");
const filenames = @import("../../filenames.zig");

// 2mb block size, on merging it takes double amount up to 4mb
// TODO: benchmark whether 2.5-3kb performs better
// TODO: move to a better place, it's used in the merger (disk table)
pub const maxBlockSize = 2 * 1024 * 1024;

pub const Error = error{
    EmptyLines,
};

const MemTable = @This();

// TODO: decouple this relation, writer must not be here
streamWriter: *StreamWriter,
tableHeader: TableHeader,

flushAtUs: i64 = std.math.maxInt(i64),

pub fn init(io: Io, allocator: std.mem.Allocator) !*MemTable {
    const streamWriter = try StreamWriter.initMem(io, allocator, 1);
    errdefer streamWriter.deinit(io, allocator);

    const p = try allocator.create(MemTable);
    //TODO should it be stack allocated instead?
    p.* = MemTable{
        .streamWriter = streamWriter,
        .tableHeader = .{},
    };

    return p;
}
pub fn deinit(self: *MemTable, io: Io, allocator: std.mem.Allocator) void {
    self.streamWriter.deinit(io, allocator);
    allocator.destroy(self);
}

pub fn addLines(self: *MemTable, io: Io, allocator: std.mem.Allocator, lines: []Line) !void {
    if (lines.len == 0) {
        return Error.EmptyLines;
    }

    var blockWriter = try BlockWriter.init(allocator);
    defer blockWriter.deinit(allocator);

    var streamI: usize = 0;
    var blockSize: u32 = 0;

    std.mem.sortUnstable(Line, lines, {}, lineLessThan);
    var prevSID: SID = lines[0].sid;

    for (lines, 0..) |line, i| {
        std.mem.sortUnstable(Field, line.fields, {}, fieldLessThan);

        if (blockSize >= maxBlockSize or !line.sid.eql(&prevSID)) {
            try blockWriter.writeLines(io, allocator, prevSID, lines[streamI..i], self.streamWriter);
            prevSID = line.sid;
            blockSize = 0;
            streamI = i;
        }
        blockSize += line.fieldsSize();
    }
    if (streamI != lines.len) {
        try blockWriter.writeLines(io, allocator, prevSID, lines[streamI..], self.streamWriter);
    }
    try blockWriter.finish(io, allocator, self.streamWriter, &self.tableHeader);
}

// TODO: this is not the best place for bloom path generation
pub fn getBloomValuesFilePath(alloc: std.mem.Allocator, partPath: []const u8, shardIdx: u64) ![]u8 {
    const shardIdxLen = std.fmt.count("{}", .{shardIdx});
    const path = try alloc.alloc(u8, partPath.len + 1 + filenames.bloomValues.len + shardIdxLen);
    _ = try std.fmt.bufPrint(path, "{s}/{s}{}", .{ partPath, filenames.bloomValues, shardIdx });
    return path;
}

// TODO: this is not the best place for bloom path generation
pub fn getBloomTokensFilePath(alloc: std.mem.Allocator, tablePath: []const u8, shardIdx: u64) ![]u8 {
    const shardIdxLen = std.fmt.count("{}", .{shardIdx});
    const path = try alloc.alloc(u8, tablePath.len + 1 + filenames.bloomTokens.len + shardIdxLen);
    _ = try std.fmt.bufPrint(path, "{s}/{s}{}", .{ tablePath, filenames.bloomTokens, shardIdx });
    return path;
}

pub fn storeToDisk(self: *MemTable, io: Io, alloc: std.mem.Allocator, path: []const u8) !void {
    // TODO: make this function parallel when it comes to writing files
    if (Dir.openDirAbsolute(io, path, .{})) |dir| {
        var d = dir;
        d.close(io);
        return error.DirAlreadyExists;
    } else |err| switch (err) {
        error.FileNotFound => {
            try Dir.createDirAbsolute(io, path, .default_dir);
        },
        else => return err,
    }

    // for mem table it's expect to have a single bloom filter shard
    std.debug.assert(self.streamWriter.bloomTokensList.items.len <= 1);
    std.debug.assert(self.streamWriter.bloomValuesList.items.len <= 1);

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

    try fs.writeBufferValToFile(io, columnKeysPath, self.streamWriter.columnKeysBuf.asSliceAssumeBuffer());
    try fs.writeBufferValToFile(io, columnIdxsPath, self.streamWriter.columnIdxsBuf.asSliceAssumeBuffer());
    try fs.writeBufferValToFile(io, metaindexPath, self.streamWriter.metaIndexDst.asSliceAssumeBuffer());
    try fs.writeBufferValToFile(io, indexPath, self.streamWriter.indexDst.asSliceAssumeBuffer());
    try fs.writeBufferValToFile(io, columnsHeaderIndexPath, self.streamWriter.columnsHeaderIndexDst.asSliceAssumeBuffer());
    try fs.writeBufferValToFile(io, columnsHeaderPath, self.streamWriter.columnsHeaderDst.asSliceAssumeBuffer());
    try fs.writeBufferValToFile(io, timestampsPath, self.streamWriter.timestampsDst.asSliceAssumeBuffer());

    try fs.writeBufferValToFile(
        io,
        messageBloomFilterPath,
        self.streamWriter.messageBloomTokensDst.asSliceAssumeBuffer(),
    );
    try fs.writeBufferValToFile(
        io,
        messageValuesPath,
        self.streamWriter.messageBloomValuesDst.asSliceAssumeBuffer(),
    );

    const bloomTokensPath = try getBloomTokensFilePath(allocator, path, 0);
    defer allocator.free(bloomTokensPath);
    const bloomTokensContent = if (self.streamWriter.bloomTokensList.items.len > 0)
        self.streamWriter.bloomTokensList.items[0].asSliceAssumeBuffer()
    else
        "";
    try fs.writeBufferValToFile(io, bloomTokensPath, bloomTokensContent);

    const bloomValuesPath = try getBloomValuesFilePath(allocator, path, 0);
    defer allocator.free(bloomValuesPath);
    const bloomValuesContent = if (self.streamWriter.bloomValuesList.items.len > 0)
        self.streamWriter.bloomValuesList.items[0].asSliceAssumeBuffer()
    else
        "";
    try fs.writeBufferValToFile(io, bloomValuesPath, bloomValuesContent);

    try self.tableHeader.writeFile(io, allocator, path);

    fs.syncPathAndParentDir(io, path);
}

const BlockHeader = @import("block_header.zig").BlockHeader;
const IndexBlockHeader = @import("IndexBlockHeader.zig");
const TimestampsEncoder = @import("TimestampsEncoder.zig");
const EncodingType = @import("TimestampsEncoder.zig").EncodingType;
const encoding = @import("encoding");

const SampleLines = struct {
    fields1: [2]Field,
    fields2: [2]Field,
    lines: [2]Line,
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
    sample.lines = .{
        .{
            .timestampNs = 2,
            .sid = .{ .id = 1, .tenantID = "1234" },
            .fields = sample.fields2[0..],
        },
        .{
            .timestampNs = 1,
            .sid = .{ .id = 1, .tenantID = "1234" },
            .fields = sample.fields1[0..],
        },
    };
}

fn readFileAll(io: Io, allocator: std.mem.Allocator, path: []const u8) ![]u8 {
    var file = try std.Io.Dir.cwd().openFile(io, path, .{});
    defer file.close(io);

    var file_reader = file.reader(io, &.{});
    return file_reader.interface.allocRemaining(allocator, .limited(std.math.maxInt(usize)));
}

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

    const memTable = try MemTable.init(io, allocator);
    defer memTable.deinit(io, allocator);
    try memTable.addLines(io, allocator, lines[0..]);

    const timestampsContent = memTable.streamWriter.timestampsDst.asSliceAssumeBuffer();
    const indexContent = memTable.streamWriter.indexDst.asSliceAssumeBuffer();

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
        try std.testing.expectEqualStrings("1234", blockHeader.sid.tenantID);
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
        const metaIndexContent = memTable.streamWriter.metaIndexDst.asSliceAssumeBuffer();
        try std.testing.expect(metaIndexContent.len > 0);

        const decompressedSize = try encoding.getFrameContentSize(metaIndexContent);
        const decompressedBuf = try allocator.alloc(u8, decompressedSize);
        defer allocator.free(decompressedBuf);
        _ = try encoding.decompress(decompressedBuf, metaIndexContent);

        const decodedIndexBlockHeader = IndexBlockHeader.decode(decompressedBuf);

        // TODO: compare all the fields in one expect
        try std.testing.expectEqualStrings("1234", decodedIndexBlockHeader.sid.tenantID);
        try std.testing.expectEqual(1, decodedIndexBlockHeader.sid.id);
        try std.testing.expectEqual(1, decodedIndexBlockHeader.minTs);
        try std.testing.expectEqual(2, decodedIndexBlockHeader.maxTs);
        try std.testing.expectEqual(0, decodedIndexBlockHeader.offset);
        try std.testing.expectEqual(@as(u64, @intCast(indexContent.len)), decodedIndexBlockHeader.size);
    }

    // validate bloom filters
    {
        const messageBloomTokensContent = memTable.streamWriter.messageBloomTokensDst.asSliceAssumeBuffer();
        const bloomTokensList = memTable.streamWriter.bloomTokensList.items;
        const messageBloomValuesContent = memTable.streamWriter.messageBloomValuesDst.asSliceAssumeBuffer();
        const bloomValuesList = memTable.streamWriter.bloomValuesList.items;

        try std.testing.expectEqual(0, messageBloomTokensContent.len);
        try std.testing.expectEqual(0, messageBloomValuesContent.len);

        for (bloomTokensList) |bloomBuf| {
            try std.testing.expectEqual(0, bloomBuf.len());
        }
        for (bloomValuesList) |bloomValuesBuf| {
            try std.testing.expect(bloomValuesBuf.len() > 0);
        }
    }
}

test "addLinesErrorOnEmpty" {
    var lines = [_]Line{};
    const memTable = try MemTable.init(std.testing.io, std.testing.allocator);
    defer memTable.deinit(std.testing.io, std.testing.allocator);
    const err = memTable.addLines(std.testing.io, std.testing.allocator, lines[0..]);
    try std.testing.expectError(Error.EmptyLines, err);
}

test "flushToDisk writes buffers" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, testFlushToDisk, .{std.testing.io});
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

    const memTable = try MemTable.init(io, allocator);
    defer memTable.deinit(io, allocator);

    try memTable.addLines(io, allocator, lines[0..]);
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
    const bloomTokensPath = try getBloomTokensFilePath(allocator, flushPath, 0);
    defer allocator.free(bloomTokensPath);
    const bloomValuesPath = try getBloomValuesFilePath(allocator, flushPath, 0);
    defer allocator.free(bloomValuesPath);
    const metadataPath = try std.fs.path.join(allocator, &.{ flushPath, filenames.header });
    defer allocator.free(metadataPath);

    const columnKeysContent = try readFileAll(io, allocator, columnKeysPath);
    defer allocator.free(columnKeysContent);
    try std.testing.expectEqualSlices(u8, memTable.streamWriter.columnKeysBuf.asSliceAssumeBuffer(), columnKeysContent);

    const columnIdxsContent = try readFileAll(io, allocator, columnIdxsPath);
    defer allocator.free(columnIdxsContent);
    try std.testing.expectEqualSlices(u8, memTable.streamWriter.columnIdxsBuf.asSliceAssumeBuffer(), columnIdxsContent);

    const metaindexContent = try readFileAll(io, allocator, metaindexPath);
    defer allocator.free(metaindexContent);
    try std.testing.expectEqualSlices(u8, memTable.streamWriter.metaIndexDst.asSliceAssumeBuffer(), metaindexContent);

    const indexContent = try readFileAll(io, allocator, indexPath);
    defer allocator.free(indexContent);
    try std.testing.expectEqualSlices(u8, memTable.streamWriter.indexDst.asSliceAssumeBuffer(), indexContent);

    const columnsHeaderIndexContent = try readFileAll(io, allocator, columnsHeaderIndexPath);
    defer allocator.free(columnsHeaderIndexContent);
    try std.testing.expectEqualSlices(u8, memTable.streamWriter.columnsHeaderIndexDst.asSliceAssumeBuffer(), columnsHeaderIndexContent);

    const columnsHeaderContent = try readFileAll(io, allocator, columnsHeaderPath);
    defer allocator.free(columnsHeaderContent);
    try std.testing.expectEqualSlices(u8, memTable.streamWriter.columnsHeaderDst.asSliceAssumeBuffer(), columnsHeaderContent);

    const timestampsContent = try readFileAll(io, allocator, timestampsPath);
    defer allocator.free(timestampsContent);
    try std.testing.expectEqualSlices(u8, memTable.streamWriter.timestampsDst.asSliceAssumeBuffer(), timestampsContent);

    const msgBloomTokensContent = try readFileAll(io, allocator, messageBloomTokensPath);
    defer allocator.free(msgBloomTokensContent);
    try std.testing.expectEqualSlices(u8, memTable.streamWriter.messageBloomTokensDst.asSliceAssumeBuffer(), msgBloomTokensContent);

    const msgBloomValuesContent = try readFileAll(io, allocator, messageBloomValuesPath);
    defer allocator.free(msgBloomValuesContent);
    try std.testing.expectEqualSlices(u8, memTable.streamWriter.messageBloomValuesDst.asSliceAssumeBuffer(), msgBloomValuesContent);

    const bloomTokensContent = try readFileAll(io, allocator, bloomTokensPath);
    defer allocator.free(bloomTokensContent);
    try std.testing.expectEqualSlices(u8, memTable.streamWriter.bloomTokensList.items[0].asSliceAssumeBuffer(), bloomTokensContent);

    const bloomValuesContent = try readFileAll(io, allocator, bloomValuesPath);
    defer allocator.free(bloomValuesContent);
    try std.testing.expectEqualSlices(u8, memTable.streamWriter.bloomValuesList.items[0].asSliceAssumeBuffer(), bloomValuesContent);

    const metadataContent = try readFileAll(io, allocator, metadataPath);
    defer allocator.free(metadataContent);
    try std.testing.expect(metadataContent.len > 0);

    try std.testing.expectError(error.DirAlreadyExists, memTable.storeToDisk(io, allocator, flushPath));
}
