const std = @import("std");
const fs = @import("../../fs.zig");

const Field = @import("../lines.zig").Field;
const Line = @import("../lines.zig").Line;
const lineLessThan = @import("../lines.zig").lineLessThan;
const fieldLessThan = @import("../lines.zig").fieldLessThan;
const SID = @import("../lines.zig").SID;

const StreamWriter = @import("StreamWriter.zig");
const BlockWriter = @import("BlockWriter.zig");
const TableHeader = @import("TableHeader.zig");
const Filenames = @import("Filenames.zig");

// 2mb block size, on merging it takes double amount up to 4mb
// TODO: benchmark whether 2.5-3kb performs better
pub const maxBlockSize = 2 * 1024 * 1024;

pub const Error = error{
    EmptyLines,
};

const Self = @This();

streamWriter: *StreamWriter,
tableHeader: *TableHeader,

flushAtUs: ?i64 = null,
isInMerge: bool = false,

pub fn init(allocator: std.mem.Allocator) !*Self {
    const streamWriter = try StreamWriter.init(allocator, 1);
    errdefer streamWriter.deinit(allocator);

    const th = try TableHeader.init(allocator);
    errdefer th.deinit(allocator);

    const p = try allocator.create(Self);
    p.* = Self{
        .streamWriter = streamWriter,
        .tableHeader = th,
    };

    return p;
}
pub fn deinit(self: *Self, allocator: std.mem.Allocator) void {
    self.streamWriter.deinit(allocator);
    self.tableHeader.deinit(allocator);
    allocator.destroy(self);
}

pub fn addLines(self: *Self, allocator: std.mem.Allocator, lines: []*const Line) !void {
    if (lines.len == 0) {
        return Error.EmptyLines;
    }

    var blockWriter = try BlockWriter.init(allocator);
    defer blockWriter.deinit(allocator);

    var streamI: usize = 0;
    var blockSize: u32 = 0;
    var prevSID: SID = lines[0].sid;

    std.mem.sortUnstable(*const Line, lines, {}, lineLessThan);
    for (lines, 0..) |line, i| {
        std.mem.sortUnstable(Field, line.fields, {}, fieldLessThan);

        if (blockSize >= maxBlockSize or !line.sid.eql(&prevSID)) {
            try blockWriter.writeLines(allocator, prevSID, lines[streamI..i], self.streamWriter);
            prevSID = line.sid;
            blockSize = 0;
            streamI = i;
        }
        blockSize += line.fieldsSize();
    }
    if (streamI != lines.len) {
        try blockWriter.writeLines(allocator, prevSID, lines[streamI..], self.streamWriter);
    }
    try blockWriter.finish(allocator, self.streamWriter, self.tableHeader);
}

pub fn getFilePathSharded(
    allocator: std.mem.Allocator,
    part_path: []const u8,
    values_filename: []const u8,
    shard_idx: u64,
) ![]u8 {
    return std.fmt.allocPrint(
        allocator,
        "{s}/{s}{}",
        .{ part_path, values_filename, shard_idx },
    );
}

pub fn flushToDisk(self: *Self, alloc: std.mem.Allocator, path: []const u8) !void {
    // TODO: make this function parallel when it comes to writing files
    if (std.fs.openDirAbsolute(path, .{})) |dir| {
        var d = dir;
        d.close();
        return error.DirAlreadyExists;
    } else |err| switch (err) {
        error.FileNotFound => {
            try std.fs.makeDirAbsolute(path);
        },
        else => return err,
    }

    std.debug.assert(self.streamWriter.bloomTokensList.items.len == 1);
    std.debug.assert(self.streamWriter.bloomValuesList.items.len == 1);

    var stack = std.heap.stackFallback(2048, alloc);
    const allocator = stack.get();

    const columnNamesPath =
        try std.fs.path.join(allocator, &.{ path, Filenames.columnNames });
    defer allocator.free(columnNamesPath);

    const columnIdxsPath =
        try std.fs.path.join(allocator, &.{ path, Filenames.columnIdxs });
    defer allocator.free(columnIdxsPath);

    const metaindexPath =
        try std.fs.path.join(allocator, &.{ path, Filenames.metaindex });
    defer allocator.free(metaindexPath);

    const indexPath =
        try std.fs.path.join(allocator, &.{ path, Filenames.index });
    defer allocator.free(indexPath);

    const columnsHeaderIndexPath =
        try std.fs.path.join(allocator, &.{ path, Filenames.columnsHeaderIndex });
    defer allocator.free(columnsHeaderIndexPath);

    const columnsHeaderPath =
        try std.fs.path.join(allocator, &.{ path, Filenames.columnsHeader });
    defer allocator.free(columnsHeaderPath);

    const timestampsPath =
        try std.fs.path.join(allocator, &.{ path, Filenames.timestamps });
    defer allocator.free(timestampsPath);

    const messageValuesPath =
        try std.fs.path.join(allocator, &.{ path, Filenames.messageValues });
    defer allocator.free(messageValuesPath);

    const messageBloomFilterPath =
        try std.fs.path.join(allocator, &.{ path, Filenames.messageBloom });
    defer allocator.free(messageBloomFilterPath);

    try fs.writeBufferValToFile(columnNamesPath, self.streamWriter.columnKeysBuf.items);
    try fs.writeBufferValToFile(columnIdxsPath, self.streamWriter.columnIdxsBuf.items);
    try fs.writeBufferValToFile(metaindexPath, self.streamWriter.metaIndexBuf.items);
    try fs.writeBufferValToFile(indexPath, self.streamWriter.indexBuf.items);
    try fs.writeBufferValToFile(columnsHeaderIndexPath, self.streamWriter.columnsHeaderIndexBuf.items);
    try fs.writeBufferValToFile(columnsHeaderPath, self.streamWriter.columnsHeaderBuf.items);
    try fs.writeBufferValToFile(timestampsPath, self.streamWriter.timestampsBuf.items);

    try fs.writeBufferValToFile(
        messageBloomFilterPath,
        self.streamWriter.messageBloomTokensBuf.items,
    );
    try fs.writeBufferValToFile(
        messageValuesPath,
        self.streamWriter.messageBloomValuesBuf.items,
    );

    const bloomPath = try getFilePathSharded(allocator, path, Filenames.values, 0);
    defer allocator.free(bloomPath);

    try fs.writeBufferValToFile(bloomPath, self.streamWriter.bloomTokensList.items[0].items);

    const valuesPath = try getFilePathSharded(allocator, path, Filenames.bloom, 0);
    defer allocator.free(valuesPath);
    try fs.writeBufferValToFile(valuesPath, self.streamWriter.bloomValuesList.items[0].items);

    try self.tableHeader.flushMetadata(allocator, path, Filenames.metadata);

    fs.syncPathAndParentDir(path);
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

fn readFileAll(allocator: std.mem.Allocator, path: []const u8) ![]u8 {
    var file = try std.fs.cwd().openFile(path, .{});
    defer file.close();
    return file.readToEndAlloc(allocator, std.math.maxInt(usize));
}

test "addLines" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, testAddLines, .{});
}

fn testAddLines(allocator: std.mem.Allocator) !void {
    var sample: SampleLines = SampleLines{
        .fields1 = undefined,
        .fields2 = undefined,
        .lines = undefined,
    };
    populateSampleLines(&sample);

    // unordered timestamps in lines so that it tests its sorting
    var lines = [2]*const Line{
        &sample.lines[0],
        &sample.lines[1],
    };

    const memTable = try Self.init(allocator);
    defer memTable.deinit(allocator);
    try memTable.addLines(allocator, lines[0..]);

    const timestampsContent = memTable.streamWriter.timestampsBuf.items;
    const indexContent = memTable.streamWriter.indexBuf.items;

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

        const blockHeader = BlockHeader.decode(decompressedBuf);

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
        const metaIndexContent = memTable.streamWriter.metaIndexBuf.items;
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
        const messageBloomTokensContent = memTable.streamWriter.messageBloomTokensBuf.items;
        const bloomTokensList = memTable.streamWriter.bloomTokensList.items;
        const messageBloomValuesContent = memTable.streamWriter.messageBloomValuesBuf.items;
        const bloomValuesList = memTable.streamWriter.bloomValuesList.items;

        try std.testing.expectEqual(0, messageBloomTokensContent.len);
        try std.testing.expectEqual(0, messageBloomValuesContent.len);

        for (bloomTokensList) |bloomBuf| {
            try std.testing.expectEqual(0, bloomBuf.items.len);
        }
        for (bloomValuesList) |bloomValuesBuf| {
            try std.testing.expect(bloomValuesBuf.items.len > 0);
        }
    }
}

test "addLinesErrorOnEmpty" {
    var lines = [_]*const Line{};
    const memTable = try Self.init(std.testing.allocator);
    defer memTable.deinit(std.testing.allocator);
    const err = memTable.addLines(std.testing.allocator, lines[0..]);
    try std.testing.expectError(Error.EmptyLines, err);
}

test "flushToDisk writes buffers" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, testFlushToDisk, .{});
}

fn testFlushToDisk(allocator: std.mem.Allocator) !void {
    var sample: SampleLines = SampleLines{
        .fields1 = undefined,
        .fields2 = undefined,
        .lines = undefined,
    };
    populateSampleLines(&sample);
    var lines = [2]*const Line{
        &sample.lines[0],
        &sample.lines[1],
    };

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const basePath = try tmp.dir.realpathAlloc(allocator, ".");
    defer allocator.free(basePath);
    const flushPath = try std.fs.path.join(allocator, &.{ basePath, "flush" });
    defer allocator.free(flushPath);

    const memTable = try Self.init(allocator);
    defer memTable.deinit(allocator);

    try memTable.addLines(allocator, lines[0..]);
    try memTable.flushToDisk(allocator, flushPath);

    const columnNamesPath = try std.fs.path.join(allocator, &.{ flushPath, Filenames.columnNames });
    defer allocator.free(columnNamesPath);
    const columnIdxsPath = try std.fs.path.join(allocator, &.{ flushPath, Filenames.columnIdxs });
    defer allocator.free(columnIdxsPath);
    const metaindexPath = try std.fs.path.join(allocator, &.{ flushPath, Filenames.metaindex });
    defer allocator.free(metaindexPath);
    const indexPath = try std.fs.path.join(allocator, &.{ flushPath, Filenames.index });
    defer allocator.free(indexPath);
    const columnsHeaderIndexPath = try std.fs.path.join(allocator, &.{ flushPath, Filenames.columnsHeaderIndex });
    defer allocator.free(columnsHeaderIndexPath);
    const columnsHeaderPath = try std.fs.path.join(allocator, &.{ flushPath, Filenames.columnsHeader });
    defer allocator.free(columnsHeaderPath);
    const timestampsPath = try std.fs.path.join(allocator, &.{ flushPath, Filenames.timestamps });
    defer allocator.free(timestampsPath);
    const messageBloomTokensPath = try std.fs.path.join(allocator, &.{ flushPath, Filenames.messageBloom });
    defer allocator.free(messageBloomTokensPath);
    const messageBloomValuesPath = try std.fs.path.join(allocator, &.{ flushPath, Filenames.messageValues });
    defer allocator.free(messageBloomValuesPath);
    const bloomTokensPath = try getFilePathSharded(allocator, flushPath, Filenames.values, 0);
    defer allocator.free(bloomTokensPath);
    const bloomValuesPath = try getFilePathSharded(allocator, flushPath, Filenames.bloom, 0);
    defer allocator.free(bloomValuesPath);
    const metadataPath = try std.fs.path.join(allocator, &.{ flushPath, Filenames.metadata });
    defer allocator.free(metadataPath);

    const columnNamesContent = try readFileAll(allocator, columnNamesPath);
    defer allocator.free(columnNamesContent);
    try std.testing.expectEqualSlices(u8, memTable.streamWriter.columnKeysBuf.items, columnNamesContent);

    const columnIdxsContent = try readFileAll(allocator, columnIdxsPath);
    defer allocator.free(columnIdxsContent);
    try std.testing.expectEqualSlices(u8, memTable.streamWriter.columnIdxsBuf.items, columnIdxsContent);

    const metaindexContent = try readFileAll(allocator, metaindexPath);
    defer allocator.free(metaindexContent);
    try std.testing.expectEqualSlices(u8, memTable.streamWriter.metaIndexBuf.items, metaindexContent);

    const indexContent = try readFileAll(allocator, indexPath);
    defer allocator.free(indexContent);
    try std.testing.expectEqualSlices(u8, memTable.streamWriter.indexBuf.items, indexContent);

    const columnsHeaderIndexContent = try readFileAll(allocator, columnsHeaderIndexPath);
    defer allocator.free(columnsHeaderIndexContent);
    try std.testing.expectEqualSlices(u8, memTable.streamWriter.columnsHeaderIndexBuf.items, columnsHeaderIndexContent);

    const columnsHeaderContent = try readFileAll(allocator, columnsHeaderPath);
    defer allocator.free(columnsHeaderContent);
    try std.testing.expectEqualSlices(u8, memTable.streamWriter.columnsHeaderBuf.items, columnsHeaderContent);

    const timestampsContent = try readFileAll(allocator, timestampsPath);
    defer allocator.free(timestampsContent);
    try std.testing.expectEqualSlices(u8, memTable.streamWriter.timestampsBuf.items, timestampsContent);

    const msgBloomTokensContent = try readFileAll(allocator, messageBloomTokensPath);
    defer allocator.free(msgBloomTokensContent);
    try std.testing.expectEqualSlices(u8, memTable.streamWriter.messageBloomTokensBuf.items, msgBloomTokensContent);

    const msgBloomValuesContent = try readFileAll(allocator, messageBloomValuesPath);
    defer allocator.free(msgBloomValuesContent);
    try std.testing.expectEqualSlices(u8, memTable.streamWriter.messageBloomValuesBuf.items, msgBloomValuesContent);

    const bloomTokensContent = try readFileAll(allocator, bloomTokensPath);
    defer allocator.free(bloomTokensContent);
    try std.testing.expectEqualSlices(u8, memTable.streamWriter.bloomTokensList.items[0].items, bloomTokensContent);

    const bloomValuesContent = try readFileAll(allocator, bloomValuesPath);
    defer allocator.free(bloomValuesContent);
    try std.testing.expectEqualSlices(u8, memTable.streamWriter.bloomValuesList.items[0].items, bloomValuesContent);

    const metadataContent = try readFileAll(allocator, metadataPath);
    defer allocator.free(metadataContent);
    try std.testing.expect(metadataContent.len > 0);

    try std.testing.expectError(error.DirAlreadyExists, memTable.flushToDisk(allocator, flushPath));
}
