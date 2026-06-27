const std = @import("std");

const filenames = @import("filenames.zig");
const encoding = @import("encoding");
const Decoder = encoding.Decoder;
const BlockHeader = @import("store/data/BlockHeader.zig");
const ColumnDict = @import("store/data/ColumnDict.zig");
const ColumnHeader = @import("store/data/ColumnHeader.zig");
const ColumnIDGen = @import("store/data/ColumnIDGen.zig");
const IndexBlockHeader = @import("store/data/IndexBlockHeader.zig");
const Unpacker = @import("store/data/Unpacker.zig");

const HeaderKind = enum {
    metaindex,
    index,
    columnsHeaderIndex,
    columnsHeader,
};

const Args = struct {
    data: []const u8 = "",
    header: ?HeaderKind = null,
    offset: usize = 0,
    size: ?usize = null,
    indexOffset: ?usize = null,
    indexSize: ?usize = null,
    count: ?usize = null,
    validateValues: bool = false,
};

const SafeDecoder = struct {
    buf: []const u8,
    offset: usize = 0,

    fn readInt(self: *SafeDecoder, comptime T: type) !T {
        const size = @sizeOf(T);
        if (self.offset + size > self.buf.len) return error.UnexpectedEnd;
        const bytes: [size]u8 = self.buf[self.offset..][0..size].*;
        self.offset += size;
        return std.mem.readInt(T, &bytes, .big);
    }

    fn readBytes(self: *SafeDecoder, len: usize) ![]const u8 {
        if (self.offset + len > self.buf.len) return error.UnexpectedEnd;
        const res = self.buf[self.offset .. self.offset + len];
        self.offset += len;
        return res;
    }

    fn readString(self: *SafeDecoder) ![]const u8 {
        const len = try self.readVarInt();
        return self.readBytes(len);
    }

    fn readVarInt(self: *SafeDecoder) !usize {
        var result: u64 = 0;
        var shift: u6 = 0;

        for (0..10) |i| {
            if (self.offset + i >= self.buf.len) return error.UnexpectedEnd;
            const byte = self.buf[self.offset + i];
            result |= @as(u64, byte & 0x7f) << shift;

            if ((byte & 0x80) == 0) {
                self.offset += i + 1;
                return std.math.cast(usize, result) orelse error.IntOverflow;
            }

            shift += 7;
        }

        return error.InvalidLeb128;
    }
};

pub fn main(init: std.process.Init) !void {
    const allocator = init.gpa;
    const io = init.io;

    const args = try parseArgs(init.minimal.args, allocator);
    defer if (args.data.len > 0) allocator.free(args.data);
    if (args.data.len == 0 or args.header == null or args.size == null) {
        usage();
        return error.InvalidArgs;
    }

    const keys = loadColumnIDGen(io, allocator, args.data) catch |err| blk: {
        std.debug.print("columnKeys: failed to load: {s}\n", .{@errorName(err)});
        break :blk null;
    };
    defer if (keys) |gen| gen.deinit(allocator);

    switch (args.header.?) {
        .metaindex => {
            const buf = try readHeaderSlice(io, allocator, args.data, filenames.metaindex, args.offset, args.size.?);
            defer allocator.free(buf);
            try inspectMetaindex(allocator, buf);
        },
        .index => {
            const buf = try readHeaderSlice(io, allocator, args.data, filenames.index, args.offset, args.size.?);
            defer allocator.free(buf);
            try inspectIndex(allocator, buf);
        },
        .columnsHeaderIndex => {
            const buf = try readHeaderSlice(io, allocator, args.data, filenames.columnsHeaderIndex, args.offset, args.size.?);
            defer allocator.free(buf);
            try inspectColumnsHeaderIndex(allocator, buf, keys);
        },
        .columnsHeader => {
            const buf = try readHeaderSlice(io, allocator, args.data, filenames.columnsHeader, args.offset, args.size.?);
            defer allocator.free(buf);

            var index: ?ColumnsIndex = null;
            defer if (index) |*idx| idx.deinit(allocator);
            if (args.indexOffset) |indexOffset| {
                if (args.indexSize) |indexSize| {
                    const indexBuf = try readHeaderSlice(io, allocator, args.data, filenames.columnsHeaderIndex, indexOffset, indexSize);
                    defer allocator.free(indexBuf);
                    index = try ColumnsIndex.decode(allocator, indexBuf);
                }
            }

            var columnIdxs: ?ColumnIdxs = null;
            defer if (columnIdxs) |*idxs| idxs.deinit();
            if (args.validateValues) {
                if (keys) |gen| {
                    columnIdxs = try loadColumnIdxs(io, allocator, args.data, gen);
                }
            }

            try inspectColumnsHeader(
                io,
                allocator,
                args.data,
                buf,
                keys,
                if (index) |*idx| idx else null,
                if (columnIdxs) |*idxs| idxs else null,
                args.count,
                args.validateValues,
            );
        },
    }
}

fn parseArgs(processArgs: std.process.Args, allocator: std.mem.Allocator) !Args {
    var it = try std.process.Args.Iterator.initAllocator(processArgs, allocator);
    defer it.deinit();
    _ = it.skip();

    var args = Args{};
    while (it.next()) |arg| {
        if (argValue(arg, "--data:")) |v| {
            args.data = try allocator.dupe(u8, v);
        } else if (argValue(arg, "--header:")) |v| {
            args.header = std.meta.stringToEnum(HeaderKind, v) orelse return error.InvalidHeader;
        } else if (argValue(arg, "--offset:")) |v| {
            args.offset = try std.fmt.parseInt(usize, v, 10);
        } else if (argValue(arg, "--size:")) |v| {
            args.size = try std.fmt.parseInt(usize, v, 10);
        } else if (argValue(arg, "--index-offset:")) |v| {
            args.indexOffset = try std.fmt.parseInt(usize, v, 10);
        } else if (argValue(arg, "--index-size:")) |v| {
            args.indexSize = try std.fmt.parseInt(usize, v, 10);
        } else if (argValue(arg, "--count:")) |v| {
            args.count = try std.fmt.parseInt(usize, v, 10);
        } else if (std.mem.eql(u8, arg, "--validate-values")) {
            args.validateValues = true;
        } else {
            return error.UnknownArg;
        }
    }
    return args;
}

fn argValue(arg: []const u8, prefix: []const u8) ?[]const u8 {
    if (!std.mem.startsWith(u8, arg, prefix)) return null;
    return arg[prefix.len..];
}

fn usage() void {
    std.debug.print(
        \\usage:
        \\  zig build scooby -- --data:<table path> --header:metaindex --offset:<offset> --size:<size>
        \\  zig build scooby -- --data:<table path> --header:index --offset:<offset> --size:<size>
        \\  zig build scooby -- --data:<table path> --header:columnsHeaderIndex --offset:<offset> --size:<size>
        \\  zig build scooby -- --data:<table path> --header:columnsHeader --offset:<offset> --size:<size> [--index-offset:<offset> --index-size:<size>] [--count:<rows> --validate-values]
        \\
    , .{});
}

fn readHeaderSlice(
    io: std.Io,
    allocator: std.mem.Allocator,
    tablePath: []const u8,
    fileName: []const u8,
    offset: usize,
    size: usize,
) ![]u8 {
    const path = try std.fs.path.join(allocator, &.{ tablePath, fileName });
    defer allocator.free(path);

    var file = try std.Io.Dir.openFileAbsolute(io, path, .{});
    defer file.close(io);

    const fileSize = (try file.stat(io)).size;
    if (offset > fileSize) return error.OffsetOutOfRange;
    if (size > fileSize - offset) return error.SizeOutOfRange;

    const buf = try allocator.alloc(u8, size);
    errdefer allocator.free(buf);

    const n = try file.readPositionalAll(io, buf, offset);
    if (n != size) return error.ShortRead;
    return buf;
}

fn loadColumnIDGen(io: std.Io, allocator: std.mem.Allocator, tablePath: []const u8) !?*ColumnIDGen {
    const path = try std.fs.path.join(allocator, &.{ tablePath, filenames.columnKeys });
    defer allocator.free(path);

    var file = std.Io.Dir.openFileAbsolute(io, path, .{}) catch |err| switch (err) {
        error.FileNotFound => return null,
        else => return err,
    };
    defer file.close(io);

    const size = (try file.stat(io)).size;
    if (size == 0) return try ColumnIDGen.init(allocator);

    const buf = try allocator.alloc(u8, size);
    defer allocator.free(buf);
    _ = try file.readPositionalAll(io, buf, 0);
    return try ColumnIDGen.decode(allocator, buf);
}

const ColumnIdxs = std.AutoHashMap(u16, u16);

fn loadColumnIdxs(
    io: std.Io,
    allocator: std.mem.Allocator,
    tablePath: []const u8,
    keys: *ColumnIDGen,
) !ColumnIdxs {
    const path = try std.fs.path.join(allocator, &.{ tablePath, filenames.columnIdxs });
    defer allocator.free(path);

    var idxs = ColumnIdxs.init(allocator);
    errdefer idxs.deinit();

    var file = std.Io.Dir.openFileAbsolute(io, path, .{}) catch |err| switch (err) {
        error.FileNotFound => return idxs,
        else => return err,
    };
    defer file.close(io);

    const size = (try file.stat(io)).size;
    if (size == 0) return idxs;

    const buf = try allocator.alloc(u8, size);
    defer allocator.free(buf);
    _ = try file.readPositionalAll(io, buf, 0);

    var dec = SafeDecoder{ .buf = buf };
    const count = try dec.readVarInt();
    try idxs.ensureTotalCapacity(@intCast(count));
    const keyItems = keys.keyIDs.keys();
    for (0..count) |_| {
        const colID = try dec.readVarInt();
        const shardIdx = try dec.readVarInt();
        if (colID >= keyItems.len) return error.ColumnIDOutOfRange;
        idxs.putAssumeCapacity(
            std.math.cast(u16, colID) orelse return error.ColumnIDOutOfRange,
            std.math.cast(u16, shardIdx) orelse return error.ShardIDOutOfRange,
        );
    }
    return idxs;
}

const ColumnsIndex = struct {
    columnsIDs: std.ArrayList(u16),
    columnOffsets: std.ArrayList(u32),
    invariantColumnsIDs: std.ArrayList(u16),
    invariantColumnOffsets: std.ArrayList(u32),

    fn init(allocator: std.mem.Allocator) ColumnsIndex {
        _ = allocator;
        return .{
            .columnsIDs = .empty,
            .columnOffsets = .empty,
            .invariantColumnsIDs = .empty,
            .invariantColumnOffsets = .empty,
        };
    }

    fn deinit(self: *ColumnsIndex, allocator: std.mem.Allocator) void {
        self.columnsIDs.deinit(allocator);
        self.columnOffsets.deinit(allocator);
        self.invariantColumnsIDs.deinit(allocator);
        self.invariantColumnOffsets.deinit(allocator);
    }

    fn decode(allocator: std.mem.Allocator, buf: []const u8) !ColumnsIndex {
        var idx = ColumnsIndex.init(allocator);
        errdefer idx.deinit(allocator);

        var dec = SafeDecoder{ .buf = buf };
        try decodeIndexEntries(allocator, &dec, &idx.columnsIDs, &idx.columnOffsets);
        try decodeIndexEntries(allocator, &dec, &idx.invariantColumnsIDs, &idx.invariantColumnOffsets);
        return idx;
    }
};

fn decodeIndexEntries(
    allocator: std.mem.Allocator,
    dec: *SafeDecoder,
    ids: *std.ArrayList(u16),
    offsets: *std.ArrayList(u32),
) !void {
    const len = try dec.readVarInt();
    try ids.ensureTotalCapacity(allocator, len);
    try offsets.ensureTotalCapacity(allocator, len);
    for (0..len) |_| {
        const id = try dec.readVarInt();
        const off = try dec.readVarInt();
        ids.appendAssumeCapacity(std.math.cast(u16, id) orelse return error.ColumnIDOutOfRange);
        offsets.appendAssumeCapacity(std.math.cast(u32, off) orelse return error.ColumnOffsetOutOfRange);
    }
}

fn inspectColumnsHeaderIndex(allocator: std.mem.Allocator, buf: []const u8, keys: ?*ColumnIDGen) !void {
    std.debug.print("columnsHeaderIndex bytes={d}\n", .{buf.len});

    var idx = try ColumnsIndex.decode(allocator, buf);
    defer idx.deinit(allocator);

    std.debug.print("columns len={d}\n", .{idx.columnsIDs.items.len});
    for (idx.columnsIDs.items, idx.columnOffsets.items, 0..) |id, off, i| {
        printIndexEntry("column", i, id, off, keys);
    }

    std.debug.print("invariantColumns len={d}\n", .{idx.invariantColumnsIDs.items.len});
    for (idx.invariantColumnsIDs.items, idx.invariantColumnOffsets.items, 0..) |id, off, i| {
        printIndexEntry("invariant", i, id, off, keys);
    }

    std.debug.print("consumed={d} remaining=0\n", .{buf.len});
}

fn inspectMetaindex(allocator: std.mem.Allocator, buf: []const u8) !void {
    std.debug.print("metaindex bytes={d}\n", .{buf.len});
    const headers = try IndexBlockHeader.readIndexBlockHeaders(allocator, buf);
    defer allocator.free(headers);

    std.debug.print("indexBlockHeaders len={d}\n", .{headers.len});
    for (headers, 0..) |header, i| {
        std.debug.print(
            "indexBlock[{d}] sid={any} minTs={d} maxTs={d} offset={d} size={d}\n",
            .{ i, header.sid, header.minTs, header.maxTs, header.offset, header.size },
        );
    }
}

fn inspectIndex(allocator: std.mem.Allocator, buf: []const u8) !void {
    std.debug.print("index bytes={d}\n", .{buf.len});
    const decompressedSize = try encoding.getFrameContentSize(buf);
    const decompressed = try allocator.alloc(u8, decompressedSize);
    defer allocator.free(decompressed);
    const n = try encoding.decompress(decompressed, buf);
    const src = decompressed[0..n];

    var off: usize = 0;
    var i: usize = 0;
    while (off < src.len) : (i += 1) {
        const decoded = BlockHeader.decode(src[off..]);
        const h = decoded.header;
        std.debug.print(
            "block[{d}] sid={any} size={d} len={d} tsOffset={d} tsSize={d} minTs={d} maxTs={d} tsEncoding={s} columnsHeaderIndexOffset={d} columnsHeaderIndexSize={d} columnsHeaderOffset={d} columnsHeaderSize={d}\n",
            .{
                i,
                h.sid,
                h.size,
                h.len,
                h.timestampsHeader.offset,
                h.timestampsHeader.size,
                h.timestampsHeader.min,
                h.timestampsHeader.max,
                @tagName(h.timestampsHeader.encodingType),
                h.columnsHeaderIndexOffset,
                h.columnsHeaderIndexSize,
                h.columnsHeaderOffset,
                h.columnsHeaderSize,
            },
        );
        off += decoded.offset;
    }
    std.debug.print("consumed={d} remaining={d}\n", .{ off, src.len - off });
}

fn printIndexEntry(kind: []const u8, i: usize, id: u16, off: u32, keys: ?*ColumnIDGen) void {
    std.debug.print("{s}[{d}] id={d} offset={d}", .{ kind, i, id, off });
    if (keyForID(keys, id)) |key| {
        std.debug.print(" key=\"{s}\"", .{key});
    } else {
        std.debug.print(" key=<unknown>", .{});
    }
    std.debug.print("\n", .{});
}

fn keyForID(keys: ?*ColumnIDGen, id: u16) ?[]const u8 {
    const gen = keys orelse return null;
    const keyItems = gen.keyIDs.keys();
    if (id >= keyItems.len) return null;
    return keyItems[id];
}

fn inspectColumnsHeader(
    io: std.Io,
    allocator: std.mem.Allocator,
    tablePath: []const u8,
    buf: []const u8,
    keys: ?*ColumnIDGen,
    index: ?*const ColumnsIndex,
    columnIdxs: ?*ColumnIdxs,
    count: ?usize,
    validateValues: bool,
) !void {
    var dec = SafeDecoder{ .buf = buf };
    const headersLen = try dec.readVarInt();
    std.debug.print("columnsHeader bytes={d} columns={d}\n", .{ buf.len, headersLen });

    for (0..headersLen) |i| {
        const start = dec.offset;
        const key = keyForColumnIndex(keys, index, i);
        const colID = columnIDForColumnIndex(index, i);
        try inspectColumnHeader(
            io,
            allocator,
            tablePath,
            &dec,
            i,
            start,
            key,
            colID,
            columnIdxs,
            count,
            validateValues,
        );
    }

    const invariantLenOffset = dec.offset;
    const invariantLen = try dec.readVarInt();
    std.debug.print("invariantColumns offset={d} len={d}\n", .{ invariantLenOffset, invariantLen });

    for (0..invariantLen) |i| {
        const start = dec.offset;
        const value = try dec.readString();
        const key = keyForInvariantIndex(keys, index, i);
        std.debug.print("invariant[{d}] offset={d} len={d}", .{ i, start, value.len });
        if (key) |k| {
            std.debug.print(" key=\"{s}\"", .{k});
        }
        std.debug.print(" value=\"{s}\"\n", .{value});
    }

    std.debug.print("consumed={d} remaining={d}\n", .{ dec.offset, buf.len - dec.offset });
}

fn keyForColumnIndex(keys: ?*ColumnIDGen, index: ?*const ColumnsIndex, i: usize) ?[]const u8 {
    const idx = index orelse return null;
    if (i >= idx.columnsIDs.items.len) return null;
    return keyForID(keys, idx.columnsIDs.items[i]);
}

fn columnIDForColumnIndex(index: ?*const ColumnsIndex, i: usize) ?u16 {
    const idx = index orelse return null;
    if (i >= idx.columnsIDs.items.len) return null;
    return idx.columnsIDs.items[i];
}

fn keyForInvariantIndex(keys: ?*ColumnIDGen, index: ?*const ColumnsIndex, i: usize) ?[]const u8 {
    const idx = index orelse return null;
    if (i >= idx.invariantColumnsIDs.items.len) return null;
    return keyForID(keys, idx.invariantColumnsIDs.items[i]);
}

fn inspectColumnHeader(
    io: std.Io,
    allocator: std.mem.Allocator,
    tablePath: []const u8,
    dec: *SafeDecoder,
    i: usize,
    start: usize,
    key: ?[]const u8,
    colID: ?u16,
    columnIdxs: ?*ColumnIdxs,
    count: ?usize,
    validateValues: bool,
) !void {
    const typeRaw = try dec.readInt(u8);
    const columnType: ColumnHeader.ColumnType = @enumFromInt(typeRaw);

    std.debug.print("column[{d}] offset={d} type={s}", .{ i, start, @tagName(columnType) });
    if (key) |k| {
        std.debug.print(" key=\"{s}\"", .{k});
    }
    std.debug.print("\n", .{});

    switch (columnType) {
        .string, .unknown => {
            const valuesOffset = try dec.readVarInt();
            const valuesSize = try dec.readVarInt();
            const bloomOffset = try dec.readVarInt();
            const bloomSize = try dec.readVarInt();
            printValuesAndBloom(valuesOffset, valuesSize, bloomOffset, bloomSize);
        },
        .dict => {
            const dictValues = try inspectDict(allocator, dec);
            defer allocator.free(dictValues);
            const valuesOffset = try dec.readVarInt();
            const valuesSize = try dec.readVarInt();
            std.debug.print("  values offset={d} size={d}\n", .{ valuesOffset, valuesSize });
            if (validateValues) {
                try inspectDictValues(
                    io,
                    allocator,
                    tablePath,
                    key,
                    colID,
                    columnIdxs,
                    valuesOffset,
                    valuesSize,
                    dictValues,
                    count,
                );
            }
        },
        .uint8 => {
            const min = try dec.readInt(u8);
            const max = try dec.readInt(u8);
            try inspectNumberColumn(dec, min, max);
        },
        .uint16 => {
            const min = try dec.readInt(u16);
            const max = try dec.readInt(u16);
            try inspectNumberColumn(dec, min, max);
        },
        .uint32, .ipv4 => {
            const min = try dec.readInt(u32);
            const max = try dec.readInt(u32);
            try inspectNumberColumn(dec, min, max);
        },
        .uint64, .int64, .float64, .timestampIso8601 => {
            const min = try dec.readInt(u64);
            const max = try dec.readInt(u64);
            try inspectNumberColumn(dec, min, max);
        },
    }
}

fn inspectDict(allocator: std.mem.Allocator, dec: *SafeDecoder) ![][]const u8 {
    const len = try dec.readInt(u8);
    std.debug.print("  dict len={d}", .{len});
    if (len > ColumnDict.maxDictColumnValuesLen) {
        std.debug.print(" warning=len exceeds max {d}", .{ColumnDict.maxDictColumnValuesLen});
    }
    std.debug.print("\n", .{});

    const values = try allocator.alloc([]const u8, len);
    errdefer allocator.free(values);

    var total: usize = 0;
    for (0..len) |i| {
        const valueOffset = dec.offset;
        const dictValue = try dec.readString();
        values[i] = dictValue;
        total += dictValue.len;
        std.debug.print("  dict[{d}] offset={d} len={d} value=\"{s}\"", .{ i, valueOffset, dictValue.len, dictValue });
        if (dictValue.len > ColumnDict.maxDictColumnValueSize) {
            std.debug.print(" warning=value exceeds max {d}", .{ColumnDict.maxDictColumnValueSize});
        }
        std.debug.print("\n", .{});
    }
    if (total > ColumnDict.maxDictColumnValueSize) {
        std.debug.print("  dict totalValueBytes={d} warning=total exceeds max {d}\n", .{ total, ColumnDict.maxDictColumnValueSize });
    } else {
        std.debug.print("  dict totalValueBytes={d}\n", .{total});
    }
    return values;
}

fn inspectDictValues(
    io: std.Io,
    allocator: std.mem.Allocator,
    tablePath: []const u8,
    key: ?[]const u8,
    colID: ?u16,
    columnIdxs: ?*ColumnIdxs,
    valuesOffset: usize,
    valuesSize: usize,
    dictValues: []const []const u8,
    count: ?usize,
) !void {
    const rowCount = count orelse {
        std.debug.print("  values validation skipped: missing --count:<rows>\n", .{});
        return;
    };

    const packedValues = try readValuesSlice(
        io,
        allocator,
        tablePath,
        key,
        colID,
        columnIdxs,
        valuesOffset,
        valuesSize,
    );
    defer allocator.free(packedValues);

    var unpacker = try Unpacker.init(allocator);
    defer unpacker.deinit(allocator);

    const values = unpacker.unpackValues(allocator, packedValues, rowCount) catch |err| {
        std.debug.print("  values validation unpack_error={s} rows={d}\n", .{ @errorName(err), rowCount });
        return;
    };
    defer allocator.free(values);

    var counts = [_]usize{0} ** 256;
    var emptyCount: usize = 0;
    var invalidCount: usize = 0;
    var firstInvalid: ?struct { row: usize, len: usize, id: usize } = null;
    for (values, 0..) |value, row| {
        if (value.len == 0) {
            emptyCount += 1;
            if (firstInvalid == null) firstInvalid = .{ .row = row, .len = 0, .id = 0 };
            continue;
        }
        const id: usize = @intCast(value[0]);
        counts[id] += 1;
        if (id >= dictValues.len) {
            invalidCount += 1;
            if (firstInvalid == null) firstInvalid = .{ .row = row, .len = value.len, .id = id };
        }
    }

    std.debug.print(
        "  values validation rows={d} dictLen={d} empty={d} invalid={d}",
        .{ rowCount, dictValues.len, emptyCount, invalidCount },
    );
    if (firstInvalid) |bad| {
        std.debug.print(" firstInvalidRow={d} firstInvalidLen={d} firstInvalidId={d}", .{ bad.row, bad.len, bad.id });
    }
    std.debug.print("\n", .{});

    const limit = @min(dictValues.len + 4, counts.len);
    for (0..limit) |id| {
        if (counts[id] == 0) continue;
        std.debug.print("    id[{d}] count={d}", .{ id, counts[id] });
        if (id < dictValues.len) {
            std.debug.print(" value=\"{s}\"", .{dictValues[id]});
        } else {
            std.debug.print(" out_of_range", .{});
        }
        std.debug.print("\n", .{});
    }
}

fn readValuesSlice(
    io: std.Io,
    allocator: std.mem.Allocator,
    tablePath: []const u8,
    key: ?[]const u8,
    colID: ?u16,
    columnIdxs: ?*ColumnIdxs,
    offset: usize,
    size: usize,
) ![]u8 {
    var pathBuf: [std.fs.max_path_bytes]u8 = undefined;
    const filePath = if (key) |k| blk: {
        if (k.len == 0) {
            break :blk try std.fs.path.join(allocator, &.{ tablePath, filenames.messageValues });
        }
        const id = colID orelse return error.MissingColumnID;
        const idxs = columnIdxs orelse return error.MissingColumnIdxs;
        const shardIdx = idxs.get(id) orelse return error.MissingColumnShard;
        const path = try filenames.writeBloomFilePath(&pathBuf, tablePath, filenames.bloomValues, shardIdx);
        break :blk try allocator.dupe(u8, path);
    } else return error.MissingColumnKey;
    defer allocator.free(filePath);

    std.debug.print("  values source=\"{s}\"\n", .{filePath});

    var file = try std.Io.Dir.openFileAbsolute(io, filePath, .{});
    defer file.close(io);

    const fileSize = (try file.stat(io)).size;
    if (offset > fileSize) return error.OffsetOutOfRange;
    if (size > fileSize - offset) return error.SizeOutOfRange;

    const buf = try allocator.alloc(u8, size);
    errdefer allocator.free(buf);
    const n = try file.readPositionalAll(io, buf, offset);
    if (n != size) return error.ShortRead;
    return buf;
}

fn inspectNumberColumn(dec: *SafeDecoder, min: anytype, max: @TypeOf(min)) !void {
    const valuesOffset = try dec.readVarInt();
    const valuesSize = try dec.readVarInt();
    const bloomOffset = try dec.readVarInt();
    const bloomSize = try dec.readVarInt();
    std.debug.print("  min={d} max={d}\n", .{ min, max });
    printValuesAndBloom(valuesOffset, valuesSize, bloomOffset, bloomSize);
}

fn printValuesAndBloom(valuesOffset: usize, valuesSize: usize, bloomOffset: usize, bloomSize: usize) void {
    std.debug.print("  values offset={d} size={d}\n", .{ valuesOffset, valuesSize });
    std.debug.print("  bloom offset={d} size={d}\n", .{ bloomOffset, bloomSize });
}
