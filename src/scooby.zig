// Data Debugging
//
// scooby inspects files inside a flushed data table without opening the database server.
// Pass an absolute data table directory from .ochi/partitions/<date>/data/<table-id>.
//
// bash
// TABLE=~/project/.ochi/partitions/07072026/data/18C00FBC6AA0DF4A
// zig build scooby -- --data:$TABLE --header:metaindex --offset:0 --size:102
// zig build scooby -- --data:$TABLE --header:index --offset:77915 --size:24714
//
//
// Use metaindex output to find index block offset and size.
// Use index output to find columnsHeaderOffset, columnsHeaderSize,
// columnsHeaderIndexOffset, and columnsHeaderIndexSize for deeper column inspection.

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

/// Selects which physical table file and decoder scooby should use.
/// These names are intentionally the command-line values accepted by
/// --header:<name>. Each variant maps to one file inside a flushed data table:
/// metaindex, index, columnsHeaderIndex, or
/// columnsHeader
const HeaderKind = enum {
    metaindex,
    index,
    columnsHeaderIndex,
    columnsHeader,
};

/// Parsed command-line state.
const Args = struct {
    data: []const u8 = "",
    header: ?HeaderKind = null,
    /// offset and size always describe a byte window inside the selected file
    offset: usize = 0,
    size: ?usize = null,
    /// The optional indexOffset and indexSize are only used when inspecting a
    /// columnsHeader slice: they point to the companion columnsHeaderIndex slice
    /// so printed columns can be resolved back to stable column ids and names
    indexOffset: ?usize = null,
    indexSize: ?usize = null,
    // performs deeper data validation
    validateValues: bool = false,
    // amount of dict values to unpack dictionary values in the validation
    count: ?usize = null,
};

/// Entry point for the data-table inspection CLI.
///
/// The command always reads one explicit byte range from one table file. Optional
/// side files are loaded opportunistically: columnKeys is used to print
/// human-readable column names, and columnIdxs is loaded only when
/// --validate-values needs to find the values shard for a dictionary column.
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

/// Parses the zig build scooby -- ... argument format.
///
/// Options intentionally use --name:value instead of separate arguments so they
/// are easy to copy from logs and shell history. The table path is duplicated
/// because the process argument iterator owns the original storage.
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

/// Returns the bytes after prefix when an argument matches a --key:value.
///
/// No unescaping or trimming is performed. Paths and values are consumed exactly
/// as the shell passes them to the process.
fn argValue(arg: []const u8, prefix: []const u8) ?[]const u8 {
    if (!std.mem.startsWith(u8, arg, prefix)) return null;
    return arg[prefix.len..];
}

/// Prints command-line help and the expected offset workflow.
///
/// The help calls out absolute table paths because the implementation uses
/// openFileAbsolute after joining the table directory with a known file name.
/// Relative paths currently trip the Zig standard-library assertion before a
/// normal file-open error can be returned.
fn usage() void {
    std.debug.print(
        \\usage:
        \\  zig build scooby -- --data:<absolute table path> --header:metaindex --offset:<offset> --size:<size>
        \\  zig build scooby -- --data:<absolute table path> --header:index --offset:<offset> --size:<size>
        \\  zig build scooby -- --data:<absolute table path> --header:columnsHeaderIndex --offset:<offset> --size:<size>
        \\  zig build scooby -- --data:<absolute table path> --header:columnsHeader --offset:<offset> --size:<size> [--index-offset:<offset> --index-size:<size>] [--count:<rows> --validate-values]
        \\
        \\notes:
        \\  --data must point to a data table directory, for example /path/to/.ochi/partitions/<date>/data/<table-id>
        \\  for --header:index, use offset and size from the matching metaindex indexBlock record
        \\  for --header:columnsHeader, use columnsHeaderOffset/columnsHeaderSize from an index block record
        \\  add --index-offset/--index-size from columnsHeaderIndexOffset/columnsHeaderIndexSize to print column keys
        \\
    , .{});
}

/// Reads one bounded byte window from a known file inside a data table.
/// tablePath must be the absolute table directory.
/// fileName is one of the constants from filenames.zig.
/// The returned buffer is heap-owned by the caller
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

    return ColumnIDGen.decodeFile(io, allocator, path);
}

/// Maps a logical column id to the values/bloom shard number that stores it.
const ColumnIdxs = std.AutoHashMap(u16, u16);

/// Loads columnIdxs for value validation.
/// Dictionary column headers contain offsets into a values file, but the values
/// file is selected through the column shard mapping. This function decodes that
/// mapping and validates that each column id is known in columnKeys, which
/// prevents later validation output from silently using the wrong file.
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

    var dec = Decoder{ .buf = buf };
    const count = dec.readVarInt();
    try idxs.ensureTotalCapacity(@intCast(count));
    const keyItems = keys.keyIDs.keys();
    for (0..count) |_| {
        const colID = dec.readVarInt();
        const shardIdx = dec.readVarInt();
        if (colID >= keyItems.len) return error.ColumnIDOutOfRange;
        idxs.putAssumeCapacity(
            std.math.cast(u16, colID) orelse return error.ColumnIDOutOfRange,
            std.math.cast(u16, shardIdx) orelse return error.ShardIDOutOfRange,
        );
    }
    return idxs;
}

/// Decoded view of one columnsHeaderIndex record.
///
/// A block header points at both columnsHeader and columnsHeaderIndex.
/// The header contains column payloads in positional order; this index maps those
/// positions to stable column ids and byte offsets. Invariant columns are stored
/// after ordinary columns and have their own id/offset arrays.
const ColumnsIndex = struct {
    columnsIDs: std.ArrayList(u16),
    columnOffsets: std.ArrayList(u32),
    invariantColumnsIDs: std.ArrayList(u16),
    invariantColumnOffsets: std.ArrayList(u32),

    /// Creates an empty decoded columns index.
    ///
    /// The allocator parameter keeps the call site symmetric with deinit and
    /// decode, even though empty ArrayList values do not allocate.
    fn init(allocator: std.mem.Allocator) ColumnsIndex {
        _ = allocator;
        return .{
            .columnsIDs = .empty,
            .columnOffsets = .empty,
            .invariantColumnsIDs = .empty,
            .invariantColumnOffsets = .empty,
        };
    }

    /// Releases all arrays owned by this decoded index.
    fn deinit(self: *ColumnsIndex, allocator: std.mem.Allocator) void {
        self.columnsIDs.deinit(allocator);
        self.columnOffsets.deinit(allocator);
        self.invariantColumnsIDs.deinit(allocator);
        self.invariantColumnOffsets.deinit(allocator);
    }

    /// Decodes a complete columnsHeaderIndex byte slice.
    ///
    /// The format is two back-to-back arrays: ordinary columns followed by
    /// invariant columns. Each array is decoded by decodeIndexEntries.
    fn decode(allocator: std.mem.Allocator, buf: []const u8) !ColumnsIndex {
        var idx = ColumnsIndex.init(allocator);
        errdefer idx.deinit(allocator);

        var dec = Decoder{ .buf = buf };
        try decodeIndexEntries(allocator, &dec, &idx.columnsIDs, &idx.columnOffsets);
        try decodeIndexEntries(allocator, &dec, &idx.invariantColumnsIDs, &idx.invariantColumnOffsets);
        return idx;
    }
};

/// Decodes one id/offset array from columnsHeaderIndex.
///
/// The on-disk form is len followed by len pairs of varints. Ids are cast to
/// u16 because column ids are stored in that width elsewhere; offsets are cast
/// to u32 because they address bytes inside a single columns-header record.
fn decodeIndexEntries(
    allocator: std.mem.Allocator,
    dec: *Decoder,
    ids: *std.ArrayList(u16),
    offsets: *std.ArrayList(u32),
) !void {
    const len = dec.readVarInt();
    try ids.ensureTotalCapacity(allocator, len);
    try offsets.ensureTotalCapacity(allocator, len);
    for (0..len) |_| {
        const id = dec.readVarInt();
        const off = dec.readVarInt();
        ids.appendAssumeCapacity(std.math.cast(u16, id) orelse return error.ColumnIDOutOfRange);
        offsets.appendAssumeCapacity(std.math.cast(u32, off) orelse return error.ColumnOffsetOutOfRange);
    }
}

/// Prints the contents of a columnsHeaderIndex slice.
/// When column keys are available, each id is also shown as a field name.
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
        printIndexEntry("invariant column", i, id, off, keys);
    }

    std.debug.print("consumed={d} remaining=0\n", .{buf.len});
}

/// Decompresses and prints IndexBlockHeader records from metaindex.
/// Each record identifies one compressed block inside index. Its offset
/// is the byte position in index; its size is the compressed byte length
/// to pass back to scooby --header:index.
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

/// Decompresses and prints all BlockHeader records in an index block.
/// The input must be exactly one compressed index block, normally selected from
/// a metaindex record. The final consumed/remaining line is the quick
/// integrity check: remaining=0 means the decompressed block was fully parsed as
/// block headers.
fn inspectIndex(allocator: std.mem.Allocator, buf: []const u8) !void {
    std.debug.print("index bytes={d}\n", .{buf.len});
    const decompressedSize = try encoding.getFrameContentSize(buf);
    const decompressed = try allocator.alloc(u8, decompressedSize);
    defer allocator.free(decompressed);
    const dctx = try encoding.createDCtx();
    defer encoding.freeDCtx(dctx);
    const n = try encoding.decompress(dctx, decompressed, buf);
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

/// Prints one entry from a decoded columnsHeaderIndex.
/// kind is only a label (column or invariant). off is the byte offset
/// inside the matching columnsHeader slice and is useful when checking whether
/// a suspicious column starts at the expected boundary.
fn printIndexEntry(kind: []const u8, i: usize, id: u16, off: u32, keys: ?*ColumnIDGen) void {
    std.debug.print("{s}[{d}] id={d} offset={d}", .{ kind, i, id, off });
    if (keyForID(keys, id)) |key| {
        std.debug.print(" key=\"{s}\"", .{key});
    } else {
        std.debug.print(" key=<unknown>", .{});
    }
    std.debug.print("\n", .{});
}

/// Resolves a column id through columnKeys.
/// Returns null when the side file is missing or the id points beyond the
/// decoded key list
fn keyForID(keys: ?*ColumnIDGen, id: u16) ?[]const u8 {
    const gen = keys orelse return null;
    const keyItems = gen.keyIDs.keys();
    if (id >= keyItems.len) return null;
    return keyItems[id];
}

/// Prints one columnsHeader record and optionally validates dictionary values.
/// buf is the exact byte slice identified by a BlockHeader's
/// columnsHeaderOffset and columnsHeaderSize. The optional index is the
/// matching columnsHeaderIndex slice; without it, the function can still decode
/// column payloads but cannot print field names or locate values shards.
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
    var dec = Decoder{ .buf = buf };
    const headersLen = dec.readVarInt();
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
    const invariantLen = dec.readVarInt();
    std.debug.print("invariantColumns offset={d} len={d}\n", .{ invariantLenOffset, invariantLen });

    for (0..invariantLen) |i| {
        const start = dec.offset;
        const value = dec.readString();
        const key = keyForInvariantIndex(keys, index, i);
        std.debug.print("invariant[{d}] offset={d} len={d}", .{ i, start, value.len });
        if (key) |k| {
            std.debug.print(" key=\"{s}\"", .{k});
        }
        std.debug.print(" value=\"{s}\"\n", .{value});
    }

    std.debug.print("consumed={d} remaining={d}\n", .{ dec.offset, buf.len - dec.offset });
}

/// Resolves the human-readable key for ordinary column position i.
fn keyForColumnIndex(keys: ?*ColumnIDGen, index: ?*const ColumnsIndex, i: usize) ?[]const u8 {
    const idx = index orelse return null;
    if (i >= idx.columnsIDs.items.len) return null;
    return keyForID(keys, idx.columnsIDs.items[i]);
}

/// Returns the stable column id for ordinary column position i.
///
/// This is needed by value validation because the values file shard is keyed by
/// column id, not by the column's position inside a single header.
fn columnIDForColumnIndex(index: ?*const ColumnsIndex, i: usize) ?u16 {
    const idx = index orelse return null;
    if (i >= idx.columnsIDs.items.len) return null;
    return idx.columnsIDs.items[i];
}

/// Resolves the human-readable key for invariant column position i.
fn keyForInvariantIndex(keys: ?*ColumnIDGen, index: ?*const ColumnsIndex, i: usize) ?[]const u8 {
    const idx = index orelse return null;
    if (i >= idx.invariantColumnsIDs.items.len) return null;
    return keyForID(keys, idx.invariantColumnsIDs.items[i]);
}

/// Decodes and prints one ordinary column header from columnsHeader.
/// The first byte selects the column representation. String-like columns only
/// point at values and bloom ranges; dictionary columns also embed the dictionary
/// values; numeric columns embed min/max values before their ranges. When
/// validateValues is true, dictionary value ids are unpacked and compared
/// against the embedded dictionary.
fn inspectColumnHeader(
    io: std.Io,
    allocator: std.mem.Allocator,
    tablePath: []const u8,
    dec: *Decoder,
    i: usize,
    start: usize,
    key: ?[]const u8,
    colID: ?u16,
    columnIdxs: ?*ColumnIdxs,
    count: ?usize,
    validateValues: bool,
) !void {
    const typeRaw = dec.readInt(u8);
    const columnType: ColumnHeader.ColumnType = @enumFromInt(typeRaw);

    std.debug.print("column[{d}] offset={d} type={s}", .{ i, start, @tagName(columnType) });
    if (key) |k| {
        std.debug.print(" key=\"{s}\"", .{k});
    }
    std.debug.print("\n", .{});

    switch (columnType) {
        .string, .unknown => {
            const valuesOffset = dec.readVarInt();
            const valuesSize = dec.readVarInt();
            const bloomOffset = dec.readVarInt();
            const bloomSize = dec.readVarInt();
            printValuesAndBloom(valuesOffset, valuesSize, bloomOffset, bloomSize);
        },
        .dict => {
            const dictValues = try inspectDict(allocator, dec);
            defer allocator.free(dictValues);
            const valuesOffset = dec.readVarInt();
            const valuesSize = dec.readVarInt();
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
            const min = dec.readInt(u8);
            const max = dec.readInt(u8);
            try inspectNumberColumn(dec, min, max);
        },
        .uint16 => {
            const min = dec.readInt(u16);
            const max = dec.readInt(u16);
            try inspectNumberColumn(dec, min, max);
        },
        .uint32, .ipv4 => {
            const min = dec.readInt(u32);
            const max = dec.readInt(u32);
            try inspectNumberColumn(dec, min, max);
        },
        .uint64, .int64, .float64, .timestampIso8601 => {
            const min = dec.readInt(u64);
            const max = dec.readInt(u64);
            try inspectNumberColumn(dec, min, max);
        },
    }
}

/// Decodes the inline dictionary portion of a dictionary column header.
/// The returned slice array is owned by the caller, but each string inside it is
/// borrowed from the inspected columnsHeader buffer. The warnings compare both
/// dictionary cardinality and string sizes against ColumnDict limits so damaged
/// dictionaries stand out in the output.
fn inspectDict(allocator: std.mem.Allocator, dec: *Decoder) ![][]const u8 {
    const len = dec.readInt(u8);
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
        const dictValue = dec.readString();
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

/// Reads and validates packed dictionary value ids for one dictionary column.
/// This is an optional deeper check enabled by --validate-values. It requires
/// --count:<rows> because the unpacker needs the expected row count to rebuild
/// one value per row. The output summarizes empty ids, ids outside the dictionary,
/// and per-id counts for the valid range plus a few out-of-range ids.
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

/// Reads the values byte range referenced by a column header.
///
/// The columnsHeader record stores only offset and size; selecting the
/// physical file requires column identity. The message column, represented by an
/// empty key, lives in messageValues. Named columns live in sharded
/// values<N> files selected through columnIdxs.
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

/// Prints numeric min/max metadata and the shared values/bloom ranges.
///
/// The caller has already read min and max at the correct integer width.
/// Everything after those bounds uses the same varint offset/size layout as
/// string columns.
fn inspectNumberColumn(dec: *Decoder, min: anytype, max: @TypeOf(min)) !void {
    const valuesOffset = dec.readVarInt();
    const valuesSize = dec.readVarInt();
    const bloomOffset = dec.readVarInt();
    const bloomSize = dec.readVarInt();
    std.debug.print("  min={d} max={d}\n", .{ min, max });
    printValuesAndBloom(valuesOffset, valuesSize, bloomOffset, bloomSize);
}

/// Prints the two external ranges common to most column encodings.
///
/// valuesOffset/valuesSize point into a values file. bloomOffset and
/// bloomSize point into the matching bloom/tokens file and are printed even
/// when the current investigation only cares about values, because mismatched
/// ranges are often visible together.
fn printValuesAndBloom(valuesOffset: usize, valuesSize: usize, bloomOffset: usize, bloomSize: usize) void {
    std.debug.print("  values offset={d} size={d}\n", .{ valuesOffset, valuesSize });
    std.debug.print("  bloom offset={d} size={d}\n", .{ bloomOffset, bloomSize });
}
