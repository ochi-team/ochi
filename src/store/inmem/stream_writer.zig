const std = @import("std");

const encode = @import("encode.zig");

const Block = @import("block.zig").Block;
const Column = @import("block.zig").Column;
const BlockHeader = @import("block_header.zig").BlockHeader;
const ColumnsHeader = @import("block_header.zig").ColumnsHeader;
const ColumnHeader = @import("block_header.zig").ColumnHeader;
const TimestampsHeader = @import("block_header.zig").TimestampsHeader;

const maxPackedValuesSize = 8 * 1024 * 1024;

pub const Error = error{
    EmptyTimestamps,
};

pub const StreamWriter = struct {
    const tsBufferSize = 2 * 1024;
    const indexBufferSize = 2 * 1024;
    const metaIndexBufferSize = 2 * 1024;
    const messageBloomValuesSize = 2 * 1024;

    // TODO: expose metrics on len/cap relations
    timestampsBuffer: std.ArrayList(u8),
    indexBuffer: std.ArrayList(u8),
    metaIndexBuf: std.ArrayList(u8),

    messageBloomValuesBuf: std.ArrayList(u8),
    bloomValuesList: std.ArrayList(std.ArrayList(u8)),

    columnIDGen: *ColumnIDGen,
    colIdx: std.AutoHashMap(u64, u64),
    nextColI: u64,
    maxColI: u64,

    pub fn init(allocator: std.mem.Allocator, maxColI: u64) !*StreamWriter {
        var timestampsBuffer = try std.ArrayList(u8).initCapacity(allocator, tsBufferSize);
        errdefer timestampsBuffer.deinit(allocator);
        var indexBuffer = try std.ArrayList(u8).initCapacity(allocator, indexBufferSize);
        errdefer indexBuffer.deinit(allocator);
        var metaIndexBuf = try std.ArrayList(u8).initCapacity(allocator, metaIndexBufferSize);
        errdefer metaIndexBuf.deinit(allocator);

        var msgBloomValuesBuf = try std.ArrayList(u8).initCapacity(allocator, messageBloomValuesSize);
        errdefer msgBloomValuesBuf.deinit(allocator);
        var bloomValuesList = try std.ArrayList(std.ArrayList(u8)).initCapacity(allocator, maxColI);
        errdefer bloomValuesList.deinit(allocator);

        const columnIDGen = try ColumnIDGen.init(allocator);
        errdefer columnIDGen.deinit(allocator);
        const colIdx = std.AutoHashMap(u64, u64).init(allocator);

        const w = try allocator.create(StreamWriter);
        w.* = StreamWriter{
            .timestampsBuffer = timestampsBuffer,
            .indexBuffer = indexBuffer,
            .metaIndexBuf = metaIndexBuf,

            .messageBloomValuesBuf = msgBloomValuesBuf,
            .bloomValuesList = bloomValuesList,

            .columnIDGen = columnIDGen,
            .colIdx = colIdx,
            .nextColI = 0,
            .maxColI = maxColI,
        };
        return w;
    }

    pub fn deinit(self: *StreamWriter, allocator: std.mem.Allocator) void {
        self.timestampsBuffer.deinit(allocator);
        self.indexBuffer.deinit(allocator);
        self.metaIndexBuf.deinit(allocator);
        self.messageBloomValuesBuf.deinit(allocator);
        for (self.bloomValuesList.items) |*bv| {
            bv.deinit(allocator);
        }
        self.bloomValuesList.deinit(allocator);
        self.columnIDGen.deinit(allocator);
        self.colIdx.deinit();
        allocator.destroy(self);
    }

    pub fn writeBlock(
        self: *StreamWriter,
        allocator: std.mem.Allocator,
        block: *Block,
        blockHeader: *BlockHeader,
    ) !void {
        try self.writeTimestamps(allocator, &blockHeader.timestampsHeader, block.timestamps);

        const columnsHeader = try ColumnsHeader.init(allocator, block);
        defer columnsHeader.deinit(allocator);
        for (block.getColumns(), 0..) |col, i| {
            var header = columnsHeader.headers[i];
            try self.writeColumnHeader(allocator, col, &header);
        }
    }

    fn writeTimestamps(
        self: *StreamWriter,
        allocator: std.mem.Allocator,
        tsHeader: *TimestampsHeader,
        timestamps: []u64,
    ) !void {
        if (timestamps.len == 0) {
            return Error.EmptyTimestamps;
        }
        // TODO: pass static buffer instead of allocator
        const encodedTimestamps = try encode.encodeTimestamps(allocator, timestamps);
        defer allocator.free(encodedTimestamps);
        // TODO: write tsHeader data from encodedTimestamps

        tsHeader.min = timestamps[0];
        tsHeader.max = timestamps[timestamps.len - 1];
        tsHeader.offset = self.timestampsBuffer.items.len;
        tsHeader.size = encodedTimestamps.len;

        try self.timestampsBuffer.appendSlice(allocator, encodedTimestamps);
    }

    fn writeColumnHeader(self: *StreamWriter, allocator: std.mem.Allocator, col: Column, ch: *ColumnHeader) !void {
        ch.key = col.key;

        const valuesEncoder = try encode.ValuesEncoder.init(allocator);
        defer valuesEncoder.deinit();
        const valueType = try valuesEncoder.encode(col.values, &ch.dict);
        ch.type = valueType.type;
        ch.min = valueType.min;
        ch.max = valueType.max;
        const packedValues = try valuesEncoder.packValues(valuesEncoder.values.items);
        defer allocator.free(packedValues);
        std.debug.assert(packedValues.len <= maxPackedValuesSize);

        const bloomValuesBuf = try self.getBloomValuesBuf(allocator, ch.key);
        ch.size = packedValues.len;
        ch.offset = bloomValuesBuf.items.len;
        try bloomValuesBuf.appendSlice(allocator, packedValues);

        const bloomHash = if (valueType.type == .dict) &[_]u8{} else blk: {
            const tokenizer = try Tokenizer.init(allocator);
            defer tokenizer.deinit(allocator);
            var tokens = try tokenizer.tokenizeValues(allocator, col.values);
            defer tokens.deinit(allocator);
            // const hashed = encodeBloomHash(allocator, tokens);
            // break :blk hashed;
            break :blk &[_]u8{};
        };
        _ = bloomHash;
    }

    fn getBloomValuesBuf(self: *StreamWriter, allocator: std.mem.Allocator, key: []const u8) !*std.ArrayList(u8) {
        if (key.len == 0) {
            return &self.messageBloomValuesBuf;
        }

        const colID = try self.columnIDGen.genID(key);
        const maybeColI = self.colIdx.get(colID);
        if (maybeColI) |colI| {
            return &self.bloomValuesList.items[colI];
        }

        // at the moment implemented only for an in mem table, so we assume max col i is ever 1
        const colI = self.nextColI % self.maxColI;
        self.nextColI += 1;
        try self.colIdx.put(colID, colI);

        // TODO: for disk resident table implement:
        // 1. different bloom values buffer creation

        if (colI >= self.bloomValuesList.items.len) {
            try self.bloomValuesList.append(allocator, std.ArrayList(u8).empty);
        }

        return &self.bloomValuesList.items[colI];
    }
};

pub const ColumnIDGen = struct {
    keyIDs: std.StringArrayHashMap(u64),

    pub fn init(allocator: std.mem.Allocator) !*ColumnIDGen {
        const nameIDs = std.StringArrayHashMap(u64).init(allocator);
        const s = try allocator.create(ColumnIDGen);
        s.* = ColumnIDGen{
            .keyIDs = nameIDs,
        };
        return s;
    }

    pub fn deinit(self: *ColumnIDGen, allocator: std.mem.Allocator) void {
        self.keyIDs.deinit();
        allocator.destroy(self);
    }

    pub fn genID(self: *ColumnIDGen, key: []const u8) !u64 {
        const maybeID = self.keyIDs.get(key);
        if (maybeID) |id| {
            return id;
        }

        const id: u64 = @intCast(self.keyIDs.count());
        try self.keyIDs.put(key, id);
        return id;
    }
};

pub const Tokenizer = struct {
    const Bucket = struct {
        value: usize,
        overflows: std.ArrayList(usize),
    };

    buckets: [1024]Bucket,
    bitset: std.bit_set.DynamicBitSet,

    pub fn init(allocator: std.mem.Allocator) !*Tokenizer {
        const s = try allocator.create(Tokenizer);
        var buckets: [1024]Bucket = undefined;
        for (0..buckets.len) |i| {
            buckets[i] = Bucket{
                .value = 0,
                .overflows = std.ArrayList(usize).empty,
            };
        }
        s.* = Tokenizer{
            .buckets = buckets,
            .bitset = try std.bit_set.DynamicBitSet.initEmpty(allocator, buckets.len),
        };
        return s;
    }

    pub fn deinit(self: *Tokenizer, allocator: std.mem.Allocator) void {
        for (0..self.buckets.len) |i| {
            self.buckets[i].overflows.deinit(allocator);
        }
        self.bitset.deinit();
        allocator.destroy(self);
    }

    pub fn tokenizeValues(self: *Tokenizer, allocator: std.mem.Allocator, values: [][]const u8) !std.ArrayList(u64) {
        var dst: std.ArrayList(u64) = try std.ArrayList(u64).initCapacity(allocator, 2);
        errdefer dst.deinit(allocator);
        for (values, 0..) |val, i| {
            if (i > 0 and std.mem.eql(u8, val, values[i - 1])) {
                continue;
            }

            try self.appendToken(allocator, &dst, val);
        }

        return dst;
    }

    fn appendToken(self: *Tokenizer, allocator: std.mem.Allocator, dst: *std.ArrayList(u64), value: []const u8) !void {
        if (isASCII(value)) {
            try self.appendAsciiToken(allocator, dst, value);
        }
        // TODO: support unicode tokens
        // try self.appendUnicodeToken(allocator, dst, value);
        return;
    }

    fn appendAsciiToken(
        self: *Tokenizer,
        allocator: std.mem.Allocator,
        dst: *std.ArrayList(u64),
        value: []const u8,
    ) !void {
        var i: usize = 0;
        while (i < value.len) {
            var start = value.len;
            // find start
            while (i < value.len) {
                if (!isTokenChar(value[i])) {
                    i += 1;
                    continue;
                }
                start = i;
                i += 1;
                break;
            }
            // find end
            var end = value.len;
            while (i < value.len) {
                if (isTokenChar(value[i])) {
                    i += 1;
                    continue;
                }
                end = i;
                i += 1;
                break;
            }

            if (end <= start) {
                break;
            }
            const token = value[start..end];
            const maybeHash = try self.addToken(allocator, token);
            if (maybeHash) |hash| {
                try dst.append(allocator, hash);
            }
        }
    }

    fn appendUnicodeToken(
        self: *Tokenizer,
        allocator: std.mem.Allocator,
        dst: *std.ArrayList(u64),
        value: []const u8,
    ) !void {
        var str = value;
        while (str.len > 0) {
            var offset = str.len;
            var strView = std.unicode.Utf8View.init(str) catch {
                str = str[1..];
                continue;
            };
            var strIter = strView.iterator();
            while (strIter.next()) |s| {
                if (isTokenSymbol(s)) {
                    offset = strIter.i;
                    break;
                }
            }

            str = str[offset..];
            offset = str.len;

            if (std.unicode.Utf8View.init(str)) |view| {
                strIter = view.iterator();
                for (strIter.next()) |s| {
                    if (!isTokenSymbol(s)) {
                        offset = strIter.i;
                        break;
                    }
                }
            }

            if (offset == 0) {
                break;
            }

            const token = str[0..offset];
            str = str[offset..];
            const maybeHash = self.addToken(token);
            if (maybeHash) |hash| {
                try dst.append(allocator, hash);
            }
        }
    }

    fn addToken(self: *Tokenizer, allocator: std.mem.Allocator, token: []const u8) !?u64 {
        const h = std.hash.XxHash64.hash(0, token);
        const idx = h % @as(u64, self.buckets.len);

        var bucket = &self.buckets[idx];
        if (!self.bitset.isSet(idx)) {
            bucket.value = h;
            self.bitset.set(idx);
            return h;
        }

        if (bucket.value == h) {
            return null;
        }

        for (bucket.overflows.items) |v| {
            if (v == h) {
                return null;
            }
        }
        try bucket.overflows.append(allocator, h);
        return h;
    }
};

test "tokenizeValues" {
    const Case = struct {
        input: []const []const u8,
        expected: []const u64,
    };
    const cases = [_]Case{
        .{ .input = &[_][]const u8{}, .expected = &[_]u64{} },
        .{ .input = &[_][]const u8{""}, .expected = &[_]u64{} },
        .{ .input = &[_][]const u8{"foo"}, .expected = &[_]u64{0x33BF00A859C4BA3F} },
        .{ .input = &[_][]const u8{ "foo -- foo", "~~'(foo) ==^%" }, .expected = &[_]u64{0x33BF00A859C4BA3F} },
        .{
            .input = &[_][]const u8{"foo bar -- .##([baz]## %^&* Groovy"},
            .expected = &[_]u64{ 0x33BF00A859C4BA3F, 0x48A37C90AD27A659, 0x42598CF26A247404, 15498472218330607137 },
        },
        .{
            .input = &[_][]const u8{"foo bar -- .##([baz]## %^&* Groovy [[foo]] <<bar>> --- baz!!"},
            .expected = &[_]u64{ 0x33BF00A859C4BA3F, 0x48A37C90AD27A659, 0x42598CF26A247404, 15498472218330607137 },
        },
        // .{
        //     .input = &[_][]const u8{ "Юникод 999 var12.34", "34 var12 qwer" },
        //     .expected = &[_]u64{ 0xFE846FA145CEABD1, 0xD8316E61D84F6BA4, 0x6D67BA71C4E03D10, 0x5E8D522CA93563ED, 0xED80AED10E029FC8 },
        // },
    };

    for (cases) |c| {
        const allocator = std.testing.allocator;
        const tokenizer = try Tokenizer.init(allocator);
        defer tokenizer.deinit(allocator);

        var tokens = try tokenizer.tokenizeValues(allocator, @constCast(c.input));
        defer tokens.deinit(allocator);

        try std.testing.expectEqualSlices(u64, c.expected, tokens.items);
    }
}

fn isASCII(s: []const u8) bool {
    for (s) |b| {
        if (b >= 0x80) {
            return false;
        }
    }
    return true;
}

inline fn isTokenChar(c: u8) bool {
    return tokenCharTable[c] != 0;
}

const tokenCharTable = blk: {
    var a: [256]u8 = undefined;
    for (0..256) |c| {
        if (c >= 'a' and c <= 'z' or c >= 'A' and c <= 'Z' or c >= '0' and c <= '9' or c == '_') {
            a[c] = 1;
        } else {
            a[c] = 0;
        }
    }
    break :blk a;
};

fn isTokenSymbol(c: u8) bool {
    if (c < 0x80) {
        return isTokenChar(c);
    }

    return isUnicodeLetter(c) or isUnicodeNumber(c) or c == '_';
}

fn isUnicodeLetter(c: u8) bool {
    return c != 0;
}

fn isUnicodeNumber(c: u8) bool {
    return c == 0;
}
