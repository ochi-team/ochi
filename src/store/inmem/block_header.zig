const std = @import("std");

const SID = @import("../lines.zig").SID;
const Block = @import("Block.zig");
const Column = @import("Column.zig");
const Encoder = @import("encoding").Encoder;
const Decoder = @import("encoding").Decoder;
const ColumnsHeaderIndex = @import("ColumnsHeaderIndex.zig");
const ColumnIDGen = @import("ColumnIDGen.zig");
const EncodingType = @import("TimestampsEncoder.zig").EncodingType;
const ColumnDict = @import("ColumnDict.zig");

pub const BlockHeader = struct {
    sid: SID,
    size: u32,
    len: u32,
    timestampsHeader: TimestampsHeader,

    columnsHeaderOffset: usize,
    columnsHeaderSize: usize,
    columnsHeaderIndexOffset: usize,
    columnsHeaderIndexSize: usize,

    pub fn init(block: *Block, sid: SID) BlockHeader {
        return .{
            .sid = sid,
            .size = block.size(),
            .len = @intCast(block.len()),
            .timestampsHeader = .{
                .offset = 0,
                .size = 0,
                .min = 0,
                .max = 0,
                .encodingType = EncodingType.Undefined,
            },
            .columnsHeaderOffset = 0,
            .columnsHeaderSize = 0,
            .columnsHeaderIndexOffset = 0,
            .columnsHeaderIndexSize = 0,
        };
    }

    // [32:sid][4:size][4:len][33:timestamps, 32 values and 1 encoding type][40:columns header]
    pub const encodeExpectedSize = 32 + 4 + 4 + 32 + 1 + 40;

    pub fn encode(self: *const BlockHeader, buf: []u8) usize {
        var enc = Encoder.init(buf);

        self.sid.encode(&enc);

        enc.writeInt(u32, self.size);
        enc.writeInt(u32, self.len);

        self.timestampsHeader.encode(&enc);

        enc.writeVarInt(self.columnsHeaderIndexOffset);
        enc.writeVarInt(self.columnsHeaderIndexSize);
        enc.writeVarInt(self.columnsHeaderOffset);
        enc.writeVarInt(self.columnsHeaderSize);

        return enc.offset;
    }

    pub fn decode(buf: []const u8) struct { header: BlockHeader, offset: usize } {
        var decoder = Decoder.init(buf);

        const sid = SID.decode(decoder.readBytes(32));

        const size = decoder.readInt(u32);
        const len = decoder.readInt(u32);

        const timestampsHeader = TimestampsHeader.decode(&decoder);

        const columnsHeaderIndexOffset = decoder.readVarInt();
        const columnsHeaderIndexSize = decoder.readVarInt();
        const columnsHeaderOffset = decoder.readVarInt();
        const columnsHeaderSize = decoder.readVarInt();

        return .{
            .header = .{
                .sid = sid,
                .size = size,
                .len = len,
                .timestampsHeader = timestampsHeader,
                .columnsHeaderOffset = columnsHeaderOffset,
                .columnsHeaderSize = columnsHeaderSize,
                .columnsHeaderIndexOffset = columnsHeaderIndexOffset,
                .columnsHeaderIndexSize = columnsHeaderIndexSize,
            },
            .offset = decoder.offset,
        };
    }

    pub fn decodeFew(
        allocator: std.mem.Allocator,
        dst: *std.ArrayList(BlockHeader),
        src: []const u8,
    ) !void {
        const dst_len = dst.items.len;
        var buf = src;

        while (buf.len > 0) {
            const res = BlockHeader.decode(buf);
            try dst.append(allocator, res.header);
            buf = buf[res.offset..];
        }

        validateBlockHeaders(dst.items[dst_len..]);
    }

    pub fn validateBlockHeaders(bhs: []const BlockHeader) void {
        if (bhs.len < 2) return;

        for (1..bhs.len) |i| {
            const curr = &bhs[i];
            const prev = &bhs[i - 1];

            std.debug.assert(!curr.sid.lessThan(&prev.sid));

            if (!curr.sid.eql(&prev.sid)) {
                continue;
            }

            const th_curr = curr.timestampsHeader;
            const th_prev = prev.timestampsHeader;

            std.debug.assert(th_curr.min >= th_prev.min);
        }
    }
};

pub const TimestampsHeader = struct {
    offset: u64,
    size: u64,
    min: u64,
    max: u64,

    encodingType: EncodingType,

    pub fn encode(self: *const TimestampsHeader, enc: *Encoder) void {
        enc.writeInt(u64, self.offset);
        enc.writeInt(u64, self.size);
        enc.writeInt(u64, self.min);
        enc.writeInt(u64, self.max);
        enc.writeInt(u8, @intFromEnum(self.encodingType));
    }

    pub fn decode(decoder: *Decoder) TimestampsHeader {
        const offset = decoder.readInt(u64);
        const size = decoder.readInt(u64);
        const min = decoder.readInt(u64);
        const max = decoder.readInt(u64);
        const encodingType = decoder.readInt(u8);

        return .{
            .offset = offset,
            .size = size,
            .min = min,
            .max = max,
            .encodingType = @enumFromInt(encodingType),
        };
    }
};

pub const ColumnsHeader = struct {
    headers: []ColumnHeader,
    celledColumns: []Column,
    /// When true, deinit owns and frees celledColumns and each column's values (decode path).
    /// TODO: find a workaround for clear ownership instead of a flag
    owns_celled_columns: bool = false,

    pub fn init(allocator: std.mem.Allocator, block: *Block) !*ColumnsHeader {
        const cols = block.getColumns();
        const headers = try allocator.alloc(ColumnHeader, cols.len);
        errdefer allocator.free(headers);
        var inited: u16 = 0;
        errdefer {
            for (0..inited) |i| {
                headers[i].dict.deinit(allocator);
            }
        }
        {
            for (0..headers.len) |i| {
                headers[i].dict = try ColumnDict.init(allocator);
                inited += 1;
            }
        }

        const celledCols = block.getCelledColumns();

        const ch = try allocator.create(ColumnsHeader);
        ch.* = .{
            .headers = headers,
            .celledColumns = celledCols,
        };

        return ch;
    }

    pub fn deinit(self: *ColumnsHeader, allocator: std.mem.Allocator) void {
        for (0..self.headers.len) |i| {
            self.headers[i].dict.deinit(allocator);
        }
        allocator.free(self.headers);
        if (self.owns_celled_columns) {
            for (self.celledColumns) |*column| allocator.free(column.values);
            allocator.free(self.celledColumns);
        }
        allocator.destroy(self);
    }

    // [10:headers len][headers][10:columns len][columns]
    pub fn encodeBound(self: *const ColumnsHeader) usize {
        var size: usize = 0;

        // Headers length varint
        size += Encoder.maxVarUint64Len;

        // Sum of all header bounds
        for (self.headers) |*header| {
            size += header.encodeBound();
        }

        // Celled columns length varint
        size += Encoder.maxVarUint64Len;

        // Sum of all celled column bounds
        for (self.celledColumns) |*col| {
            size += col.celledBound(false);
        }

        return size;
    }
    pub fn encode(
        self: *ColumnsHeader,
        dst: []u8,
        cshIdx: *ColumnsHeaderIndex,
        columnIDGen: *ColumnIDGen,
    ) usize {
        var enc = Encoder.init(dst);
        enc.writeVarInt(@intCast(self.headers.len));
        var offset = enc.offset;

        for (self.headers) |*header| {
            const colID = columnIDGen.keyIDs.get(header.key).?;
            header.encode(&enc);
            cshIdx.columns.appendAssumeCapacity(.{
                .columndID = colID,
                .offset = offset,
            });
            offset = enc.offset;
        }

        enc.writeVarInt(@intCast(self.celledColumns.len));
        offset = enc.offset;

        for (self.celledColumns) |*celledCol| {
            const colID = columnIDGen.genIDAssumeCapacity(celledCol.key);
            celledCol.encodeAsCelled(&enc, false);
            cshIdx.celledColumns.appendAssumeCapacity(.{
                .columndID = colID,
                .offset = offset,
            });
            offset = enc.offset;
        }

        return enc.offset;
    }

    pub fn decode(
        allocator: std.mem.Allocator,
        buf: []const u8,
        cshIdx: *const ColumnsHeaderIndex,
        columnIDGen: *const ColumnIDGen,
    ) !*ColumnsHeader {
        var dec = Decoder.init(buf);

        const headersLen = dec.readVarInt();
        const headers = try allocator.alloc(ColumnHeader, headersLen);
        var headersDecoded: usize = 0;
        errdefer {
            for (headers[0..headersDecoded]) |*header| header.dict.deinit(allocator);
            allocator.free(headers);
        }

        for (0..headersLen) |i| {
            const colID = cshIdx.columns.items[i].columndID;
            const key = columnIDGen.keyIDs.keys()[colID];
            headers[i] = try ColumnHeader.decode(&dec, key, allocator);
            headersDecoded += 1;
        }

        const celledLen = dec.readVarInt();
        const celledColumns = try allocator.alloc(Column, celledLen);
        var celledDecoded: usize = 0;
        errdefer {
            for (celledColumns[0..celledDecoded]) |*column| allocator.free(column.values);
            allocator.free(celledColumns);
        }

        for (0..celledLen) |i| {
            const colID = cshIdx.celledColumns.items[i].columndID;
            celledColumns[i] = try Column.decodeAsCelled(&dec, allocator, false);
            celledColumns[i].key = columnIDGen.keyIDs.keys()[colID];
            celledDecoded = i + 1;
        }

        const ch = try allocator.create(ColumnsHeader);
        ch.* = .{
            .headers = headers,
            .celledColumns = celledColumns,
            .owns_celled_columns = true,
        };

        return ch;
    }
};

pub const ColumnType = enum(u8) {
    unknown = 0,
    string = 1,
    dict = 2,
    uint8 = 3,
    uint16 = 4,
    uint32 = 5,
    uint64 = 6,
    int64 = 10,
    float64 = 7,
    ipv4 = 8,
    timestampIso8601 = 9,
};

pub const ColumnHeader = struct {
    key: []const u8,
    dict: ColumnDict,
    type: ColumnType,
    min: u64,
    max: u64,
    size: usize,
    offset: usize,
    bloomFilterSize: usize,
    bloomFilterOffset: usize,

    pub fn encode(self: *ColumnHeader, enc: *Encoder) void {
        enc.writeInt(u8, @intFromEnum(self.type));

        switch (self.type) {
            .string => self.encodeValuesAndBloom(enc),
            .dict => {
                self.dict.encode(enc);
                self.encodeValues(enc);
            },
            .uint8 => {
                enc.writeInt(u8, @intCast(self.min));
                enc.writeInt(u8, @intCast(self.max));
                self.encodeValuesAndBloom(enc);
            },
            .uint16 => {
                enc.writeInt(u16, @intCast(self.min));
                enc.writeInt(u16, @intCast(self.max));
                self.encodeValuesAndBloom(enc);
            },
            .uint32 => {
                enc.writeInt(u32, @intCast(self.min));
                enc.writeInt(u32, @intCast(self.max));
                self.encodeValuesAndBloom(enc);
            },
            .uint64 => {
                enc.writeInt(u64, self.min);
                enc.writeInt(u64, self.max);
                self.encodeValuesAndBloom(enc);
            },
            .int64 => {
                enc.writeInt(u64, self.min);
                enc.writeInt(u64, self.max);
                self.encodeValuesAndBloom(enc);
            },
            .float64 => {
                enc.writeInt(u64, self.min);
                enc.writeInt(u64, self.max);
                self.encodeValuesAndBloom(enc);
            },
            .ipv4 => {
                enc.writeInt(u32, @intCast(self.min));
                enc.writeInt(u32, @intCast(self.max));
                self.encodeValuesAndBloom(enc);
            },
            .timestampIso8601 => {
                enc.writeInt(u64, self.min);
                enc.writeInt(u64, self.max);
                self.encodeValuesAndBloom(enc);
            },
            .unknown => self.encodeValuesAndBloom(enc),
        }
    }

    inline fn encodeValuesAndBloom(self: *ColumnHeader, enc: *Encoder) void {
        self.encodeValues(enc);
        self.encodeBloom(enc);
    }

    inline fn encodeValues(self: *ColumnHeader, enc: *Encoder) void {
        enc.writeVarInt(self.offset);
        enc.writeVarInt(self.size);
    }
    inline fn encodeBloom(self: *ColumnHeader, enc: *Encoder) void {
        enc.writeVarInt(self.bloomFilterOffset);
        enc.writeVarInt(self.bloomFilterSize);
    }

    pub fn decode(dec: *Decoder, key: []const u8, allocator: std.mem.Allocator) !ColumnHeader {
        const columnType: ColumnType = @enumFromInt(dec.readInt(u8));

        var header = ColumnHeader{
            .key = key,
            .dict = ColumnDict{ .values = std.ArrayList([]const u8).empty },
            .type = columnType,
            .min = 0,
            .max = 0,
            .size = 0,
            .offset = 0,
            .bloomFilterSize = 0,
            .bloomFilterOffset = 0,
        };

        switch (columnType) {
            .string => header.decodeValuesAndBloom(dec),
            .dict => {
                header.dict = try ColumnDict.decode(dec, allocator);
                header.decodeValues(dec);
            },
            .uint8 => {
                header.min = dec.readInt(u8);
                header.max = dec.readInt(u8);
                header.decodeValuesAndBloom(dec);
            },
            .uint16 => {
                header.min = dec.readInt(u16);
                header.max = dec.readInt(u16);
                header.decodeValuesAndBloom(dec);
            },
            .uint32 => {
                header.min = dec.readInt(u32);
                header.max = dec.readInt(u32);
                header.decodeValuesAndBloom(dec);
            },
            .uint64 => {
                header.min = dec.readInt(u64);
                header.max = dec.readInt(u64);
                header.decodeValuesAndBloom(dec);
            },
            .int64 => {
                header.min = dec.readInt(u64);
                header.max = dec.readInt(u64);
                header.decodeValuesAndBloom(dec);
            },
            .float64 => {
                header.min = dec.readInt(u64);
                header.max = dec.readInt(u64);
                header.decodeValuesAndBloom(dec);
            },
            .ipv4 => {
                header.min = dec.readInt(u32);
                header.max = dec.readInt(u32);
                header.decodeValuesAndBloom(dec);
            },
            .timestampIso8601 => {
                header.min = dec.readInt(u64);
                header.max = dec.readInt(u64);
                header.decodeValuesAndBloom(dec);
            },
            .unknown => header.decodeValuesAndBloom(dec),
        }

        return header;
    }

    inline fn decodeValuesAndBloom(self: *ColumnHeader, dec: *Decoder) void {
        self.decodeValues(dec);
        self.decodeBloom(dec);
    }

    inline fn decodeValues(self: *ColumnHeader, dec: *Decoder) void {
        self.offset = dec.readVarInt();
        self.size = dec.readVarInt();
    }

    inline fn decodeBloom(self: *ColumnHeader, dec: *Decoder) void {
        self.bloomFilterOffset = dec.readVarInt();
        self.bloomFilterSize = dec.readVarInt();
    }

    pub fn encodeBound(self: *const ColumnHeader) usize {
        var size: usize = 1; // type byte

        switch (self.type) {
            .string => size += self.valuesAndBloomBound(),
            .dict => {
                size += self.dict.bound();
                size += self.valuesBound();
            },
            .uint8 => size += 1 + 1 + self.valuesAndBloomBound(), // min + max + values and bloom
            .uint16 => size += 2 + 2 + self.valuesAndBloomBound(),
            .uint32 => size += 4 + 4 + self.valuesAndBloomBound(),
            .uint64 => size += 8 + 8 + self.valuesAndBloomBound(),
            .int64 => size += 8 + 8 + self.valuesAndBloomBound(),
            .float64 => size += 8 + 8 + self.valuesAndBloomBound(),
            .ipv4 => size += 4 + 4 + self.valuesAndBloomBound(),
            .timestampIso8601 => size += 8 + 8 + self.valuesAndBloomBound(),
            .unknown => size += self.valuesAndBloomBound(),
        }

        return size;
    }

    inline fn valuesBound(self: *const ColumnHeader) usize {
        _ = self;
        // TODO: Use Encoder.varIntSize(self.offset) + Encoder.varIntSize(self.size) for precise calculation
        return Encoder.maxVarUint64Len * 2;
    }

    inline fn bloomBound(self: *const ColumnHeader) usize {
        _ = self;
        // TODO: Use Encoder.varIntSize() for precise calculation
        return Encoder.maxVarUint64Len * 2;
    }

    inline fn valuesAndBloomBound(self: *const ColumnHeader) usize {
        return self.valuesBound() + self.bloomBound();
    }
};

test "BlockHeaderEncode" {
    const Case = struct {
        header: BlockHeader,
        expectedLen: usize,
    };

    const cases = &[_]Case{
        .{
            .header = .{
                .sid = .{
                    .tenantID = "tenant",
                    .id = 42,
                },
                .size = 1234,
                .len = 123,
                .timestampsHeader = .{
                    .offset = 1,
                    .size = 2,
                    .min = 50,
                    .max = 100,
                    .encodingType = EncodingType.ZDeltapack,
                },
                .columnsHeaderOffset = 10,
                .columnsHeaderSize = 20,
                .columnsHeaderIndexOffset = 30,
                .columnsHeaderIndexSize = 40,
            },
            .expectedLen = 77,
        },
        .{
            .header = std.mem.zeroInit(BlockHeader, .{}),
            .expectedLen = 77,
        },
        .{
            .header = .{
                .sid = .{
                    .tenantID = "tenant",
                    .id = std.math.maxInt(u128),
                },
                .size = std.math.maxInt(u32),
                .len = std.math.maxInt(u32),
                .timestampsHeader = .{
                    .offset = std.math.maxInt(u64),
                    .size = std.math.maxInt(u64),
                    .min = std.math.maxInt(u64),
                    .max = std.math.maxInt(u64),
                    .encodingType = EncodingType.ZDeltapack,
                },
                .columnsHeaderOffset = std.math.maxInt(usize),
                .columnsHeaderSize = std.math.maxInt(usize),
                .columnsHeaderIndexOffset = std.math.maxInt(usize),
                .columnsHeaderIndexSize = std.math.maxInt(usize),
            },
            .expectedLen = BlockHeader.encodeExpectedSize,
        },
    };

    for (cases) |case| {
        var encodeBuf: [BlockHeader.encodeExpectedSize]u8 = undefined;
        const offset = case.header.encode(&encodeBuf);
        try std.testing.expectEqual(case.expectedLen, offset);

        const h = BlockHeader.decode(encodeBuf[0..offset]);
        try std.testing.expectEqualDeep(case.header, h.header);
    }
}

test "ColumnsHeaderEncode" {
    const alloc = std.testing.allocator;

    // Create ColumnIDGen and populate it with test keys
    const columnIDGen = try ColumnIDGen.init(alloc);
    defer columnIDGen.deinit(alloc);
    try columnIDGen.keyIDs.ensureUnusedCapacity(5);
    _ = columnIDGen.genIDAssumeCapacity("col_string");
    _ = columnIDGen.genIDAssumeCapacity("col_dict");
    _ = columnIDGen.genIDAssumeCapacity("col_uint32");
    _ = columnIDGen.genIDAssumeCapacity("celled_col1");
    _ = columnIDGen.genIDAssumeCapacity("celled_col2");

    // Create test ColumnsHeader
    const headers = try alloc.alloc(ColumnHeader, 3);
    defer alloc.free(headers);

    // String column header (non-dict type, so dict is empty)
    headers[0] = .{
        .key = "col_string",
        .dict = ColumnDict{ .values = std.ArrayList([]const u8).empty },
        .type = .string,
        .min = 0,
        .max = 0,
        .size = 100,
        .offset = 1000,
        .bloomFilterSize = 50,
        .bloomFilterOffset = 2000,
    };

    // Dict column header (dict type, so dict has capacity)
    headers[1] = .{
        .key = "col_dict",
        .dict = try ColumnDict.init(alloc),
        .type = .dict,
        .min = 0,
        .max = 0,
        .size = 200,
        .offset = 1100,
        .bloomFilterSize = 0,
        .bloomFilterOffset = 0,
    };
    headers[1].dict.values.appendAssumeCapacity("value1");
    headers[1].dict.values.appendAssumeCapacity("value2");
    defer headers[1].dict.deinit(alloc);

    // Uint32 column header (non-dict type, so dict is empty)
    headers[2] = .{
        .key = "col_uint32",
        .dict = ColumnDict{ .values = std.ArrayList([]const u8).empty },
        .type = .uint32,
        .min = 10,
        .max = 1000,
        .size = 150,
        .offset = 1200,
        .bloomFilterSize = 60,
        .bloomFilterOffset = 2100,
    };

    // Create test celled columns
    const celledColumns = try alloc.alloc(Column, 2);
    defer alloc.free(celledColumns);

    const celledValues1 = try alloc.alloc([]const u8, 1);
    celledValues1[0] = "constant_value_1";
    defer alloc.free(celledValues1);

    const celledValues2 = try alloc.alloc([]const u8, 1);
    celledValues2[0] = "constant_value_2";
    defer alloc.free(celledValues2);

    celledColumns[0] = .{
        .key = "celled_col1",
        .values = celledValues1,
    };

    celledColumns[1] = .{
        .key = "celled_col2",
        .values = celledValues2,
    };

    var columnsHeader = ColumnsHeader{
        .headers = headers,
        .celledColumns = celledColumns,
    };

    // Create ColumnsHeaderIndex
    const cshIdx = try ColumnsHeaderIndex.init(alloc);
    defer cshIdx.deinit(alloc);
    try cshIdx.columns.ensureTotalCapacity(alloc, 3);
    try cshIdx.celledColumns.ensureTotalCapacity(alloc, 2);

    // Encode
    const encodeBound = columnsHeader.encodeBound();
    const encodeBuf = try alloc.alloc(u8, encodeBound);
    defer alloc.free(encodeBuf);

    const encodedSize = columnsHeader.encode(encodeBuf, cshIdx, columnIDGen);

    // Decode
    const decodedHeader = try ColumnsHeader.decode(
        alloc,
        encodeBuf[0..encodedSize],
        cshIdx,
        columnIDGen,
    );
    defer decodedHeader.deinit(alloc);

    // Verify using deep comparison
    try std.testing.expectEqual(headers.len, decodedHeader.headers.len);
    for (headers, decodedHeader.headers) |orig, decoded| {
        try std.testing.expectEqualDeep(orig, decoded);
    }

    try std.testing.expectEqual(celledColumns.len, decodedHeader.celledColumns.len);
    for (celledColumns, decodedHeader.celledColumns) |orig, decoded| {
        try std.testing.expectEqualDeep(orig, decoded);
    }
}

test "ColumnHeaderEncode" {
    const alloc = std.testing.allocator;

    const Case = struct {
        header: ColumnHeader,
        description: []const u8,

        fn makeDict(allocator: std.mem.Allocator, values: []const []const u8) !ColumnDict {
            if (values.len == 0) {
                // For empty dict (non-dict column types), match what decode produces
                return ColumnDict{ .values = std.ArrayList([]const u8).empty };
            }
            var dict = try ColumnDict.init(allocator);
            for (values) |val| {
                dict.values.appendAssumeCapacity(val);
            }
            return dict;
        }
    };

    var cases = [_]Case{
        .{
            .header = .{
                .key = "string_col",
                .dict = try Case.makeDict(alloc, &[_][]const u8{}),
                .type = .string,
                .min = 0,
                .max = 0,
                .size = 100,
                .offset = 1000,
                .bloomFilterSize = 50,
                .bloomFilterOffset = 2000,
            },
            .description = "string type",
        },
        .{
            .header = .{
                .key = "dict_col",
                .dict = try Case.makeDict(alloc, &[_][]const u8{ "value1", "value2", "value3" }),
                .type = .dict,
                .min = 0,
                .max = 0,
                .size = 200,
                .offset = 1100,
                .bloomFilterSize = 0,
                .bloomFilterOffset = 0,
            },
            .description = "dict type with values",
        },
        .{
            .header = .{
                .key = "uint8_col",
                .dict = try Case.makeDict(alloc, &[_][]const u8{}),
                .type = .uint8,
                .min = 0,
                .max = 255,
                .size = 150,
                .offset = 1200,
                .bloomFilterSize = 60,
                .bloomFilterOffset = 2100,
            },
            .description = "uint8 type",
        },
        .{
            .header = .{
                .key = "uint16_col",
                .dict = try Case.makeDict(alloc, &[_][]const u8{}),
                .type = .uint16,
                .min = 0,
                .max = 65535,
                .size = 200,
                .offset = 1300,
                .bloomFilterSize = 70,
                .bloomFilterOffset = 2200,
            },
            .description = "uint16 type",
        },
        .{
            .header = .{
                .key = "uint32_col",
                .dict = try Case.makeDict(alloc, &[_][]const u8{}),
                .type = .uint32,
                .min = 10,
                .max = 1000,
                .size = 250,
                .offset = 1400,
                .bloomFilterSize = 80,
                .bloomFilterOffset = 2300,
            },
            .description = "uint32 type",
        },
        .{
            .header = .{
                .key = "uint64_col",
                .dict = try Case.makeDict(alloc, &[_][]const u8{}),
                .type = .uint64,
                .min = 100,
                .max = 10000,
                .size = 300,
                .offset = 1500,
                .bloomFilterSize = 90,
                .bloomFilterOffset = 2400,
            },
            .description = "uint64 type",
        },
        .{
            .header = .{
                .key = "int64_col",
                .dict = try Case.makeDict(alloc, &[_][]const u8{}),
                .type = .int64,
                .min = 0,
                .max = 5000,
                .size = 350,
                .offset = 1600,
                .bloomFilterSize = 100,
                .bloomFilterOffset = 2500,
            },
            .description = "int64 type",
        },
        .{
            .header = .{
                .key = "float64_col",
                .dict = try Case.makeDict(alloc, &[_][]const u8{}),
                .type = .float64,
                .min = 0,
                .max = 1000,
                .size = 400,
                .offset = 1700,
                .bloomFilterSize = 110,
                .bloomFilterOffset = 2600,
            },
            .description = "float64 type",
        },
        .{
            .header = .{
                .key = "ipv4_col",
                .dict = try Case.makeDict(alloc, &[_][]const u8{}),
                .type = .ipv4,
                .min = 0,
                .max = 4294967295,
                .size = 450,
                .offset = 1800,
                .bloomFilterSize = 120,
                .bloomFilterOffset = 2700,
            },
            .description = "ipv4 type",
        },
        .{
            .header = .{
                .key = "timestamp_col",
                .dict = try Case.makeDict(alloc, &[_][]const u8{}),
                .type = .timestampIso8601,
                .min = 1000000,
                .max = 2000000,
                .size = 500,
                .offset = 1900,
                .bloomFilterSize = 130,
                .bloomFilterOffset = 2800,
            },
            .description = "timestamp type",
        },
    };
    defer {
        for (&cases) |*case| {
            case.header.dict.deinit(alloc);
        }
    }

    for (cases) |case| {
        // Encode
        var buf: [1024]u8 = undefined;
        var enc = Encoder.init(&buf);
        var header = case.header;
        header.encode(&enc);

        // Decode
        var dec = Decoder.init(buf[0..enc.offset]);
        var decoded = try ColumnHeader.decode(&dec, case.header.key, alloc);
        defer decoded.dict.deinit(alloc);

        // Verify using deep comparison
        try std.testing.expectEqualDeep(case.header, decoded);
    }
}
