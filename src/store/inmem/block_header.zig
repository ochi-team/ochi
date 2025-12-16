const std = @import("std");

const SID = @import("../lines.zig").SID;
const Block = @import("block.zig").Block;
const Column = @import("block.zig").Column;
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

    pub fn decode(buf: []const u8) BlockHeader {
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
            .sid = sid,
            .size = size,
            .len = len,
            .timestampsHeader = timestampsHeader,
            .columnsHeaderOffset = columnsHeaderOffset,
            .columnsHeaderSize = columnsHeaderSize,
            .columnsHeaderIndexOffset = columnsHeaderIndexOffset,
            .columnsHeaderIndexSize = columnsHeaderIndexSize,
        };
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
        allocator.destroy(self);
    }

    pub fn encodeBound(self: *const ColumnsHeader) usize {
        // [10:len][256 * headers.len:dict{dict is max here}][20:len,offset][10:celledLen][256 * celledCols]
        return 10 + ColumnDict.maxDictColumnValueSize * self.headers.len + 20 + 10 +
            self.celledColumns.len * Column.maxCelledColumnValueSize;
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
        try std.testing.expectEqualDeep(case.header, h);
    }
}
