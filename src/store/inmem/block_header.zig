const std = @import("std");

const SID = @import("../lines.zig").SID;
const Block = @import("block.zig").Block;
const Column = @import("block.zig").Column;
const Encoder = @import("encoding").Encoder;
const Decoder = @import("encoding").Decoder;
const ColumnsHeaderIndex = @import("ColumnsHeaderIndex.zig");
const ColumnIDGen = @import("ColumnIDGen.zig");
const EncodingType = @import("TimestampsEncoder.zig").EncodingType;

pub const BlockHeader = struct {
    sid: SID,
    size: u64,
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

    // 32 sid 8 size 4 len 32 timestamps = 76
    // [32:sid][8:size][4:len][33:timestamps, 32 values and 1 encoding type][40:columns header]
    pub const encodeExpectedSize = 32 + 8 + 4 + 32 + 1 + 40;

    pub fn encode(self: *BlockHeader, buf: []u8) usize {
        var enc = Encoder.init(buf);

        self.sid.encode(&enc);

        enc.writeInt(u64, self.size);
        enc.writeInt(u32, self.len);

        self.timestampsHeader.encode(&enc);

        enc.writeVarInt(self.columnsHeaderIndexOffset);
        enc.writeVarInt(self.columnsHeaderIndexSize);
        enc.writeVarInt(self.columnsHeaderOffset);
        enc.writeVarInt(self.columnsHeaderSize);

        return enc.offset;
    }

    pub fn decode(buf: []const u8) !BlockHeader {
        var decoder = Decoder.init(buf);

        const sid = try SID.decode(try decoder.readBytes(32));

        const size = try decoder.readInt(u64);
        const len = try decoder.readInt(u32);

        const timestampsHeader = try TimestampsHeader.decode(&decoder);

        return .{
            .sid = sid,
            .size = size,
            .len = len,
            .timestampsHeader = timestampsHeader,
            .columnsHeaderOffset = 0,
            .columnsHeaderSize = 0,
            .columnsHeaderIndexOffset = 0,
            .columnsHeaderIndexSize = 0,
        };
    }
};

pub const TimestampsHeader = struct {
    offset: u64,
    size: u64,
    min: u64,
    max: u64,

    encodingType: EncodingType,

    pub fn encode(self: *TimestampsHeader, enc: *Encoder) void {
        enc.writeInt(u64, self.offset);
        enc.writeInt(u64, self.size);
        enc.writeInt(u64, self.min);
        enc.writeInt(u64, self.max);
        enc.writeInt(u8, @intFromEnum(self.encodingType));
    }

    pub fn decode(decoder: *Decoder) !TimestampsHeader {
        const offset = try decoder.readInt(u64);
        const size = try decoder.readInt(u64);
        const min = try decoder.readInt(u64);
        const max = try decoder.readInt(u64);
        const encodingType = try decoder.readInt(u8);

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
        return 10 + maxDictColumnValueSize * self.headers.len + 20 + 10 +
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

pub const maxDictColumnValueSize = 256;
pub const maxDictColumnValuesLen = 8;
pub const ColumnDict = struct {
    values: std.ArrayList([]const u8),

    pub fn init(allocator: std.mem.Allocator) !ColumnDict {
        const values = try std.ArrayList([]const u8).initCapacity(allocator, maxDictColumnValuesLen);
        return .{
            .values = values,
        };
    }
    pub fn deinit(self: *ColumnDict, allocator: std.mem.Allocator) void {
        self.values.deinit(allocator);
    }

    pub fn reset(self: *ColumnDict) void {
        self.values.clearRetainingCapacity();
    }

    pub fn set(self: *ColumnDict, v: []const u8) ?u8 {
        if (v.len > maxDictColumnValueSize) return null;

        var valSize: u16 = 0;
        for (0..self.values.items.len) |i| {
            if (std.mem.eql(u8, v, self.values.items[i])) {
                return @intCast(i);
            }

            valSize += @intCast(self.values.items[i].len);
        }
        if (self.values.items.len >= maxDictColumnValuesLen) return null;
        if (valSize + v.len > maxDictColumnValueSize) return null;

        // we don't allocate more than 8 elements
        self.values.appendAssumeCapacity(v);
        return @intCast(self.values.items.len - 1);
    }

    pub fn encode(self: *ColumnDict, enc: *Encoder) void {
        enc.writeInt(u8, @intCast(self.values.items.len));
        for (self.values.items) |str| {
            enc.writeBytes(str);
        }
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

test "setReturnsNullOnExceedingMaxColumnValueSize" {
    var cv = try ColumnDict.init(std.testing.allocator);
    defer cv.deinit(std.testing.allocator);

    const oversized_value = try std.testing.allocator.alloc(u8, maxDictColumnValueSize + 1);
    defer std.testing.allocator.free(oversized_value);

    const result = cv.set(oversized_value);
    try std.testing.expect(result == null);
}

test "setReturnsNullOnExceedingTotalValueSize" {
    var cv = try ColumnDict.init(std.testing.allocator);
    defer cv.deinit(std.testing.allocator);

    const v1 = try std.testing.allocator.alloc(u8, maxDictColumnValueSize / 2);
    const v2 = try std.testing.allocator.alloc(u8, maxDictColumnValueSize / 2);
    const v3 = try std.testing.allocator.alloc(u8, maxDictColumnValueSize / 2);
    defer std.testing.allocator.free(v1);
    defer std.testing.allocator.free(v2);
    defer std.testing.allocator.free(v3);

    // fill with some data
    @memset(v1, 'a');
    const r1 = cv.set(v1);
    try std.testing.expect(r1 != null);

    @memset(v2, 'b');
    const r2 = cv.set(v2);
    try std.testing.expect(r2 != null);

    // this should fail
    @memset(v3, 'c');
    const r3 = cv.set(v3);
    try std.testing.expect(r3 == null);
}

test "setReturnsNullOnExceedingTotalValuesLen" {
    var cv = try ColumnDict.init(std.testing.allocator);
    defer cv.deinit(std.testing.allocator);

    var testValues: [8][]const u8 = undefined;
    for (0..8) |i| {
        testValues[i] = try std.testing.allocator.dupe(u8, &[_]u8{@intCast(i)});
    }
    defer {
        for (0..8) |i| {
            std.testing.allocator.free(testValues[i]);
        }
    }

    // fill with some data
    for (0..8) |i| {
        const r = cv.set(testValues[i]);
        try std.testing.expect(r != null);
    }

    const r = cv.set("1a");
    try std.testing.expect(r == null);
}
