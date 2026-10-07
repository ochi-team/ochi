const std = @import("std");
const Allocator = std.mem.Allocator;

const encoding = @import("encoding");
const Encoder = encoding.Encoder;
const Decoder = encoding.Decoder;

const ColumnDict = @import("ColumnDict.zig");

pub const ColumnType = enum(u8) {
    unknown = 0,
    string = 1,
    dict = 2,
    uint8 = 3,
    uint16 = 4,
    uint32 = 5,
    uint64 = 6,
    float64 = 7,
    ipv4 = 8,
    timestampIso8601 = 9,
    int64 = 10,
};

pub const ColumnHeader = @This();
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
        // TODO: index strings as identities (min/max) in addition to bloom filter,
        // so we can reduce bloom miss for high cardinality string columns.
        // Currently the string are indexed only using limited size dictionaries.
        // Integer values are indexed in data blocks as min/max values.
        // There is a broad use case to index string identities in a
        // similar manner in order not to rely on bloom filter.
        // Treating strings as identities allows us to index unlimited cardinality values,
        // but limited values sizes. e.g. max size of uuid is 36 bytes.
        // if we can write min/max identity values to the block,
        // not only bloom filter it can reduce bloom miss.
        // In advance it requires measuring bloom miss metric.
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

fn encodeValuesAndBloom(self: *ColumnHeader, enc: *Encoder) void {
    self.encodeValues(enc);
    self.encodeBloom(enc);
}

fn encodeValues(self: *ColumnHeader, enc: *Encoder) void {
    enc.writeVarInt(self.offset);
    enc.writeVarInt(self.size);
}
fn encodeBloom(self: *ColumnHeader, enc: *Encoder) void {
    enc.writeVarInt(self.bloomFilterOffset);
    enc.writeVarInt(self.bloomFilterSize);
}

pub fn decode(dec: *Decoder, key: []const u8, allocator: Allocator) !ColumnHeader {
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

fn decodeValuesAndBloom(self: *ColumnHeader, dec: *Decoder) void {
    self.decodeValues(dec);
    self.decodeBloom(dec);
}

fn decodeValues(self: *ColumnHeader, dec: *Decoder) void {
    self.offset = dec.readVarInt();
    self.size = dec.readVarInt();
}

fn decodeBloom(self: *ColumnHeader, dec: *Decoder) void {
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

fn valuesBound(self: *const ColumnHeader) usize {
    return Encoder.varIntBound(self.offset) + Encoder.varIntBound(self.size);
}

fn bloomBound(self: *const ColumnHeader) usize {
    return Encoder.varIntBound(self.bloomFilterOffset) + Encoder.varIntBound(self.bloomFilterSize);
}

fn valuesAndBloomBound(self: *const ColumnHeader) usize {
    return self.valuesBound() + self.bloomBound();
}
