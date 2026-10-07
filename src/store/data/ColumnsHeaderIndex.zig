const std = @import("std");

const Encoder = @import("encoding").Encoder;
const Decoder = @import("encoding").Decoder;

const ColumnsHeaderIndex = @This();

const ColumnDesc = struct {
    id: u16,
    // we can use u32 since it fits maxColumnsHeaderIndexSize
    offset: u32,
};

columnsIDs: std.ArrayList(u16),
columnOffsets: std.ArrayList(u32),
invariantColumnsIDs: std.ArrayList(u16),
invariantColumnOffsets: std.ArrayList(u32),

pub fn initBufferKnown(bufferIDs: []u16, bufferOffsets: []u32, i: usize) ColumnsHeaderIndex {
    std.debug.assert(bufferIDs.len == bufferOffsets.len);

    return .{
        .columnsIDs = .initBuffer(bufferIDs[0..i]),
        .columnOffsets = .initBuffer(bufferOffsets[0..i]),
        .invariantColumnsIDs = .initBuffer(bufferIDs[i..]),
        .invariantColumnOffsets = .initBuffer(bufferOffsets[i..]),
    };
}
// unknown means we allocate all the space for columns and
// when the decoding is done we move the rest of the space to invariant
pub fn initBufferUnknown(bufferIDs: []u16, bufferOffsets: []u32) ColumnsHeaderIndex {
    std.debug.assert(bufferIDs.len == bufferOffsets.len);

    return .{
        .columnsIDs = .initBuffer(bufferIDs),
        .columnOffsets = .initBuffer(bufferOffsets),
        .invariantColumnsIDs = .empty,
        .invariantColumnOffsets = .empty,
    };
}

pub fn appendColumnAssumeCapacity(self: *ColumnsHeaderIndex, col: ColumnDesc) void {
    self.columnsIDs.appendAssumeCapacity(col.id);
    self.columnOffsets.appendAssumeCapacity(col.offset);
}

pub fn appendInvariantColumnAssumeCapacity(self: *ColumnsHeaderIndex, col: ColumnDesc) void {
    self.invariantColumnsIDs.appendAssumeCapacity(col.id);
    self.invariantColumnOffsets.appendAssumeCapacity(col.offset);
}

pub fn columnID(self: *const ColumnsHeaderIndex, i: usize) u16 {
    return self.columnsIDs.items[i];
}

pub fn columnOffset(self: *const ColumnsHeaderIndex, i: usize) u32 {
    return self.columnOffsets.items[i];
}

pub fn invariantColumnID(self: *const ColumnsHeaderIndex, i: usize) u16 {
    return self.invariantColumnsIDs.items[i];
}

pub fn invariantColumnOffset(self: *const ColumnsHeaderIndex, i: usize) u32 {
    return self.invariantColumnOffsets.items[i];
}

pub fn encodeBound(self: *ColumnsHeaderIndex) usize {
    var res = Encoder.varIntBound(self.columnsIDs.items.len);
    for (0..self.columnsIDs.items.len) |i| {
        res += Encoder.varIntBound(self.columnsIDs.items[i]);
        res += Encoder.varIntBound(self.columnOffsets.items[i]);
    }

    res += Encoder.varIntBound(self.invariantColumnsIDs.items.len);
    for (0..self.invariantColumnsIDs.items.len) |i| {
        res += Encoder.varIntBound(self.invariantColumnsIDs.items[i]);
        res += Encoder.varIntBound(self.invariantColumnOffsets.items[i]);
    }
    return res;
}

pub fn encode(self: *ColumnsHeaderIndex, dst: []u8) usize {
    var enc = Encoder.init(dst);
    encodeColumnDescs(&enc, self.columnsIDs, self.columnOffsets);
    encodeColumnDescs(&enc, self.invariantColumnsIDs, self.invariantColumnOffsets);
    return enc.offset;
}

fn encodeColumnDescs(enc: *Encoder, ids: std.ArrayList(u16), offsets: std.ArrayList(u32)) void {
    std.debug.assert(ids.items.len == offsets.items.len);

    enc.writeVarInt(ids.items.len);
    for (0..ids.items.len) |i| {
        enc.writeVarInt(ids.items[i]);
        enc.writeVarInt(offsets.items[i]);
    }
}

pub fn decode(
    self: *ColumnsHeaderIndex,
    src: []const u8,
) void {
    var dec = Decoder.init(src);

    decodeColumnDescs(&dec, &self.columnsIDs, &self.columnOffsets);
    // move the rest of the buffer to invariant columns
    self.invariantColumnsIDs = .initBuffer(self.columnsIDs.allocatedSlice()[self.columnsIDs.items.len..]);
    self.invariantColumnOffsets = .initBuffer(self.columnOffsets.allocatedSlice()[self.columnOffsets.items.len..]);
    decodeColumnDescs(&dec, &self.invariantColumnsIDs, &self.invariantColumnOffsets);
}

fn decodeColumnDescs(dec: *Decoder, ids: *std.ArrayList(u16), offsets: *std.ArrayList(u32)) void {
    const len = dec.readVarInt();
    for (0..len) |_| {
        const colID: u16 = @intCast(dec.readVarInt());
        const offset = dec.readVarInt();
        ids.appendAssumeCapacity(colID);
        offsets.appendAssumeCapacity(@intCast(offset));
    }
}

const testing = std.testing;
