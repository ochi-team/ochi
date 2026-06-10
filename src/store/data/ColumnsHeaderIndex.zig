const std = @import("std");

const Encoder = @import("encoding").Encoder;
const Decoder = @import("encoding").Decoder;

const ColumnsHeaderIndex = @This();

columnsIDs: std.ArrayList(u16),
columnOffsets: std.ArrayList(u32),
celledColumnsIDs: std.ArrayList(u16),
celledColumnOffsets: std.ArrayList(u32),

pub fn initBuffer(bufferIDs: []u16, bufferOffsets: []u32, i: usize) ColumnsHeaderIndex {
    std.debug.assert(bufferIDs.len == bufferOffsets.len);
    std.debug.assert(i <= bufferIDs.len);

    return .{
        .columnsIDs = .initBuffer(bufferIDs[0..i]),
        .columnOffsets = .initBuffer(bufferOffsets[0..i]),
        .celledColumnsIDs = .initBuffer(bufferIDs[i..]),
        .celledColumnOffsets = .initBuffer(bufferOffsets[i..]),
    };
}

pub fn encodeBound(self: *ColumnsHeaderIndex) usize {
    var res = Encoder.varIntBound(self.columnsIDs.items.len);
    for (0..self.columnsIDs.items.len) |i| {
        res += Encoder.varIntBound(self.columnsIDs.items[i]);
        res += Encoder.varIntBound(self.columnOffsets.items[i]);
    }

    res += Encoder.varIntBound(self.celledColumnsIDs.items.len);
    for (0..self.celledColumnsIDs.items.len) |i| {
        res += Encoder.varIntBound(self.celledColumnsIDs.items[i]);
        res += Encoder.varIntBound(self.celledColumnOffsets.items[i]);
    }
    return res;
}

pub fn encode(self: *ColumnsHeaderIndex, dst: []u8) usize {
    var enc = Encoder.init(dst);
    encodeColumnDescs(&enc, self.columnsIDs, self.columnOffsets);
    encodeColumnDescs(&enc, self.celledColumnsIDs, self.celledColumnOffsets);
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
    decodeColumnDescs(&dec, &self.celledColumnsIDs, &self.celledColumnOffsets);
}

fn decodeColumnDescs(dec: *Decoder, ids: *std.ArrayList(u16), offsets: *std.ArrayList(u32)) void {
    const len = dec.readVarInt();
    for (0..len) |_| {
        const columndID: u16 = @intCast(dec.readVarInt());
        const offset = dec.readVarInt();
        ids.appendAssumeCapacity(columndID);
        offsets.appendAssumeCapacity(@intCast(offset));
    }
}

test "encode columns and celledColumns" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, testEncode, .{});
}

fn testEncode(allocator: std.mem.Allocator) !void {
    var columnIDs: [4]u16 = undefined;
    var columnOffsets: [4]u32 = undefined;
    var s = ColumnsHeaderIndex.initBuffer(&columnIDs, &columnOffsets, 2);

    try s.columns.append(allocator, .{ .columndID = 1, .offset = 10 });
    try s.columns.append(allocator, .{ .columndID = 2, .offset = 20 });

    try s.celledColumns.append(allocator, .{ .columndID = 3, .offset = 30 });
    try s.celledColumns.append(allocator, .{ .columndID = 4, .offset = 40 });

    var buf = try allocator.alloc(u8, s.encodeBound());
    defer allocator.free(buf);

    var decColumnDescs: [2]ColumnsHeaderIndex.ColumnDesc = undefined;
    var decCelledColumnDescs: [2]ColumnsHeaderIndex.ColumnDesc = undefined;
    var decoded = ColumnsHeaderIndex.initBuffer(&decColumnDescs, &decCelledColumnDescs);
    const written = s.encode(buf);

    decoded.decode(buf[0..written]);

    try std.testing.expectEqual(@as(usize, 2), decoded.columns.items.len);
    try std.testing.expectEqual(@as(usize, 2), decoded.celledColumns.items.len);

    try std.testing.expectEqual(@as(u16, 4), decoded.celledColumns.items[1].columndID);
    try std.testing.expectEqual(@as(usize, 40), decoded.celledColumns.items[1].offset);
}
