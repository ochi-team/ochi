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
celledColumnsIDs: std.ArrayList(u16),
celledColumnOffsets: std.ArrayList(u32),

pub fn initBufferKnown(bufferIDs: []u16, bufferOffsets: []u32, i: usize) ColumnsHeaderIndex {
    std.debug.assert(bufferIDs.len == bufferOffsets.len);

    return .{
        .columnsIDs = .initBuffer(bufferIDs[0..i]),
        .columnOffsets = .initBuffer(bufferOffsets[0..i]),
        .celledColumnsIDs = .initBuffer(bufferIDs[i..]),
        .celledColumnOffsets = .initBuffer(bufferOffsets[i..]),
    };
}
// unknown means we allocate all the space for columns and
// when the decoding is done we move the rest of the space to celled
pub fn initBufferUnknown(bufferIDs: []u16, bufferOffsets: []u32) ColumnsHeaderIndex {
    std.debug.assert(bufferIDs.len == bufferOffsets.len);

    return .{
        .columnsIDs = .initBuffer(bufferIDs),
        .columnOffsets = .initBuffer(bufferOffsets),
        .celledColumnsIDs = .empty,
        .celledColumnOffsets = .empty,
    };
}

pub fn appendColumnAssumeCapacity(self: *ColumnsHeaderIndex, col: ColumnDesc) void {
    self.columnsIDs.appendAssumeCapacity(col.id);
    self.columnOffsets.appendAssumeCapacity(col.offset);
}

pub fn appendCelledColumnAssumeCapacity(self: *ColumnsHeaderIndex, col: ColumnDesc) void {
    self.celledColumnsIDs.appendAssumeCapacity(col.id);
    self.celledColumnOffsets.appendAssumeCapacity(col.offset);
}

pub fn columnID(self: *const ColumnsHeaderIndex, i: usize) u16 {
    return self.columnsIDs.items[i];
}

pub fn columnOffset(self: *const ColumnsHeaderIndex, i: usize) u32 {
    return self.columnOffsets.items[i];
}

pub fn celledColumnID(self: *const ColumnsHeaderIndex, i: usize) u16 {
    return self.celledColumnsIDs.items[i];
}

pub fn celledColumnOffset(self: *const ColumnsHeaderIndex, i: usize) u32 {
    return self.celledColumnOffsets.items[i];
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
    // move the rest of the buffer to celled columns
    self.celledColumnsIDs = .initBuffer(self.columnsIDs.allocatedSlice()[self.columnsIDs.items.len..]);
    self.celledColumnOffsets = .initBuffer(self.columnOffsets.allocatedSlice()[self.columnOffsets.items.len..]);
    decodeColumnDescs(&dec, &self.celledColumnsIDs, &self.celledColumnOffsets);
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

test "encode columns and celledColumns" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, testEncode, .{});
}

fn testEncode(allocator: std.mem.Allocator) !void {
    const Entry = struct {
        id: u16,
        offset: u32,
    };
    const Case = struct {
        columns: []const Entry,
        celledColumns: []const Entry,
    };

    const cases = [_]Case{
        .{
            .columns = &.{
                .{ .id = 1, .offset = 10 },
                .{ .id = 2, .offset = 20 },
            },
            .celledColumns = &.{
                .{ .id = 3, .offset = 30 },
                .{ .id = 4, .offset = 40 },
            },
        },
        .{
            .columns = &.{
                .{ .id = 5, .offset = 50 },
                .{ .id = 6, .offset = 60 },
                .{ .id = 7, .offset = 70 },
            },
            .celledColumns = &.{},
        },
        .{
            .columns = &.{},
            .celledColumns = &.{
                .{ .id = 8, .offset = 80 },
                .{ .id = 9, .offset = 90 },
            },
        },
    };

    for (cases) |case| {
        var columnIDs: [6]u16 = undefined;
        var columnOffsets: [6]u32 = undefined;
        var s = ColumnsHeaderIndex.initBufferKnown(
            &columnIDs,
            &columnOffsets,
            3,
        );

        for (case.columns) |entry| {
            s.appendColumnAssumeCapacity(.{
                .id = entry.id,
                .offset = entry.offset,
            });
        }
        for (case.celledColumns) |entry| {
            s.appendCelledColumnAssumeCapacity(.{
                .id = entry.id,
                .offset = entry.offset,
            });
        }

        var buf = try allocator.alloc(u8, s.encodeBound());
        defer allocator.free(buf);

        var decColumnIDs: [4]u16 = undefined;
        var decColumnOffsets: [4]u32 = undefined;
        var decoded = ColumnsHeaderIndex.initBufferUnknown(&decColumnIDs, &decColumnOffsets);
        const written = s.encode(buf);

        decoded.decode(buf[0..written]);

        try std.testing.expectEqual(case.columns.len, decoded.columnsIDs.items.len);
        try std.testing.expectEqual(case.celledColumns.len, decoded.celledColumnsIDs.items.len);

        for (case.columns, 0..) |entry, i| {
            try std.testing.expectEqual(entry.id, decoded.columnID(i));
            try std.testing.expectEqual(entry.offset, decoded.columnOffset(i));
        }
        for (case.celledColumns, 0..) |entry, i| {
            try std.testing.expectEqual(entry.id, decoded.celledColumnID(i));
            try std.testing.expectEqual(entry.offset, decoded.celledColumnOffset(i));
        }
    }
}
