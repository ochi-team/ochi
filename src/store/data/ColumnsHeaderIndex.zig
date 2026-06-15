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

test "encode columns and invariantColumns" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, testEncode, .{});
}

fn testEncode(allocator: std.mem.Allocator) !void {
    const Entry = struct {
        id: u16,
        offset: u32,
    };
    const Case = struct {
        columns: []const Entry,
        invariantColumns: []const Entry,
    };

    const cases = [_]Case{
        .{
            .columns = &.{
                .{ .id = 1, .offset = 10 },
                .{ .id = 2, .offset = 20 },
            },
            .invariantColumns = &.{
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
            .invariantColumns = &.{},
        },
        .{
            .columns = &.{},
            .invariantColumns = &.{
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
        for (case.invariantColumns) |entry| {
            s.appendInvariantColumnAssumeCapacity(.{
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
        try std.testing.expectEqual(case.invariantColumns.len, decoded.invariantColumnsIDs.items.len);

        for (case.columns, 0..) |entry, i| {
            try std.testing.expectEqual(entry.id, decoded.columnID(i));
            try std.testing.expectEqual(entry.offset, decoded.columnOffset(i));
        }
        for (case.invariantColumns, 0..) |entry, i| {
            try std.testing.expectEqual(entry.id, decoded.invariantColumnID(i));
            try std.testing.expectEqual(entry.offset, decoded.invariantColumnOffset(i));
        }
    }
}
