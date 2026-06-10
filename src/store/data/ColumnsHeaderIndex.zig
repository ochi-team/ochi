const std = @import("std");

const Encoder = @import("encoding").Encoder;
const Decoder = @import("encoding").Decoder;

const ColumnsHeaderIndex = @This();

pub const ColumnDesc = struct {
    columndID: u16,
    offset: usize,
};

// TODO: store them in multi array list,
// currently u16 makes it as 8 byte, therefore 2k columns become as 64kb,
// if we make it multi array and store columns in a single array and make fields as
// computed property with a separator it reduces the memory to  20kb (x3 diff)
columns: std.ArrayList(ColumnDesc) = .empty,
celledColumns: std.ArrayList(ColumnDesc) = .empty,

pub fn initBuffer(columnsBuffer: []ColumnDesc, celledColumnsBuffer: []ColumnDesc) ColumnsHeaderIndex {
    return .{
        .columns = std.ArrayList(ColumnDesc).initBuffer(columnsBuffer),
        .celledColumns = std.ArrayList(ColumnDesc).initBuffer(celledColumnsBuffer),
    };
}

pub fn deinit(self: *ColumnsHeaderIndex, allocator: std.mem.Allocator) void {
    self.columns.deinit(allocator);
    self.celledColumns.deinit(allocator);
}

pub fn encodeBound(self: *ColumnsHeaderIndex) usize {
    var res = Encoder.varIntBound(self.columns.items.len);
    for (self.columns.items) |desc| {
        res += Encoder.varIntBound(desc.columndID);
        res += Encoder.varIntBound(desc.offset);
    }

    res += Encoder.varIntBound(self.celledColumns.items.len);
    for (self.celledColumns.items) |desc| {
        res += Encoder.varIntBound(desc.columndID);
        res += Encoder.varIntBound(desc.offset);
    }
    return res;
}

pub fn encode(self: *ColumnsHeaderIndex, dst: []u8) usize {
    var enc = Encoder.init(dst);
    encodeColumnDescs(&enc, self.columns);
    encodeColumnDescs(&enc, self.celledColumns);
    return enc.offset;
}

fn encodeColumnDescs(enc: *Encoder, descs: std.ArrayList(ColumnDesc)) void {
    enc.writeVarInt(descs.items.len);
    for (descs.items) |desc| {
        enc.writeVarInt(desc.columndID);
        enc.writeVarInt(desc.offset);
    }
}

pub fn decode(
    self: *ColumnsHeaderIndex,
    src: []const u8,
) void {
    var dec = Decoder.init(src);

    decodeColumnDescs(&dec, &self.columns);
    decodeColumnDescs(&dec, &self.celledColumns);
}

fn decodeColumnDescs(
    dec: *Decoder,
    descs: *std.ArrayList(ColumnDesc),
) void {
    const len = dec.readVarInt();

    for (0..len) |_| {
        const columndID: u16 = @intCast(dec.readVarInt());
        const offset = dec.readVarInt();

        descs.appendAssumeCapacity(.{
            .columndID = columndID,
            .offset = offset,
        });
    }
}

test "encode columns and celledColumns" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, testEncode, .{});
}

fn testEncode(allocator: std.mem.Allocator) !void {
    var columnDescs: [2]ColumnsHeaderIndex.ColumnDesc = undefined;
    var celledColumnDescs: [2]ColumnsHeaderIndex.ColumnDesc = undefined;
    var s = ColumnsHeaderIndex.initBuffer(&columnDescs, &celledColumnDescs);

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
