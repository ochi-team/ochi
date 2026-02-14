const std = @import("std");

const Encoder = @import("encoding").Encoder;
const Decoder = @import("encoding").Decoder;

const Self = @This();

const ColumnDesc = struct {
    columndID: u16,
    offset: usize,
};

columns: std.ArrayList(ColumnDesc),
celledColumns: std.ArrayList(ColumnDesc),

pub fn init(allocator: std.mem.Allocator) !*Self {
    const s = try allocator.create(Self);
    s.* = Self{
        .columns = std.ArrayList(ColumnDesc).empty,
        .celledColumns = std.ArrayList(ColumnDesc).empty,
    };
    return s;
}

pub fn deinit(self: *Self, allocator: std.mem.Allocator) void {
    self.columns.deinit(allocator);
    self.celledColumns.deinit(allocator);
    allocator.destroy(self);
}

pub fn encodeBound(self: *Self) usize {
    // [10:cols len][20 * cols len][10: cells len][20 * cells len]
    return 10 + 20 * self.columns.items.len + 10 + 20 * self.celledColumns.items.len;
}

pub fn encode(self: *Self, dst: []u8) usize {
    var enc = Encoder.init(dst);
    encodeColumnDescs(&enc, self.columns);
    encodeColumnDescs(&enc, self.celledColumns);
    return enc.offset;
}

inline fn encodeColumnDescs(enc: *Encoder, descs: std.ArrayList(ColumnDesc)) void {
    enc.writeVarInt(descs.items.len);
    for (descs.items) |desc| {
        enc.writeVarInt(desc.columndID);
        enc.writeVarInt(desc.offset);
    }
}

pub fn decode(
    allocator: std.mem.Allocator,
    src: []const u8,
) !*Self {
    var dec = Decoder.init(src);

    const s = try Self.init(allocator);
    errdefer s.deinit(allocator);

    try decodeColumnDescs(&dec, allocator, &s.columns);
    try decodeColumnDescs(&dec, allocator, &s.celledColumns);

    return s;
}

fn decodeColumnDescs(
    dec: *Decoder,
    allocator: std.mem.Allocator,
    descs: *std.ArrayList(ColumnDesc),
) !void {
    const len = dec.readVarInt();

    try descs.ensureTotalCapacity(allocator, len);

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
    var s = try Self.init(allocator);
    defer s.deinit(allocator);

    try s.columns.append(allocator, .{ .columndID = 1, .offset = 10 });
    try s.columns.append(allocator, .{ .columndID = 2, .offset = 20 });

    try s.celledColumns.append(allocator, .{ .columndID = 3, .offset = 30 });
    try s.celledColumns.append(allocator, .{ .columndID = 4, .offset = 40 });

    var buf = try allocator.alloc(u8, s.encodeBound());
    defer allocator.free(buf);

    const written = s.encode(buf);

    const decoded = try Self.decode(allocator, buf[0..written]);
    defer decoded.deinit(allocator);

    try std.testing.expectEqual(@as(usize, 2), decoded.columns.items.len);
    try std.testing.expectEqual(@as(usize, 2), decoded.celledColumns.items.len);

    try std.testing.expectEqual(@as(u16, 4), decoded.celledColumns.items[1].columndID);
    try std.testing.expectEqual(@as(usize, 40), decoded.celledColumns.items[1].offset);
}
