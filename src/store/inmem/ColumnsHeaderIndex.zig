const std = @import("std");

const Encoder = @import("encode.zig").Encoder;

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
