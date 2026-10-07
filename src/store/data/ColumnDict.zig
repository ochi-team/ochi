const std = @import("std");

const Encoder = @import("encoding").Encoder;
const Decoder = @import("encoding").Decoder;

pub const maxDictColumnValueSize = 256;
pub const maxDictColumnValuesLen = 8;

const ColumnDict = @This();

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

pub fn copy(self: *const ColumnDict, allocator: std.mem.Allocator) !ColumnDict {
    var values = try std.ArrayList([]const u8).initCapacity(allocator, maxDictColumnValuesLen);
    errdefer {
        for (values.items) |v| allocator.free(v);
        values.deinit(allocator);
    }
    for (self.values.items) |v| {
        values.appendAssumeCapacity(try allocator.dupe(u8, v));
    }
    return .{ .values = values };
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

pub fn bound(self: *const ColumnDict) usize {
    // 1 byte for count + varint length + string data for each value
    var size: usize = 1; // u8 for count
    for (self.values.items) |str| {
        size += Encoder.varIntBound(str.len); // varint length
        size += str.len; // string data
    }
    return size;
}

pub fn encode(self: *const ColumnDict, enc: *Encoder) void {
    enc.writeInt(u8, @intCast(self.values.items.len));
    for (self.values.items) |str| {
        enc.writeString(str);
    }
}

pub fn decode(dec: *Decoder, allocator: std.mem.Allocator) !ColumnDict {
    const len = dec.readInt(u8);
    var values = try std.ArrayList([]const u8).initCapacity(allocator, maxDictColumnValuesLen);
    for (0..len) |_| {
        const str = dec.readString();
        values.appendAssumeCapacity(str);
    }
    return .{
        .values = values,
    };
}
