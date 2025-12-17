const std = @import("std");

const Encoder = @import("encoding").Encoder;
const Decoder = @import("encoding").Decoder;

pub const maxDictColumnValueSize = 256;
pub const maxDictColumnValuesLen = 8;

const Self = @This();

values: std.ArrayList([]const u8),

pub fn init(allocator: std.mem.Allocator) !Self {
    const values = try std.ArrayList([]const u8).initCapacity(allocator, maxDictColumnValuesLen);
    return .{
        .values = values,
    };
}
pub fn deinit(self: *Self, allocator: std.mem.Allocator) void {
    self.values.deinit(allocator);
}

pub fn reset(self: *Self) void {
    self.values.clearRetainingCapacity();
}

pub fn set(self: *Self, v: []const u8) ?u8 {
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

pub fn bound(self: *const Self) usize {
    // 1 byte for count + varint length + string data for each value
    var size: usize = 1; // u8 for count
    for (self.values.items) |str| {
        size += Encoder.maxVarUint64Len; // varint length
        size += str.len; // string data
    }
    return size;
}

pub fn encode(self: *const Self, enc: *Encoder) void {
    enc.writeInt(u8, @intCast(self.values.items.len));
    for (self.values.items) |str| {
        enc.writeString(str);
    }
}

pub fn decode(dec: *Decoder, allocator: std.mem.Allocator) !Self {
    const len = dec.readInt(u8);
    var values = try std.ArrayList([]const u8).initCapacity(allocator, len);
    for (0..len) |_| {
        const str = dec.readString();
        values.appendAssumeCapacity(str);
    }
    return .{
        .values = values,
    };
}

test "setReturnsNullOnExceedingMaxColumnValueSize" {
    var cv = try Self.init(std.testing.allocator);
    defer cv.deinit(std.testing.allocator);

    const oversized_value = try std.testing.allocator.alloc(u8, maxDictColumnValueSize + 1);
    defer std.testing.allocator.free(oversized_value);

    const result = cv.set(oversized_value);
    try std.testing.expect(result == null);
}

test "setReturnsNullOnExceedingTotalValueSize" {
    var cv = try Self.init(std.testing.allocator);
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
    var cv = try Self.init(std.testing.allocator);
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

test "ColumnDictEncode" {
    const alloc = std.testing.allocator;

    const Case = struct {
        values: []const []const u8,
    };

    const cases = &[_]Case{
        .{
            .values = &[_][]const u8{},
        },
        .{
            .values = &[_][]const u8{"value1"},
        },
        .{
            .values = &[_][]const u8{ "value1", "value2", "value3" },
        },
        .{
            .values = &[_][]const u8{ "a", "b", "c", "d", "e", "f", "g", "h" },
        },
        .{
            .values = &[_][]const u8{ "", "non-empty", "another" },
        },
    };

    for (cases) |case| {
        var dict = try Self.init(alloc);
        defer dict.deinit(alloc);

        // Populate dict
        for (case.values) |value| {
            dict.values.appendAssumeCapacity(value);
        }

        // Encode
        const bufSize = dict.bound();
        const buf = try alloc.alloc(u8, bufSize);
        defer alloc.free(buf);

        var enc = Encoder.init(buf);
        dict.encode(&enc);

        // Decode
        var dec = Decoder.init(buf[0..enc.offset]);
        var decoded = try Self.decode(&dec, alloc);
        defer decoded.deinit(alloc);

        // Verify
        try std.testing.expectEqual(case.values.len, decoded.values.items.len);
        for (case.values, decoded.values.items) |expected, actual| {
            try std.testing.expectEqualStrings(expected, actual);
        }
    }
}
