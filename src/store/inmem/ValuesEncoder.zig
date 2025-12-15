const std = @import("std");

const zeit = @import("zeit");

const encoding = @import("encoding");
const Encoder = encoding.Encoder;

const ColumnDict = @import("ColumnDict.zig");
const ColumnType = @import("block_header.zig").ColumnType;

pub const EncodeValueType = struct {
    type: ColumnType,
    min: u64,
    max: u64,
};

pub const EncodedValue = struct {
    buf: []u8,
    len: usize,
};

const Self = @This();

// Buffer is for memory ownership,
// TODO: find a way to get rid of it and reuse the memory of values directly
buf: std.ArrayList(u8),
values: std.ArrayList([]const u8),
parsed: std.ArrayList(u64),
allocator: std.mem.Allocator,

pub fn init(allocator: std.mem.Allocator) !*Self {
    const parsed = std.ArrayList(u64).empty;
    var buf = try std.ArrayList(u8).initCapacity(allocator, 512);
    errdefer buf.deinit(allocator);
    var values = try std.ArrayList([]const u8).initCapacity(allocator, 64);
    errdefer values.deinit(allocator);
    const e = try allocator.create(Self);
    e.* = .{
        .buf = buf,
        .values = values,
        .allocator = allocator,
        .parsed = parsed,
    };
    return e;
}

pub fn deinit(self: *Self) void {
    self.values.deinit(self.allocator);
    self.buf.deinit(self.allocator);
    self.parsed.deinit(self.allocator);
    self.allocator.destroy(self);
}

pub fn encode(self: *Self, values: []const []const u8, columnValues: *ColumnDict) !EncodeValueType {
    if (values.len == 0) {
        return .{
            .type = .string,
            .min = 0,
            .max = 0,
        };
    }

    if (try self.tryDictEncoding(values, columnValues)) |result| {
        return result;
    }

    if (try self.tryUintEncoding(values)) |result| {
        return result;
    }

    if (try self.tryIntEncoding(values)) |result| {
        return result;
    }

    if (try self.tryFloat64Encoding(values)) |result| {
        return result;
    }

    if (try self.tryIPv4Encoding(values)) |result| {
        return result;
    }

    if (try self.tryTimestampISO8601Encoding(values)) |result| {
        return result;
    }

    // fall back to string encoding
    for (values) |v| {
        try self.values.append(self.allocator, v);
    }
    return .{ .type = .string, .min = 0, .max = 0 };
}

fn tryDictEncoding(self: *Self, values: []const []const u8, columnValues: *ColumnDict) !?EncodeValueType {
    const startBufLen = self.buf.items.len;
    const startValuesLen = self.values.items.len;
    errdefer {
        self.buf.items.len = startBufLen;
        self.values.items.len = startValuesLen;
        columnValues.reset();
    }

    for (values) |v| {
        const idx = columnValues.set(v) orelse {
            self.buf.items.len = startBufLen;
            self.values.items.len = startValuesLen;
            columnValues.reset();
            return null;
        };

        const start = self.buf.items.len;
        try self.buf.append(self.allocator, idx);
        try self.values.append(self.allocator, self.buf.items[start..]);
    }

    return .{
        .type = .dict,
        .min = 0,
        .max = 0,
    };
}

// TODO: make most of the encoding methods generic
fn tryUintEncoding(self: *Self, values: []const []const u8) !?EncodeValueType {
    if (values.len == 0) return null;

    var minVal: u64 = std.math.maxInt(u64);
    var maxVal: u64 = 0;

    defer self.parsed.clearRetainingCapacity();
    try self.parsed.ensureUnusedCapacity(self.allocator, values.len);
    for (values) |v| {
        const n = std.fmt.parseInt(u64, v, 10) catch return null;
        try self.parsed.append(self.allocator, n);
        minVal = @min(minVal, n);
        maxVal = @max(maxVal, n);
    }

    const bits = if (maxVal == 0) 1 else (64 - @clz(maxVal));
    const vt: ColumnType = switch (bits) {
        0...8 => .uint8,
        9...16 => .uint16,
        17...32 => .uint32,
        else => .uint64,
    };

    // Second pass: encode in one generic codepath
    for (self.parsed.items) |n| {
        const start = self.buf.items.len;
        switch (vt) {
            .uint8 => try self.buf.append(self.allocator, @as(u8, @intCast(n))),
            .uint16 => try self.buf.appendSlice(self.allocator, &Encoder.toBytes(u16, @as(u16, @intCast(n)))),
            .uint32 => try self.buf.appendSlice(self.allocator, &Encoder.toBytes(u32, @as(u32, @intCast(n)))),
            .uint64 => try self.buf.appendSlice(self.allocator, &Encoder.toBytes(u64, n)),
            else => unreachable,
        }
        const slice = self.buf.items[start..];
        try self.values.append(self.allocator, slice);
    }

    return .{
        .type = vt,
        .min = minVal,
        .max = maxVal,
    };
}

fn tryIntEncoding(self: *Self, values: []const []const u8) !?EncodeValueType {
    if (values.len == 0) return null;

    var minVal: i64 = std.math.maxInt(i64);
    var maxVal: i64 = std.math.minInt(i64);

    const startBufLen = self.buf.items.len;
    const startValuesLen = self.values.items.len;
    errdefer {
        self.buf.items.len = startBufLen;
        self.values.items.len = startValuesLen;
    }

    for (values) |v| {
        const n = std.fmt.parseInt(i64, v, 10) catch {
            self.buf.items.len = startBufLen;
            self.values.items.len = startValuesLen;
            return null;
        };
        minVal = @min(minVal, n);
        maxVal = @max(maxVal, n);

        const start = self.buf.items.len;
        try self.buf.appendSlice(self.allocator, &Encoder.toBytes(i64, n));
        try self.values.append(self.allocator, self.buf.items[start..]);
    }

    return .{
        .type = .int64,
        .min = @bitCast(minVal),
        .max = @bitCast(maxVal),
    };
}

fn tryFloat64Encoding(self: *Self, values: []const []const u8) !?EncodeValueType {
    if (values.len == 0) return null;

    var minVal: f64 = std.math.inf(f64);
    var maxVal: f64 = -std.math.inf(f64);

    const startBufLen = self.buf.items.len;
    const startValuesLen = self.values.items.len;
    errdefer {
        self.buf.items.len = startBufLen;
        self.values.items.len = startValuesLen;
    }

    for (values) |v| {
        const n = std.fmt.parseFloat(f64, v) catch {
            self.buf.items.len = startBufLen;
            self.values.items.len = startValuesLen;
            return null;
        };

        minVal = @min(minVal, n);
        maxVal = @max(maxVal, n);

        const bits: u64 = @bitCast(n);

        const start = self.buf.items.len;
        try self.buf.appendSlice(self.allocator, &Encoder.toBytes(u64, bits));
        try self.values.append(self.allocator, self.buf.items[start..]);
    }

    return .{
        .type = .float64,
        .min = @bitCast(minVal),
        .max = @bitCast(maxVal),
    };
}

fn tryIPv4Encoding(self: *Self, values: []const []const u8) !?EncodeValueType {
    var minVal: u32 = std.math.maxInt(u32);
    var maxVal: u32 = 0;

    const startBufLen = self.buf.items.len;
    const startValuesLen = self.values.items.len;
    errdefer {
        self.buf.items.len = startBufLen;
        self.values.items.len = startValuesLen;
    }

    for (values) |v| {
        const n = parseIPv4(v) catch {
            self.buf.items.len = startBufLen;
            self.values.items.len = startValuesLen;
            return null;
        };

        minVal = @min(minVal, n);
        maxVal = @max(maxVal, n);

        const bits: u32 = @bitCast(n);

        const start = self.buf.items.len;
        try self.buf.appendSlice(self.allocator, &Encoder.toBytes(u32, bits));
        try self.values.append(self.allocator, self.buf.items[start..]);
    }

    return .{
        .type = .ipv4,
        .min = minVal,
        .max = maxVal,
    };
}

fn tryTimestampISO8601Encoding(self: *Self, values: []const []const u8) !?EncodeValueType {
    var minVal: i64 = std.math.maxInt(i64);
    var maxVal: i64 = std.math.minInt(i64);

    const startBufLen = self.buf.items.len;
    const startValuesLen = self.values.items.len;
    errdefer {
        self.buf.items.len = startBufLen;
        self.values.items.len = startValuesLen;
    }

    for (values) |v| {
        const time = zeit.Time.fromISO8601(v) catch {
            self.buf.items.len = startBufLen;
            self.values.items.len = startValuesLen;
            return null;
        };
        const n: i64 = @intCast(time.instant().timestamp);

        minVal = @min(minVal, n);
        maxVal = @max(maxVal, n);

        const bits: i64 = @bitCast(n);

        const start = self.buf.items.len;
        try self.buf.appendSlice(self.allocator, &Encoder.toBytes(i64, bits));
        try self.values.append(self.allocator, self.buf.items[start..]);
    }

    return .{
        .type = .timestampIso8601,
        .min = @bitCast(minVal),
        .max = @bitCast(maxVal),
    };
}

fn parseIPv4(s: []const u8) !u32 {
    if (s.len < 7 or s.len > 15) {
        return error.InvalidIPv4;
    }

    var octets: [4]u8 = undefined;
    var octetIdx: u32 = 0;
    var start: usize = 0;

    for (s, 0..) |ch, i| {
        if (ch == '.') {
            if (i == start) {
                return error.InvalidIPv4;
            }
            const octetStr = s[start..i];
            const octet = std.fmt.parseInt(u8, octetStr, 10) catch return error.InvalidIPv4;
            if (octetIdx >= 4) {
                return error.InvalidIPv4;
            }
            octets[octetIdx] = octet;
            octetIdx += 1;
            start = i + 1;
        }
    }

    if (octetIdx != 3 or start >= s.len) {
        return error.InvalidIPv4;
    }

    const last_octet = std.fmt.parseInt(u8, s[start..], 10) catch return error.InvalidIPv4;
    octets[3] = last_octet;

    return (@as(u32, octets[0]) << 24) |
        (@as(u32, octets[1]) << 16) |
        (@as(u32, octets[2]) << 8) |
        @as(u32, octets[3]);
}

test "ValuesEncoder.encodeAndDecodeRoundtrip" {
    const ValuesDecoder = @import("ValuesDecoder.zig");
    const allocator = std.testing.allocator;
    var dictValues = try std.ArrayList([]const u8).initCapacity(allocator, 8);
    defer dictValues.deinit(allocator);
    const dictV = [_][]const u8{ "1111", "2222" };
    dictValues.appendSliceAssumeCapacity(&dictV);

    const Case = struct {
        values: []const []const u8,
        expectedType: ColumnType,
        expectedMin: u64,
        expectedMax: u64,
        expectedDict: ?ColumnDict = null,
    };

    const cases = [_]Case{
        // empty values list
        .{
            .values = &[_][]const u8{},
            .expectedType = .string,
            .expectedMin = 0,
            .expectedMax = 0,
        },
        // String values (more than maxColumnValuesLen = 8)
        .{
            .values = &[_][]const u8{
                "value_0", "value_1", "value_2", "value_3", "value_4",
                "value_5", "value_6", "value_7", "value_8",
            },
            .expectedType = .string,
            .expectedMin = 0,
            .expectedMax = 0,
        },
        // Dict values
        .{
            .values = &[_][]const u8{ "1111", "2222" },
            .expectedType = .dict,
            .expectedMin = 0,
            .expectedMax = 0,
            .expectedDict = .{
                .values = dictValues,
            },
        },
        // uint8 values
        .{
            .values = &[_][]const u8{ "1", "2", "3", "4", "5", "6", "7", "8", "9" },
            .expectedType = .uint8,
            .expectedMin = 1,
            .expectedMax = 9,
        },
        // uint16 values
        .{
            .values = &[_][]const u8{ "256", "512", "768", "1024", "1280", "1536", "1792", "2048", "2304" },
            .expectedType = .uint16,
            .expectedMin = 256,
            .expectedMax = 2304,
        },
        // uint32 values
        .{
            .values = &[_][]const u8{
                "65536",
                "131072",
                "196608",
                "262144",
                "327680",
                "393216",
                "458752",
                "524288",
                "589824",
            },
            .expectedType = .uint32,
            .expectedMin = 65536,
            .expectedMax = 589824,
        },
        // uint64 values
        .{
            .values = &[_][]const u8{
                "4294967296",
                "8589934592",
                "12884901888",
                "17179869184",
                "21474836480",
                "25769803776",
                "30064771072",
                "34359738368",
                "38654705664",
            },
            .expectedType = .uint64,
            .expectedMin = 4294967296,
            .expectedMax = 38654705664,
        },
        // ipv4 values
        .{
            .values = &[_][]const u8{
                "1.2.3.0",
                "1.2.3.1",
                "1.2.3.2",
                "1.2.3.3",
                "1.2.3.4",
                "1.2.3.5",
                "1.2.3.6",
                "1.2.3.7",
                "1.2.3.8",
            },
            .expectedType = .ipv4,
            .expectedMin = 16909056,
            .expectedMax = 16909064,
        },
        // iso8601 timestamps
        .{
            .values = &[_][]const u8{
                "2011-04-19T03:44:01.000Z",
                "2011-04-19T03:44:01.001Z",
                "2011-04-19T03:44:01.002Z",
                "2011-04-19T03:44:01.003Z",
                "2011-04-19T03:44:01.004Z",
                "2011-04-19T03:44:01.005Z",
                "2011-04-19T03:44:01.006Z",
                "2011-04-19T03:44:01.007Z",
                "2011-04-19T03:44:01.008Z",
            },
            .expectedType = .timestampIso8601,
            .expectedMin = 1303184641000000000,
            .expectedMax = 1303184641008000000,
        },
    };

    for (cases) |case| {
        const encoder = try Self.init(allocator);
        defer encoder.deinit();

        var cv = try ColumnDict.init(allocator);
        defer cv.deinit(allocator);

        const valueType = try encoder.encode(case.values, &cv);
        try std.testing.expectEqual(case.expectedType, valueType.type);
        try std.testing.expectEqual(case.expectedMin, valueType.min);
        try std.testing.expectEqual(case.expectedMax, valueType.max);

        const decoder = try ValuesDecoder.init(allocator);
        defer decoder.deinit();

        // create mutable values array pointing to encoded bytes (before we transfer encoder.values)
        var decodedValues = try allocator.alloc([]u8, encoder.values.items.len);
        defer allocator.free(decodedValues);
        for (encoder.values.items, 0..) |encodedValue, i| {
            decodedValues[i] = @constCast(encodedValue);
        }

        // transfer encoder's values to decoder (decoder takes ownership)
        decoder.values = encoder.values;
        decoder.values.clearRetainingCapacity();
        encoder.values = std.ArrayList([]const u8).empty;

        // we can't reuse encoder.buf because it contains the encoded bytes
        try decoder.buf.ensureTotalCapacity(allocator, encoder.buf.capacity);

        // Decode the values - decoder reads encoded bytes from decodedValues,
        // writes strings to decoder.buf, and updates decodedValues pointers
        try decoder.decode(decodedValues, valueType.type, cv.values.items);

        // Compare decoded values with original values
        const expected = if (case.values.len == 0) &[_][]const u8{} else case.values;
        try std.testing.expectEqual(expected.len, decodedValues.len);
        for (expected, decodedValues) |exp, got| {
            try std.testing.expectEqualStrings(exp, got);
        }
        if (case.expectedDict) |expectedDict| {
            try std.testing.expectEqualDeep(expectedDict.values.items, cv.values.items);
        } else {
            try std.testing.expect(cv.values.items.len == 0);
        }
    }
}
