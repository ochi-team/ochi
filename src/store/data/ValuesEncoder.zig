const std = @import("std");

const zeit = @import("zeit");

const encoding = @import("encoding");
const Encoder = encoding.Encoder;

const parseTimestampISO8601 = @import("../../stds/time.zig").parseTimestampISO8601;
const ColumnDict = @import("ColumnDict.zig");
const ColumnType = @import("ColumnHeader.zig").ColumnType;

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
    const e = try allocator.create(Self);
    e.* = .{
        .buf = .empty,
        .values = .empty,
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

    // same amount since buf would store only dict ids (1 byte each)
    try self.buf.ensureUnusedCapacity(self.allocator, values.len);
    try self.values.ensureUnusedCapacity(self.allocator, values.len);
    for (values) |v| {
        const idx = columnValues.set(v) orelse {
            self.buf.items.len = startBufLen;
            self.values.items.len = startValuesLen;
            columnValues.reset();
            return null;
        };

        const start = self.buf.items.len;
        self.buf.appendAssumeCapacity(idx);
        self.values.appendAssumeCapacity(self.buf.items[start..]);
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
    const width: usize = switch (vt) {
        .uint8 => 1,
        .uint16 => 2,
        .uint32 => 4,
        .uint64 => 8,
        else => std.debug.panic("unexpected uint type, given={any}", .{vt}),
    };

    // Second pass: encode in one generic codepath
    try self.buf.ensureUnusedCapacity(self.allocator, width * self.parsed.items.len);
    try self.values.ensureUnusedCapacity(self.allocator, self.parsed.items.len);
    for (self.parsed.items) |n| {
        const start = self.buf.items.len;
        switch (vt) {
            .uint8 => self.buf.appendAssumeCapacity(@as(u8, @intCast(n))),
            .uint16 => self.buf.appendSliceAssumeCapacity(&Encoder.toBytes(u16, @as(u16, @intCast(n)))),
            .uint32 => self.buf.appendSliceAssumeCapacity(&Encoder.toBytes(u32, @as(u32, @intCast(n)))),
            .uint64 => self.buf.appendSliceAssumeCapacity(&Encoder.toBytes(u64, n)),
            else => std.debug.panic("unexpected uint type, given={any}", .{vt}),
        }
        const slice = self.buf.items[start..];
        self.values.appendAssumeCapacity(slice);
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

    try self.buf.ensureUnusedCapacity(self.allocator, @sizeOf(i64) * values.len);
    try self.values.ensureUnusedCapacity(self.allocator, values.len);
    for (values) |v| {
        const n = std.fmt.parseInt(i64, v, 10) catch {
            self.buf.items.len = startBufLen;
            self.values.items.len = startValuesLen;
            return null;
        };
        minVal = @min(minVal, n);
        maxVal = @max(maxVal, n);

        const start = self.buf.items.len;
        self.buf.appendSliceAssumeCapacity(&Encoder.toBytes(i64, n));
        self.values.appendAssumeCapacity(self.buf.items[start..]);
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

    try self.buf.ensureUnusedCapacity(self.allocator, @sizeOf(u64) * values.len);
    try self.values.ensureUnusedCapacity(self.allocator, values.len);
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
        self.buf.appendSliceAssumeCapacity(&Encoder.toBytes(u64, bits));
        self.values.appendAssumeCapacity(self.buf.items[start..]);
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

    try self.buf.ensureUnusedCapacity(self.allocator, @sizeOf(u32) * values.len);
    try self.values.ensureUnusedCapacity(self.allocator, values.len);
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
        self.buf.appendSliceAssumeCapacity(&Encoder.toBytes(u32, bits));
        self.values.appendAssumeCapacity(self.buf.items[start..]);
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

    try self.buf.ensureUnusedCapacity(self.allocator, @sizeOf(i64) * values.len);
    try self.values.ensureUnusedCapacity(self.allocator, values.len);
    for (values) |v| {
        const n = parseTimestampISO8601(v) orelse {
            self.buf.items.len = startBufLen;
            self.values.items.len = startValuesLen;
            return null;
        };

        minVal = @min(minVal, n);
        maxVal = @max(maxVal, n);

        const bits: i64 = @bitCast(n);

        const start = self.buf.items.len;
        self.buf.appendSliceAssumeCapacity(&Encoder.toBytes(i64, bits));
        self.values.appendAssumeCapacity(self.buf.items[start..]);
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

const ValuesDecoder = @import("ValuesDecoder.zig");

test "ValuesEncoder.encodeAndDecodeRoundtrip" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var dictValues = try std.ArrayList([]const u8).initCapacity(allocator, 8);
    defer dictValues.deinit(allocator);
    const dictV = [_][]const u8{ "1111", "2222" };
    dictValues.appendSliceAssumeCapacity(&dictV);

    var dictValuesOverflow = try std.ArrayList([]const u8).initCapacity(allocator, 8);
    defer dictValuesOverflow.deinit(allocator);
    const dictValuesOverflowV = [_][]const u8{"2424242424242424242424242424242424242424"};
    dictValuesOverflow.appendSliceAssumeCapacity(&dictValuesOverflowV);

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
        // int overflow
        .{
            .values = &[_][]const u8{
                "2424242424242424242424242424242424242424",
            },
            .expectedType = .dict,
            .expectedMin = 0,
            .expectedMax = 0,
            .expectedDict = .{
                .values = dictValuesOverflow,
            },
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
        var decodedValues = try allocator.alloc([]const u8, encoder.values.items.len);
        defer allocator.free(decodedValues);
        for (encoder.values.items, 0..) |encodedValue, i| {
            decodedValues[i] = encodedValue;
        }

        // transfer encoder's values to decoder (decoder takes ownership)
        decoder.values = encoder.values;
        decoder.values.clearRetainingCapacity();
        encoder.values = std.ArrayList([]const u8).empty;

        // we can't reuse encoder.buf because it contains the encoded bytes
        try decoder.buf.ensureTotalCapacity(allocator, encoder.buf.capacity);

        // Decode the values - decoder reads encoded bytes from decodedValues,
        // writes strings to decoder.buf, and updates decodedValues pointers
        try decoder.decode(io, decodedValues, valueType.type, cv.values.items);

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

test "ValuesEncoder fuzz" {
    try std.testing.fuzz({}, fuzzMixedShortValues, .{
        .corpus = &.{
            "",
            "a",
            "b",
            "0",
            "1",
            "255",
            "256",
            "65535",
            "65536",
            "4294967295",
            "4294967296",
            "18446744073709551615",
            "001",
            "000022",
            "42.2",
            "44.989898989",
            "-9223372036854775808",
            "9223372036854775807",
            "-0",
            "+42",
            "1.5",
            "2e3",
            "-0.0",
            "inf",
            "-inf",
            "nan",
            "1e309",
            // IPv4 octet boundaries.
            "0.0.0.0",
            "0.0.0.1",
            "0.0.0.255",
            "0.0.1.0",
            "0.0.255.0",
            "0.1.0.0",
            "0.255.0.0",
            "1.0.0.0",
            "1.2.3.4",
            "127.0.0.1",
            "128.0.0.1",
            "192.168.0.1",
            "255.255.255.255",
            // IPv4 leading zero policy.
            "01.2.3.4",
            "1.02.3.4",
            "1.2.003.4",
            "1.2.3.004",
            "000.000.000.000",
            "255.255.255.0255",
            // IPv4 too many/few octets.
            "1",
            "1.2",
            "1.2.3",
            "1.2.3.4.5",
            "1.2.3.4.5.6",
            // IPv4 empty octets and trailing dots.
            ".1.2.3",
            "1..2.3",
            "1.2..3",
            "1.2.3.",
            "1.2.3.4.",
            // IPv4 out-of-range octets.
            "256.0.0.0",
            "0.256.0.0",
            "0.0.256.0",
            "0.0.0.256",
            "999.999.999.999",
            "-1.2.3.4",
            "1.-2.3.4",
            // ISO8601 timestamps around epoch.
            "1969-12-31T23:59:59.999Z",
            "1970-01-01T00:00:00.000Z",
            "1970-01-01T00:00:00.001Z",
            "2011-04-19T03:44:01.000Z",
            "2034-04-19T03:44:01.000123+03:00",
            // ISO8601 leap-day dates.
            "2000-02-29T00:00:00.000Z",
            "2004-02-29T12:34:56.789Z",
            "1900-02-29T00:00:00.000Z",
            "2019-02-29T00:00:00.000Z",
            "2020-02-29T23:59:59.999Z",
            // ISO8601 fractional milliseconds and unsupported precision.
            "2011-04-19T03:44:01Z",
            "2011-04-19T03:44:01.0Z",
            "2011-04-19T03:44:01.01Z",
            "2011-04-19T03:44:01.001Z",
            "2011-04-19T03:44:01.000001Z",
            "2011-04-19T03:44:01.000000001Z",
            "2011-04-19T03:44:01.0000000001Z",
            // ISO8601 invalid dates.
            "2011-00-19T03:44:01.000Z",
            "2011-13-19T03:44:01.000Z",
            "2011-04-00T03:44:01.000Z",
            "2011-04-31T03:44:01.000Z",
            "2011-04-19T24:00:00.000Z",
            "2011-04-19T23:60:00.000Z",
            "2011-04-19T23:59:60.000Z",
            // ISO8601 missing timezone.
            "2011-04-19T03:44:01",
            "2011-04-19T03:44:01.000",
            "20240224T154944",
            // ISO8601 timezone variants.
            "2011-04-19T03:44:01.000+03:00",
            "2011-04-19T03:44:01.000-07:30",
            "2011-04-19T03:44:01.000+0300",
            "2011-04-19T03:44:01.000+03",
            "@key",
            "ke.y",
            ":key",
            "key:",
            "#key,",
            "key:42",
            "&key:42",
        },
    });
}

fn fuzzMixedShortValues(_: void, smith: *std.testing.Smith) !void {
    @disableInstrumentation();

    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var storage: [32][64]u8 = undefined;
    var values: [32][]const u8 = undefined;
    const count: usize = @intCast(smith.valueRangeAtMost(u8, 0, values.len));

    for (values[0..count], 0..) |*value, i| {
        const len = smith.sliceWeightedBytes(&storage[i], &.{
            .rangeAtMost(u8, '0', '9', 4),
            .rangeAtMost(u8, 'a', 'z', 5),
            .rangeAtMost(u8, 'A', 'Z', 2),
            .value(u8, '.', 3),
            .value(u8, '-', 2),
            .value(u8, ':', 1),
            .value(u8, 'T', 1),
            .value(u8, 'Z', 1),
            .value(u8, '_', 1),
            .value(u8, 0, 1),
        });
        value.* = storage[i][0..len];
    }

    try expectEncodeRoundtrip(io, allocator, values[0..count]);
}

fn expectEncodeRoundtrip(
    io: std.Io,
    allocator: std.mem.Allocator,
    input: []const []const u8,
) !void {
    const encoder = try Self.init(allocator);
    defer encoder.deinit();

    var cv = try ColumnDict.init(allocator);
    defer cv.deinit(allocator);

    const valueType = try encoder.encode(input, &cv);

    const decoder = try ValuesDecoder.init(allocator);
    defer decoder.deinit();

    var decodedValues = try allocator.alloc([]const u8, encoder.values.items.len);
    defer allocator.free(decodedValues);
    for (encoder.values.items, 0..) |encodedValue, i| {
        decodedValues[i] = encodedValue;
    }

    try decoder.decode(io, decodedValues, valueType.type, cv.values.items);

    switch (valueType.type) {
        .string, .dict => {
            try std.testing.expectEqual(input.len, decodedValues.len);
            for (input, decodedValues) |expected, got| {
                try std.testing.expectEqualStrings(expected, got);
            }
        },
        .uint8, .uint16, .uint32, .uint64 => {
            try std.testing.expectEqual(input.len, decodedValues.len);
            for (input, decodedValues) |expected, got| {
                try std.testing.expectEqual(
                    try std.fmt.parseInt(u64, expected, 10),
                    try std.fmt.parseInt(u64, got, 10),
                );
            }
        },
        .int64 => {
            try std.testing.expectEqual(input.len, decodedValues.len);
            for (input, decodedValues) |expected, got| {
                try std.testing.expectEqual(
                    try std.fmt.parseInt(i64, expected, 10),
                    try std.fmt.parseInt(i64, got, 10),
                );
            }
        },
        .float64 => {
            try std.testing.expectEqual(input.len, decodedValues.len);
            for (input, decodedValues) |expected, got| {
                const expectedFloat = try std.fmt.parseFloat(f64, expected);
                const gotFloat = try std.fmt.parseFloat(f64, got);
                if (std.math.isNan(expectedFloat)) {
                    try std.testing.expect(std.math.isNan(gotFloat));
                } else {
                    try std.testing.expectEqual(expectedFloat, gotFloat);
                }
            }
        },
        .ipv4 => {
            try std.testing.expectEqual(input.len, decodedValues.len);
            for (input, decodedValues) |expected, got| {
                try std.testing.expectEqual(try parseIPv4(expected), try parseIPv4(got));
            }
        },
        .timestampIso8601 => {
            try std.testing.expectEqual(input.len, decodedValues.len);
            for (input, decodedValues) |expected, got| {
                try std.testing.expectEqual(parseTimestampISO8601(expected), parseTimestampISO8601(got));
            }
        },
        .unknown => return error.UnknownValueType,
    }
}

test "ValuesEncoder does not treat bare number as timestamp" {
    const allocator = std.testing.allocator;

    const values = [_][]const u8{
        "2011-04-19T03:44:01.000Z",
        "2011-04-19T03:44:01.001Z",
        "2011-04-19T03:44:01.002Z",
        "2011-04-19T03:44:01.003Z",
        "2011-04-19T03:44:01.004Z",
        "2011-04-19T03:44:01.005Z",
        "2011-04-19T03:44:01.006Z",
        "2011-04-19T03:44:01.007Z",
        "389",
    };

    const encoder = try Self.init(allocator);
    defer encoder.deinit();

    var cv = try ColumnDict.init(allocator);
    defer cv.deinit(allocator);

    const valueType = try encoder.encode(&values, &cv);
    try std.testing.expectEqual(.string, valueType.type);
    try std.testing.expectEqual(0, valueType.min);
    try std.testing.expectEqual(0, valueType.max);
    try std.testing.expectEqualDeep(&values, encoder.values.items);
}

test "ValuesEncoder handles dict values shorter than row count" {
    const allocator = std.testing.allocator;

    const Case = struct {
        values: []const []const u8,
        expectedDict: []const []const u8,
        expectedIndexes: []const u8,
    };

    const cases = [_]Case{
        .{
            .values = &[_][]const u8{""},
            .expectedDict = &[_][]const u8{""},
            .expectedIndexes = &[_]u8{0},
        },
        .{
            .values = &[_][]const u8{ "", "" },
            .expectedDict = &[_][]const u8{""},
            .expectedIndexes = &[_]u8{ 0, 0 },
        },
        .{
            .values = &[_][]const u8{ "a", "", "a" },
            .expectedDict = &[_][]const u8{ "a", "" },
            .expectedIndexes = &[_]u8{ 0, 1, 0 },
        },
    };

    for (cases) |case| {
        const encoder = try Self.init(allocator);
        defer encoder.deinit();

        var cv = try ColumnDict.init(allocator);
        defer cv.deinit(allocator);

        const valueType = try encoder.encode(case.values, &cv);
        try std.testing.expectEqual(.dict, valueType.type);
        try std.testing.expectEqualDeep(case.expectedDict, cv.values.items);
        try std.testing.expectEqual(case.expectedIndexes.len, encoder.values.items.len);
        for (case.expectedIndexes, encoder.values.items) |expectedIdx, encodedValue| {
            try std.testing.expectEqualSlices(u8, &[_]u8{expectedIdx}, encodedValue);
        }
    }
}

test "ValuesEncoder keeps buffered value slices stable after growth" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var input: [ColumnDict.maxDictColumnValuesLen * 4][]const u8 = undefined;
    const dictValues = [_][]const u8{ "error", "", "warn", "info" };
    for (&input, 0..) |*value, i| {
        value.* = dictValues[i % dictValues.len];
    }

    const encoder = try Self.init(allocator);
    defer encoder.deinit();

    var cv = try ColumnDict.init(allocator);
    defer cv.deinit(allocator);

    const valueType = try encoder.encode(&input, &cv);
    try std.testing.expectEqual(.dict, valueType.type);
    try std.testing.expectEqualDeep(&dictValues, cv.values.items);

    var decodedValues: [ColumnDict.maxDictColumnValuesLen * 4][]const u8 = undefined;
    for (encoder.values.items, 0..) |encodedValue, i| {
        decodedValues[i] = encodedValue;
    }

    const decoder = try ValuesDecoder.init(allocator);
    defer decoder.deinit();

    try decoder.decode(io, decodedValues[0..decodedValues.len], valueType.type, cv.values.items);
    try std.testing.expectEqualDeep(&input, decodedValues[0..decodedValues.len]);
}
