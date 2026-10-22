const std = @import("std");

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

const ValuesEncoder = @This();

// Buffer is for memory ownership,
// TODO: find a way to get rid of it and reuse the memory of values directly
buf: std.ArrayList(u8),
values: std.ArrayList([]const u8),
parsed: std.ArrayList(u64),
allocator: std.mem.Allocator,

pub fn init(allocator: std.mem.Allocator) !ValuesEncoder {
    const parsed = std.ArrayList(u64).empty;
    return .{
        .buf = .empty,
        .values = .empty,
        .allocator = allocator,
        .parsed = parsed,
    };
}

pub fn deinit(self: *ValuesEncoder) void {
    self.values.deinit(self.allocator);
    self.buf.deinit(self.allocator);
    self.parsed.deinit(self.allocator);
}

pub fn reset(self: *ValuesEncoder) void {
    self.values.clearRetainingCapacity();
    self.buf.clearRetainingCapacity();
    self.parsed.clearRetainingCapacity();
}

pub fn encode(self: *ValuesEncoder, values: []const []const u8, columnValues: *ColumnDict) !EncodeValueType {
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
    // TODO: consider using FSST/snappy/lz4 instead of direct zstd or combination of either,
    // zstd feels not the best compression/throughput
    for (values) |v| {
        try self.values.append(self.allocator, v);
    }
    return .{ .type = .string, .min = 0, .max = 0 };
}

fn tryDictEncoding(self: *ValuesEncoder, values: []const []const u8, columnValues: *ColumnDict) !?EncodeValueType {
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

pub fn encodeUint(vt: ColumnType, n: u64, dst: *[8]u8) usize {
    switch (vt) {
        .uint8 => {
            dst[0] = @intCast(n);
            return 1;
        },
        .uint16 => {
            dst[0..2].* = Encoder.toBytes(u16, @intCast(n));
            return 2;
        },
        .uint32 => {
            dst[0..4].* = Encoder.toBytes(u32, @intCast(n));
            return 4;
        },
        .uint64 => {
            dst.* = Encoder.toBytes(u64, n);
            return 8;
        },
        else => std.debug.panic("unexpected uint type, given={any}", .{vt}),
    }
}

// TODO: make most of the encoding methods generic
fn tryUintEncoding(self: *ValuesEncoder, values: []const []const u8) !?EncodeValueType {
    if (values.len == 0) return null;

    var minVal: u64 = std.math.maxInt(u64);
    var maxVal: u64 = 0;

    defer self.parsed.clearRetainingCapacity();
    try self.parsed.ensureUnusedCapacity(self.allocator, values.len);
    for (values) |v| {
        const n = parseUint64(v) orelse return null;
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
    var dst: [8]u8 = undefined;
    for (self.parsed.items) |i| {
        const start = self.buf.items.len;
        const n = encodeUint(vt, i, &dst);
        const val = dst[0..n];
        self.buf.appendSliceAssumeCapacity(val);
        self.values.appendAssumeCapacity(self.buf.items[start..]);
    }

    return .{
        .type = vt,
        .min = minVal,
        .max = maxVal,
    };
}

fn tryIntEncoding(self: *ValuesEncoder, values: []const []const u8) !?EncodeValueType {
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
        const n = parseInt64(v) orelse {
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
        // TODO: validate it's searchable (the value fits in rnage after bitCast)
        .min = @bitCast(minVal),
        .max = @bitCast(maxVal),
    };
}

fn tryFloat64Encoding(self: *ValuesEncoder, values: []const []const u8) !?EncodeValueType {
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
        const n = parseFloat64(v) orelse {
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

fn tryIPv4Encoding(self: *ValuesEncoder, values: []const []const u8) !?EncodeValueType {
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

fn tryTimestampISO8601Encoding(self: *ValuesEncoder, values: []const []const u8) !?EncodeValueType {
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

/// Parses decimal digits with optional '_' separators between them,
/// rejects sign and leading zeros that std.fmt.parseInt accepts.
/// '_' doesn't format back ("1_000" decodes as "1000")
pub fn parseUint64(s: []const u8) ?u64 {
    if (s.len == 0 or (s.len > 1 and s[0] == '0')) return null;
    if (s[0] == '_' or s[s.len - 1] == '_') return null;
    var n: u64 = 0;
    for (s) |c| {
        if (c == '_') continue;
        if (c < '0' or c > '9') return null;
        n = std.math.mul(u64, n, 10) catch return null;
        n = std.math.add(u64, n, c - '0') catch return null;
    }
    return n;
}

/// Same as parseUint64 with an optional '-'.
/// "-0" is rejected.
pub fn parseInt64(s: []const u8) ?i64 {
    if (s.len == 0 or s[0] != '-') return std.math.cast(i64, parseUint64(s) orelse return null);
    const n = parseUint64(s[1..]) orelse return null;
    if (n == 0) return null;
    return std.math.cast(i64, -@as(i128, n));
}

/// Accepts only [-]int[.digits], where int is canonical as in parseUint64,
/// std.fmt.parseFloat also takes "+1", "01", "1e3", "inf", ".5" that don't format back.
pub fn parseFloat64(s: []const u8) ?f64 {
    const unsigned = if (s.len > 0 and s[0] == '-') s[1..] else s;
    const dot = std.mem.indexOfScalar(u8, unsigned, '.') orelse unsigned.len;
    _ = parseUint64(unsigned[0..dot]) orelse return null;
    if (dot < unsigned.len) {
        const frac = unsigned[dot + 1 ..];
        if (frac.len == 0) return null;
        for (frac) |c| if (c < '0' or c > '9') return null;
    }
    return std.fmt.parseFloat(f64, s) catch null;
}

pub fn parseIPv4(s: []const u8) !u32 {
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
