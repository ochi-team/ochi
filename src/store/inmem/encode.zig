const std = @import("std");

const zeit = @import("zeit");

const ColumnValues = @import("block_header.zig").ColumnValues;

// valueType represents the encoding type of values stored in every column block
pub const valueType = enum(u8) {
    unknown = 0,
    string = 1,
    dict = 2,
    uint8 = 3,
    uint16 = 4,
    uint32 = 5,
    uint64 = 6,
    int64 = 10,
    float64 = 7,
    ipv4 = 8,
    timestampIso8601 = 9,
};

pub fn encodeTimestamps(allocator: std.mem.Allocator, tss: []u64) ![]u8 {
    return std.fmt.allocPrint(allocator, "{any}", .{tss});
}

/// Decode timestamps from the encoded format (debug print format: "{ 1, 2, 3 }")
pub fn decodeTimestamps(allocator: std.mem.Allocator, encoded: []const u8) ![]u64 {
    var timestamps = try std.ArrayList(u64).initCapacity(allocator, 10);
    errdefer timestamps.deinit(allocator);

    // Format is "{ N, N, ... }"
    var iter = std.mem.tokenizeScalar(u8, encoded, ' ');

    // Skip opening brace
    _ = iter.next();

    while (iter.next()) |token| {
        if (std.mem.eql(u8, token, "}")) {
            break;
        }

        // Remove trailing comma if present
        const num_str = if (std.mem.endsWith(u8, token, ","))
            token[0 .. token.len - 1]
        else
            token;

        const num = std.fmt.parseInt(u64, num_str, 10) catch continue;
        try timestamps.append(allocator, num);
    }

    return timestamps.toOwnedSlice(allocator);
}

/// Serializer provides a single point for encoding values into byte buffers.
pub const Encoder = struct {
    buf: *std.ArrayList(u8),

    pub fn init(buf: *std.ArrayList(u8)) Encoder {
        return .{ .buf = buf };
    }

    /// Write a typed integer value to the buffer using bitcast
    pub fn writeInt(self: Encoder, comptime T: type, value: T) void {
        const bytes: [@sizeOf(T)]u8 = @bitCast(value);
        self.buf.appendSliceAssumeCapacity(&bytes);
    }

    /// Write raw bytes to the buffer
    pub fn writeBytes(self: Encoder, bytes: []const u8) void {
        self.buf.appendSliceAssumeCapacity(bytes);
    }

    /// Write bytes padded to a fixed size (padding with zeros)
    pub fn writePadded(self: Encoder, bytes: []const u8, totalSize: usize) void {
        if (self.buf.capacity - self.buf.items.len < totalSize) unreachable;
        if (bytes.len > totalSize) @panic("negative padding now allowed");

        const slice = self.buf.unusedCapacitySlice()[0..totalSize];
        @memset(slice, 0x00);
        @memcpy(slice[0..bytes.len], bytes);
        self.buf.items.len += totalSize;
    }
};

const DecodeError = error{
    InsufficientBuffer,
};

/// Decoder provides a single point for reading values from byte buffers.
pub const Decoder = struct {
    buf: []const u8,
    offset: usize = 0,

    pub fn init(buf: []const u8) Decoder {
        return .{ .buf = buf, .offset = 0 };
    }

    /// Read a typed integer value from the buffer using bitcast
    pub fn readInt(self: *Decoder, comptime T: type) !T {
        const size = @sizeOf(T);
        if (self.offset + size > self.buf.len) {
            return DecodeError.InsufficientBuffer;
        }
        const bytes: [size]u8 = self.buf[self.offset..][0..size].*;
        self.offset += size;
        return @bitCast(bytes);
    }

    /// Read raw bytes from the buffer
    pub fn readBytes(self: *Decoder, len: usize) ![]const u8 {
        if (self.offset + len > self.buf.len) {
            return DecodeError.InsufficientBuffer;
        }
        const result = self.buf[self.offset .. self.offset + len];
        self.offset += len;
        return result;
    }

    /// Read padded bytes (fixed size with zero padding), return the actual content without padding
    pub fn readPadded(self: *Decoder, totalSize: usize) ![]const u8 {
        const bytes = try self.readBytes(totalSize);
        // Find the length of actual content (before padding zeros)
        const len = std.mem.indexOfScalar(u8, bytes, 0) orelse totalSize;
        return bytes[0..len];
    }

    /// Peek at the current position without advancing offset
    pub fn peek(self: *Decoder, len: usize) ![]const u8 {
        if (self.offset + len > self.buf.len) {
            return DecodeError.InsufficientBuffer;
        }
        defer {
            self.offset += len;
        }
        return self.buf[self.offset .. self.offset + len];
    }

    /// Get remaining bytes from current offset
    pub fn remaining(self: Decoder) []const u8 {
        return self.buf[self.offset..];
    }
};

pub const EncodeValueType = struct {
    vt: valueType,
    min: u64,
    max: u64,
};

pub const ValuesEncoder = struct {
    buf: std.ArrayList(u8),
    values: std.ArrayList([]const u8),
    parsed: std.ArrayList(u64),
    allocator: std.mem.Allocator,

    pub fn init(allocator: std.mem.Allocator) !*ValuesEncoder {
        var parsed = try std.ArrayList(u64).initCapacity(allocator, 128);
        errdefer parsed.deinit(allocator);
        var buf = try std.ArrayList(u8).initCapacity(allocator, 512);
        errdefer buf.deinit(allocator);
        var values = try std.ArrayList([]const u8).initCapacity(allocator, 64);
        errdefer values.deinit(allocator);
        const e = try allocator.create(ValuesEncoder);
        e.* = .{
            .buf = buf,
            .values = values,
            .allocator = allocator,
            .parsed = parsed,
        };
        return e;
    }

    pub fn deinit(self: *ValuesEncoder) void {
        self.values.deinit(self.allocator);
        self.buf.deinit(self.allocator);
        self.parsed.deinit(self.allocator);
        self.allocator.destroy(self);
    }

    pub fn encode(self: *ValuesEncoder, values: []const []const u8, columnValues: *ColumnValues) !EncodeValueType {
        if (values.len == 0) {
            return .{ .vt = .string, .min = 0, .max = 0 };
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
        return .{ .vt = .string, .min = 0, .max = 0 };
    }

    fn tryDictEncoding(self: *ValuesEncoder, values: []const []const u8, columnValues: *ColumnValues) !?EncodeValueType {
        for (values) |v| {
            const idx = columnValues.set(v) orelse return null;

            const start = self.buf.items.len;
            try self.buf.append(self.allocator, idx);
            try self.values.append(self.allocator, self.buf.items[start..]);
        }

        return .{ .vt = .dict, .min = 0, .max = 0 };
    }

    // TODO: make most of the encoding methods generic
    fn tryUintEncoding(self: *ValuesEncoder, values: []const []const u8) !?EncodeValueType {
        if (values.len == 0) return null;

        var minVal: u64 = std.math.maxInt(u64);
        var maxVal: u64 = 0;

        defer self.parsed.clearRetainingCapacity();
        for (values) |v| {
            const n = std.fmt.parseInt(u64, v, 10) catch return null;
            try self.parsed.append(self.allocator, n);
            minVal = @min(minVal, n);
            maxVal = @max(maxVal, n);
        }

        const bits = if (maxVal == 0) 1 else (64 - @clz(maxVal));
        const vt: valueType = switch (bits) {
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
                .uint16 => try self.buf.appendSlice(self.allocator, &std.mem.toBytes(@as(u16, @intCast(n)))),
                .uint32 => try self.buf.appendSlice(self.allocator, &std.mem.toBytes(@as(u32, @intCast(n)))),
                .uint64 => try self.buf.appendSlice(self.allocator, &std.mem.toBytes(n)),
                else => unreachable,
            }
            const slice = self.buf.items[start..];
            try self.values.append(self.allocator, slice);
        }

        return .{ .vt = vt, .min = minVal, .max = maxVal };
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

        for (values) |v| {
            const n = std.fmt.parseInt(i64, v, 10) catch {
                self.buf.items.len = startBufLen;
                self.values.items.len = startValuesLen;
                return null;
            };
            minVal = @min(minVal, n);
            maxVal = @max(maxVal, n);

            const start = self.buf.items.len;
            try self.buf.appendSlice(self.allocator, &std.mem.toBytes(n));
            try self.values.append(self.allocator, self.buf.items[start..]);
        }

        return .{ .vt = .int64, .min = @bitCast(minVal), .max = @bitCast(maxVal) };
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

        for (values) |v| {
            const n = std.fmt.parseFloat(f64, v) catch {
                self.buf.items.len = startBufLen;
                self.values.items.len = startValuesLen;
                return null;
            };

            minVal = @min(minVal, n);
            maxVal = @max(maxVal, n);

            const bits = @as(u64, @bitCast(n));

            const start = self.buf.items.len;
            try self.buf.appendSlice(self.allocator, &std.mem.toBytes(bits));
            try self.values.append(self.allocator, self.buf.items[start..]);
        }

        return .{
            .vt = .float64,
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
            try self.buf.appendSlice(self.allocator, &std.mem.toBytes(bits));
            try self.values.append(self.allocator, self.buf.items[start..]);
        }

        return .{ .vt = .ipv4, .min = minVal, .max = maxVal };
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
            try self.buf.appendSlice(self.allocator, &std.mem.toBytes(bits));
            try self.values.append(self.allocator, self.buf.items[start..]);
        }

        return .{ .vt = .timestampIso8601, .min = @bitCast(minVal), .max = @bitCast(maxVal) };
    }
};

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
