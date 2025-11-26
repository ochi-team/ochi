const std = @import("std");

const zeit = @import("zeit");

const compress = @import("../../compress/compress.zig");

const ColumnValues = @import("block_header.zig").ColumnDict;
const ColumnType = @import("block_header.zig").ColumnType;

pub fn encodeTimestamps(allocator: std.mem.Allocator, tss: []u64) ![]u8 {
    return std.fmt.allocPrint(allocator, "{any}", .{tss});
}

/// Decode timestamps from the encoded format (debug print format: "{ 1, 2, 3 }")
pub fn decodeTimestamps(allocator: std.mem.Allocator, encoded: []const u8) !std.ArrayList(u64) {
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

    return timestamps;
}

/// Serializer provides a single point for encoding values into byte buffers.
pub const Encoder = struct {
    buf: []u8,
    offset: usize = 0,

    pub fn init(buf: []u8) Encoder {
        return .{ .buf = buf };
    }

    /// Write a typed integer value to the buffer using bitcast
    pub fn writeInt(self: *Encoder, comptime T: type, value: T) void {
        const slice = self.buf[self.offset .. self.offset + @sizeOf(T)];
        if (slice.len < @sizeOf(T)) unreachable;
        self.offset += @sizeOf(T);
        const bytes: [@sizeOf(T)]u8 = @bitCast(value);
        @memcpy(slice, bytes[0..]);
    }

    /// Write raw bytes to the buffer
    pub fn writeBytes(self: *Encoder, bytes: []const u8) void {
        const slice = self.buf[self.offset .. self.offset + bytes.len];
        if (slice.len < bytes.len) unreachable;
        self.offset += bytes.len;
        @memcpy(slice, bytes[0..]);
    }

    /// Write bytes padded to a fixed size (padding with zeros)
    pub fn writePadded(self: *Encoder, bytes: []const u8, totalSize: usize) void {
        if (bytes.len > totalSize) @panic("negative padding now allowed");

        const slice = self.buf[self.offset .. self.offset + totalSize];
        if (slice.len < totalSize) unreachable;
        self.offset += totalSize;

        @memset(slice, 0x00);
        @memcpy(slice[0..bytes.len], bytes);
    }

    /// The maximum number of bytes a varint-encoded 64-bit integer can occupy.
    pub const maxVarUint64Len = 10;

    /// writeLeb128 encodes a u64 into a variable-length byte sequence.
    /// Returns error.OutOfMemory if the buffer has not enough capacity.
    pub fn writeLeb128(self: *Encoder, value: u64) std.mem.Allocator.Error!void {
        if (self.buf[self.offset..].len < 10) return std.mem.Allocator.Error.OutOfMemory;

        const slice = self.buf[self.offset .. self.offset + 10];

        var i: u8 = 0;
        var v = value;
        while (v >= 0x80) {
            slice[i] = @as(u8, @truncate(v)) | 0x80;
            v >>= 7;
            i += 1;
        }
        slice[i] = @as(u8, @truncate(v));

        self.offset = i + 1;
    }

    fn writeIntBytes(self: *Encoder, size: usize, value: u64) void {
        const buf: [8]u8 = @bitCast(value);
        self.writeBytes(buf[0..size]);
    }
};

test "Encoder.writeIntBytes" {
    var allocator = std.testing.allocator;
    const Case = struct {
        type: type,
        value: u64,
    };
    inline for ([_]Case{
        .{
            .type = u8,
            .value = 42,
        },
        .{
            .type = u16,
            .value = 501,
        },
        .{
            .type = u32,
            .value = 123456,
        },
    }) |case| {
        const buf1 = try allocator.alloc(u8, @sizeOf(case.type));
        const buf2 = try allocator.alloc(u8, @sizeOf(case.type));
        defer allocator.free(buf1);
        defer allocator.free(buf2);
        var enc1 = Encoder.init(buf1);
        var enc2 = Encoder.init(buf2);
        enc1.writeInt(case.type, case.value);
        enc2.writeIntBytes(@sizeOf(case.type), case.value);
        try std.testing.expectEqualSlices(u8, buf1, buf2);
    }
}

test "Encoder.writeVarUint64" {
    const allocator = std.testing.allocator;
    const buf = try allocator.alloc(u8, 20);
    defer allocator.free(buf);

    const Case = struct {
        value: u64,
        expected: []const u8,
    };

    const cases = [_]Case{
        .{ .value = 0, .expected = &[_]u8{0x00} },
        .{ .value = 1, .expected = &[_]u8{0x01} },
        .{ .value = 127, .expected = &[_]u8{0x7f} },
        .{ .value = 128, .expected = &[_]u8{ 0x80, 0x01 } },
        .{ .value = 255, .expected = &[_]u8{ 0xff, 0x01 } },
        .{ .value = 16383, .expected = &[_]u8{ 0xff, 0x7f } },
        .{ .value = 16384, .expected = &[_]u8{ 0x80, 0x80, 0x01 } },
        .{ .value = std.math.maxInt(u64), .expected = &[_]u8{ 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x01 } },
    };

    for (cases) |case| {
        var enc = Encoder.init(buf);
        try enc.writeLeb128(case.value);
        try std.testing.expectEqualSlices(u8, case.expected, buf[0..enc.offset]);
    }

    // Test OutOfMemory
    const small_buf = try allocator.alloc(u8, 1);
    defer allocator.free(small_buf);
    var enc = Encoder.init(small_buf);
    try std.testing.expectError(std.mem.Allocator.Error.OutOfMemory, enc.writeLeb128(std.math.maxInt(u64)));
}

const DecodeError = error{
    InsufficientBuffer,
};

/// Decoder provides a single point for reading values from byte buffers.
pub const Decoder = struct {
    buf: []const u8,
    offset: usize = 0,

    pub fn init(buf: []const u8) Decoder {
        return .{ .buf = buf };
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
};

pub const EncodeValueType = struct {
    type: ColumnType,
    min: u64,
    max: u64,
};

pub const EncodedValue = struct {
    buf: []u8,
    len: usize,
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
    const Width = struct {
        max: u64,
        size: usize,
        block: u8,
        blockCell: u8,
    };
    const uintBlockType8: u8 = 0;
    const uintBlockType16: u8 = 1;
    const uintBlockType32: u8 = 2;
    const uintBlockType64: u8 = 3;
    const uintBlockTypeCell8: u8 = 4;
    const uintBlockTypeCell16: u8 = 5;
    const uintBlockTypeCell32: u8 = 6;
    const uintBlockTypeCell64: u8 = 7;

    const widths = [_]Width{
        .{ .max = (1 << 8), .block = uintBlockType8, .blockCell = uintBlockTypeCell8, .size = @sizeOf(u8) },
        .{ .max = (1 << 16), .block = uintBlockType16, .blockCell = uintBlockTypeCell16, .size = @sizeOf(u16) },
        .{ .max = (1 << 32), .block = uintBlockType32, .blockCell = uintBlockTypeCell32, .size = @sizeOf(u32) },
        .{ .max = ~@as(u64, 0), .block = uintBlockType64, .blockCell = uintBlockTypeCell64, .size = @sizeOf(u64) },
    };
    fn pickWidth(maxLen: u64) Width {
        for (widths) |w| {
            if (maxLen < w.max) return w;
        }
        unreachable;
    }

    const compressionKindPlain: u8 = 0;
    const compressionKindZstd: u8 = 1;

    pub fn encodeValues(self: *ValuesEncoder) ![]u8 {
        defer self.parsed.clearRetainingCapacity();
        try self.parsed.ensureUnusedCapacity(self.allocator, self.values.items.len);
        var lenSum: usize = 0;
        for (self.values.items) |v| {
            self.parsed.appendAssumeCapacity(@intCast(v.len));
            lenSum += v.len;
        }

        var maxLen: u64 = 0;
        for (self.parsed.items) |n| {
            if (n > maxLen) maxLen = n;
        }

        var stackFba = std.heap.stackFallback(2048, self.allocator);
        const fba = stackFba.get();
        var interBuf: []u8 = &[_]u8{};
        defer {
            if (interBuf.len > 0) fba.free(interBuf);
        }
        const areCells = (self.parsed.items.len >= 2) and numbersAreSame(self.parsed.items[0..]);
        const w = pickWidth(maxLen);
        if (areCells) {
            interBuf = try fba.alloc(u8, 1 + w.size);
            var enc = Encoder.init(interBuf);
            enc.writeInt(u8, w.blockCell);
            enc.writeIntBytes(w.size, self.parsed.items[0]);
        } else {
            interBuf = try fba.alloc(u8, 1 + w.size * self.parsed.items.len);
            var enc = Encoder.init(interBuf);
            _ = enc.writeInt(u8, w.block);
            for (self.parsed.items) |n| _ = enc.writeIntBytes(w.size, n);
        }

        const encodedLens = try self.encodeCompressed(fba, interBuf);
        defer fba.free(encodedLens.buf);

        // TODO: implement short path when column values are celled, but outgrow 256 bytes
        const buf = try self.allocator.alloc(u8, lenSum);
        defer self.allocator.free(buf);
        var bufOffset: usize = 0;
        for (self.values.items) |value| {
            @memcpy(buf[bufOffset .. bufOffset + value.len], value);
            bufOffset += value.len;
        }
        const encodedValues = try self.encodeCompressed(self.allocator, buf);
        defer fba.free(encodedValues.buf);

        return std.mem.concat(self.allocator, u8, &[_][]const u8{
            encodedLens.buf[0..encodedLens.len],
            encodedValues.buf[0..encodedValues.len],
        });
    }

    fn encodeCompressed(self: *ValuesEncoder, fba: std.mem.Allocator, src: []u8) !EncodedValue {
        if (src.len < 128) {
            // skip compression, up to 127 can be in a single byte to be compatible with leb128
            // 1 compression kind, 1 len, len of the buf
            const res = try self.allocator.alloc(u8, 2 + src.len);
            var enc = Encoder.init(res);
            enc.writeInt(u8, compressionKindPlain);
            enc.writeInt(u8, @intCast(src.len));
            enc.writeBytes(src);
            return .{ .buf = res, .len = enc.offset };
        }

        const compressSize = try compress.bound(src.len);
        const compressed = try fba.alloc(u8, compressSize);
        defer fba.free(compressed);
        const compressedSize = try compress.compressAuto(compressed, src);

        // 1 compression kind, 10 compressed len via leb128, len of the compressed
        const res = try self.allocator.alloc(u8, 11 + compressedSize);
        var enc = Encoder.init(res);
        enc.writeInt(u8, compressionKindZstd);
        try enc.writeLeb128(compressedSize);
        enc.writeBytes(compressed[0..compressedSize]);
        return .{ .buf = res, .len = enc.offset };
    }

    pub fn encode(self: *ValuesEncoder, values: []const []const u8, columnValues: *ColumnValues) !EncodeValueType {
        if (values.len == 0) {
            return .{ .type = .string, .min = 0, .max = 0 };
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

    fn tryDictEncoding(self: *ValuesEncoder, values: []const []const u8, columnValues: *ColumnValues) !?EncodeValueType {
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

        return .{ .type = .dict, .min = 0, .max = 0 };
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
                .uint16 => try self.buf.appendSlice(self.allocator, &std.mem.toBytes(@as(u16, @intCast(n)))),
                .uint32 => try self.buf.appendSlice(self.allocator, &std.mem.toBytes(@as(u32, @intCast(n)))),
                .uint64 => try self.buf.appendSlice(self.allocator, &std.mem.toBytes(n)),
                else => unreachable,
            }
            const slice = self.buf.items[start..];
            try self.values.append(self.allocator, slice);
        }

        return .{ .type = vt, .min = minVal, .max = maxVal };
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

        return .{ .type = .int64, .min = @bitCast(minVal), .max = @bitCast(maxVal) };
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

            const bits: u64 = @bitCast(n);

            const start = self.buf.items.len;
            try self.buf.appendSlice(self.allocator, &std.mem.toBytes(bits));
            try self.values.append(self.allocator, self.buf.items[start..]);
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

        return .{ .type = .ipv4, .min = minVal, .max = maxVal };
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

        return .{ .type = .timestampIso8601, .min = @bitCast(minVal), .max = @bitCast(maxVal) };
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
fn numbersAreSame(a: []const u64) bool {
    if (a.len == 0) return false;
    const v = a[0];
    for (a[1..]) |x| if (x != v) return false;
    return true;
}

/// ValuesDecoder reverses the encoding done by ValuesEncoder.encodeValues
pub const ValuesDecoder = struct {
    buf: []const u8,
    offset: usize = 0,

    pub fn init(buf: []const u8) ValuesDecoder {
        return .{ .buf = buf };
    }

    /// Read a single value's encoded bytes
    pub fn readValue(self: *ValuesDecoder, len: usize) ![]const u8 {
        if (self.offset + len > self.buf.len) {
            return DecodeError.InsufficientBuffer;
        }
        const result = self.buf[self.offset .. self.offset + len];
        self.offset += len;
        return result;
    }

    /// Decompress a section of encoded data (both plain and zstd formats)
    pub fn decompressSection(allocator: std.mem.Allocator, data: []const u8) ![]u8 {
        if (data.len < 1) return DecodeError.InsufficientBuffer;

        var dec = Decoder.init(data);
        const compressionKind = try dec.readInt(u8);

        if (compressionKind == ValuesEncoder.compressionKindPlain) {
            const len = try dec.readInt(u8);
            const payload = try dec.readBytes(len);
            return try allocator.dupe(u8, payload);
        } else if (compressionKind == ValuesEncoder.compressionKindZstd) {
            // Read the compressed size (leb128 encoded)
            var compressedSize: u64 = 0;
            var shift: u6 = 0;
            while (true) {
                const byte = try dec.readInt(u8);
                compressedSize |= @as(u64, @intCast(byte & 0x7f)) << shift;
                if ((byte & 0x80) == 0) break;
                shift +|= 7;
            }

            const compressedPayload = try dec.readBytes(compressedSize);

            // Get the decompressed size from the frame
            const decompressedSize = compress.getFrameContentSize(compressedPayload) catch |err| {
                return switch (err) {
                    compress.DecompressError.Unknown => error.InvalidCompression,
                    else => error.InvalidCompression,
                };
            };

            // Allocate buffer for decompressed data
            const decompressed = try allocator.alloc(u8, decompressedSize);
            errdefer allocator.free(decompressed);

            const actualSize = compress.decompress(decompressed, compressedPayload) catch {
                return error.InvalidCompression;
            };

            // Return only the actual decompressed portion
            return decompressed[0..actualSize];
        } else {
            return error.InvalidCompression;
        }
    }

    /// Decode full encodeValues output and return decoded values
    /// Returns array of decoded string representations
    pub fn decodeValues(allocator: std.mem.Allocator, data: []const u8) !std.ArrayList([]const u8) {
        var result = try std.ArrayList([]const u8).initCapacity(allocator, 10);
        errdefer result.deinit(allocator);

        var dec = ValuesDecoder.init(data);

        // Read and decompress lengths section
        const lengthsHeader = try dec.readValue(2);
        const lengthsSize: usize = lengthsHeader[1];

        dec.offset = 0;
        const lengthsSectionData = try dec.readValue(2 + lengthsSize);
        const decompressedLengths = try decompressSection(allocator, lengthsSectionData);
        defer allocator.free(decompressedLengths);

        // Parse lengths from decompressed data
        var lengthsDec = Decoder.init(decompressedLengths);
        const blockTypeOrCell = try lengthsDec.readInt(u8);

        var valueLengths = try std.ArrayList(u64).initCapacity(allocator, 10);
        defer valueLengths.deinit(allocator);

        const isCellEncoded = blockTypeOrCell >= 4;

        if (isCellEncoded) {
            // Cell encoding: all values same size, read one value size
            const cellSize = switch (blockTypeOrCell) {
                4 => @as(usize, 1), // uint8 cell
                5 => @as(usize, 2), // uint16 cell
                6 => @as(usize, 4), // uint32 cell
                7 => @as(usize, 8), // uint64 cell
                else => return error.InvalidEncoding,
            };
            _ = try lengthsDec.readBytes(cellSize); // read the cell value size
            // Can't determine count from cell encoding alone, will be determined by values section
        } else {
            // Block encoding: each value has its size
            const valueSize = switch (blockTypeOrCell) {
                0 => @as(usize, 1), // uint8
                1 => @as(usize, 2), // uint16
                2 => @as(usize, 4), // uint32
                3 => @as(usize, 8), // uint64
                else => return error.InvalidEncoding,
            };

            while (lengthsDec.offset < decompressedLengths.len) {
                const lenBytes = lengthsDec.readBytes(valueSize) catch break;
                var buf: [8]u8 = undefined;
                @memcpy(buf[0..valueSize], lenBytes);
                const lenVal: u64 = switch (valueSize) {
                    1 => buf[0],
                    2 => std.mem.bytesAsValue(u16, buf[0..2]).*,
                    4 => std.mem.bytesAsValue(u32, buf[0..4]).*,
                    8 => std.mem.bytesAsValue(u64, buf[0..8]).*,
                    else => 0,
                };
                try valueLengths.append(allocator, lenVal);
            }
        }

        // Read and decompress values section
        const valuesSectionStart = 2 + lengthsSize;
        if (valuesSectionStart >= data.len) return error.InvalidEncoding;

        const valuesSectionHeader = data[valuesSectionStart..@min(valuesSectionStart + 2, data.len)];
        const valuesSectionSize = if (valuesSectionHeader.len > 1) valuesSectionHeader[1] else 0;

        var valuesDec2 = ValuesDecoder.init(data[valuesSectionStart..]);
        const valuesSectionData = try valuesDec2.readValue(2 + valuesSectionSize);
        const decompressedValues = try decompressSection(allocator, valuesSectionData);
        defer allocator.free(decompressedValues);

        // Extract individual values
        var valuesOffset: usize = 0;
        if (isCellEncoded) {
            // For cell encoding, determine cell size from block type
            const cellSize = switch (blockTypeOrCell) {
                4 => @as(usize, 1),
                5 => @as(usize, 2),
                6 => @as(usize, 4),
                7 => @as(usize, 8),
                else => return error.InvalidEncoding,
            };
            // Split values by cell size
            while (valuesOffset + cellSize <= decompressedValues.len) {
                const valueBuf = decompressedValues[valuesOffset .. valuesOffset + cellSize];
                const owned = try allocator.dupe(u8, valueBuf);
                try result.append(allocator, owned);
                valuesOffset += cellSize;
            }
        } else {
            for (valueLengths.items) |len| {
                if (valuesOffset + len > decompressedValues.len) break;
                const valueBuf = decompressedValues[valuesOffset .. valuesOffset + len];
                const owned = try allocator.dupe(u8, valueBuf);
                try result.append(allocator, owned);
                valuesOffset += len;
            }
        }

        return result;
    }
};

test "ValuesEncoder.encode" {
    const allocator = std.testing.allocator;
    const Case = struct {
        values: []const []const u8,
        expectedType: EncodeValueType,
        expectedColumnValues: []const []const u8,
    };

    const cases = [_]Case{
        .{
            .values = &[_][]const u8{ "0", "1", "2", "3", "4", "5", "6", "7", "8" },
            .expectedType = .{ .min = 0, .max = 8, .type = .uint8 },
            .expectedColumnValues = &[_][]const u8{},
        },
    };

    for (cases) |case| {
        const encoder = try ValuesEncoder.init(allocator);
        defer encoder.deinit();

        var cv = try ColumnValues.init(allocator);
        defer cv.deinit(allocator);

        const valueType = encoder.encode(case.values, &cv);
        try std.testing.expectEqual(case.expectedType, valueType);
        try std.testing.expectEqualSlices([]const u8, case.expectedColumnValues, cv.values.items);
        for (0..case.expectedColumnValues.len) |i| {
            try std.testing.expectEqualSlices(u8, case.expectedColumnValues[i], cv.values.items[i]);
        }

        const encoded = try encoder.encodeValues();
        const buf = try allocator.alloc(u8, encoded.len);
        defer allocator.free(buf);
        var decoder = ValuesDecoder.init(buf);
        const decoded = try decoder.decodeValues(encoded);
        try std.testing.expectEqualSlices([]const u8, case.values, decoded.items);
    }
}
