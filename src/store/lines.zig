const std = @import("std");

const Encoder = @import("encoding").Encoder;
const Decoder = @import("encoding").Decoder;

const sizing = @import("inmem/sizing.zig");

pub const maxTenantIDLen = 16;

pub const SID = struct {
    tenantID: []const u8,
    id: u128,

    pub fn eql(self: *const SID, another: *const SID) bool {
        return std.mem.eql(u8, self.tenantID, another.tenantID) and
            self.id == another.id;
    }

    pub fn lessThan(self: *const SID, another: *const SID) bool {
        // tenant is less
        return std.mem.lessThan(u8, self.tenantID, another.tenantID) or
            // or if tenant is eq than id is less
            (std.mem.eql(u8, self.tenantID, another.tenantID) and
                self.id < another.id);
    }

    pub const encodeBound = 32;
    pub fn encode(self: *const SID, enc: *Encoder) void {
        enc.writePadded(self.tenantID, maxTenantIDLen);
        enc.writeInt(u128, self.id);
    }

    pub fn decode(buf: []const u8) SID {
        var decoder = Decoder.init(buf);
        const tenantID = decoder.readPadded(maxTenantIDLen);
        const id = decoder.readInt(u128);
        return .{
            .tenantID = tenantID,
            .id = id,
        };
    }
};

const ControlChar = enum(u8) {
    escape = 0,
    tagTerminator = 1,
};

pub const Field = struct {
    key: []const u8,
    value: []const u8,

    pub fn eql(self: Field, another: Field) bool {
        return std.mem.eql(u8, self.key, another.key) and
            std.mem.eql(u8, self.value, another.value);
    }

    pub fn encodeIndexTagBound(self: Field) usize {
        var res = self.key.len + self.value.len;
        res += count(u8, self.key, @intFromEnum(ControlChar.escape));
        res += count(u8, self.key, @intFromEnum(ControlChar.tagTerminator));
        res += count(u8, self.value, @intFromEnum(ControlChar.escape));
        res += count(u8, self.value, @intFromEnum(ControlChar.tagTerminator));
        res += 2; // two terminators
        return res;
    }
    pub fn count(comptime T: type, haystack: []const T, needle: T) usize {
        var i: usize = 0;
        var found: usize = 0;

        while (std.mem.indexOfScalarPos(T, haystack, i, needle)) |idx| {
            i = idx + 1;
            found += 1;
        }

        return found;
    }

    pub fn encodeIndexTag(self: Field, dst: []u8) usize {
        var offset = escapeTag(dst, self.key);
        dst[offset] = @intFromEnum(ControlChar.tagTerminator);
        offset += 1;
        offset += escapeTag(dst[offset..], self.value);
        dst[offset] = @intFromEnum(ControlChar.tagTerminator);
        offset += 1;
        return offset;
    }

    fn escapeTag(dst: []u8, src: []const u8) usize {
        var offset: usize = 0;
        var last: usize = 0;
        for (0..src.len) |i| {
            switch (src[i]) {
                @intFromEnum(ControlChar.escape) => {
                    // Copy everything before the escape char
                    const len = i - last;
                    if (len > 0) {
                        @memcpy(dst[offset..][0..len], src[last..i]);
                        offset += len;
                    }
                    // Write escape sequence: escape char + '0'
                    dst[offset] = @intFromEnum(ControlChar.escape);
                    dst[offset + 1] = '0';
                    offset += 2;
                    last = i + 1;
                },
                @intFromEnum(ControlChar.tagTerminator) => {
                    // Copy everything before the terminator
                    const len = i - last;
                    if (len > 0) {
                        @memcpy(dst[offset..][0..len], src[last..i]);
                        offset += len;
                    }
                    // Write escape sequence: escape char + '1'
                    dst[offset] = @intFromEnum(ControlChar.escape);
                    dst[offset + 1] = '1';
                    offset += 2;
                    last = i + 1;
                },
                else => {},
            }
        }

        if (last < src.len) {
            const remaining = src.len - last;
            @memcpy(dst[offset..][0..remaining], src[last..]);
            offset += remaining;
        }

        return offset;
    }

    pub const UnescapeResult = struct {
        srcConsumed: usize,
        dstWritten: usize,
    };

    pub fn decodeIndexTag(self: *Field, src: []u8) usize {
        const keyResult = unescapeTagValue(src);
        self.key = src[0..keyResult.dstWritten];

        const valueResult = unescapeTagValue(src[keyResult.srcConsumed..]);
        self.value = src[keyResult.srcConsumed..][0..valueResult.dstWritten];

        return keyResult.srcConsumed + valueResult.srcConsumed;
    }

    fn unescapeTagValue(src: []u8) UnescapeResult {
        // Find the terminator
        const n = std.mem.indexOfScalar(u8, src, @intFromEnum(ControlChar.tagTerminator)) orelse {
            std.debug.panic("cannot find tag terminator", .{});
        };

        // Unescape in-place: read from src[0..n], write back to src[0..]
        var readPos: usize = 0;
        var writePos: usize = 0;

        while (readPos < n) {
            const escapeIdx = std.mem.indexOfScalarPos(u8, src[0..n], readPos, @intFromEnum(ControlChar.escape));

            if (escapeIdx == null) {
                // No more escape chars, copy remaining data
                const remaining = n - readPos;
                if (writePos != readPos) {
                    std.mem.copyForwards(u8, src[writePos..][0..remaining], src[readPos..n]);
                }
                writePos += remaining;
                break;
            }

            const idx = escapeIdx.?;
            // Copy data before the escape char
            const chunkLen = idx - readPos;
            if (chunkLen > 0 and writePos != readPos) {
                std.mem.copyForwards(u8, src[writePos..][0..chunkLen], src[readPos..idx]);
            }
            writePos += chunkLen;
            readPos = idx + 1;

            std.debug.assert(readPos < n);

            // Process the escaped character
            switch (src[readPos]) {
                '0' => {
                    src[writePos] = @intFromEnum(ControlChar.escape);
                    writePos += 1;
                },
                '1' => {
                    src[writePos] = @intFromEnum(ControlChar.tagTerminator);
                    writePos += 1;
                },
                else => std.debug.panic("unsupported escape char: {c}", .{src[readPos]}),
            }
            readPos += 1;
        }

        return .{
            .srcConsumed = n + 1, // include the terminator
            .dstWritten = writePos,
        };
    }
};

// Line is an internal representation of a log line,
pub const Line = struct {
    timestampNs: u64,
    sid: SID,
    // field.key can be empty meaning it's a message field (_msg by fefault in the API)
    // can't be const because we reorder fields
    fields: []Field,

    pub fn fieldsSize(self: *const Line) u32 {
        return sizing.fieldsJsonSize(self);
    }
};

pub fn lineLessThan(_: void, one: *const Line, another: *const Line) bool {
    // sid is less
    return one.sid.lessThan(&another.sid) or
        // or sid is eq, but timestamp is less
        (one.sid.eql(&another.sid) and one.timestampNs < another.timestampNs);
}

pub fn fieldLessThan(_: void, one: Field, another: Field) bool {
    return std.mem.lessThan(u8, one.key, another.key);
}

const testing = std.testing;

test "Field.encodeIndexTag" {
    const alloc = testing.allocator;
    const Case = struct {
        key: []const u8,
        value: []const u8,
        expected: []const u8,
    };

    const cases = [_]Case{
        .{
            .key = "key",
            .value = "value",
            .expected = "key\x01value\x01",
        },
        .{
            .key = "ke\x00y",
            .value = "value",
            .expected = "ke\x000y\x01value\x01",
        },
        .{
            .key = "key",
            .value = "val\x01ue",
            .expected = "key\x01val\x001ue\x01",
        },
        .{
            .key = "k\x00e\x01y",
            .value = "v\x01al\x00ue",
            .expected = "k\x000e\x001y\x01v\x001al\x000ue\x01",
        },
    };
    for (cases) |case| {
        const f = Field{ .key = case.key, .value = case.value };
        const bound = f.encodeIndexTagBound();
        const buf = try alloc.alloc(u8, bound);
        defer alloc.free(buf);

        const encodedLen = f.encodeIndexTag(buf);
        try testing.expectEqualSlices(u8, case.expected, buf[0..encodedLen]);

        // Test round-trip: decode and verify we get back the original key/value
        const decodeBuf = try alloc.alloc(u8, bound);
        defer alloc.free(decodeBuf);
        @memcpy(decodeBuf[0..encodedLen], buf[0..encodedLen]);

        var decoded = Field{ .key = "", .value = "" };
        const decodeOffset = decoded.decodeIndexTag(decodeBuf[0..encodedLen]);

        try testing.expectEqualSlices(u8, case.key, decoded.key);
        try testing.expectEqualSlices(u8, case.value, decoded.value);
        try testing.expectEqual(decodeBuf.len, decodeOffset);

        try testing.expect(f.eql(.{ .key = decoded.key, .value = decoded.value }));
    }
}
