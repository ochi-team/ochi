const std = @import("std");
const zeit = @import("zeit");

const Encoder = @import("encoding").Encoder;
const Decoder = @import("encoding").Decoder;

const sizing = @import("data/sizing.zig");

pub const timestampKey = "_time";
pub const msgKey = "_msg";

// TODO: if we don't use decoding of it perhaps we don't need to encode lens
pub fn encodeTags(allocator: std.mem.Allocator, tags: []const Field) ![]u8 {
    var size: usize = Encoder.varIntBound(tags.len);
    for (tags) |tag| {
        size += Encoder.varIntBound(tag.key.len) + Encoder.varIntBound(tag.value.len);
        size += tag.key.len + tag.value.len;
    }
    const buf = try allocator.alloc(u8, size);
    errdefer allocator.free(buf);

    var enc = Encoder.init(buf);
    enc.writeVarInt(tags.len);
    for (tags) |tag| {
        enc.writeString(tag.key);
        enc.writeString(tag.value);
    }

    std.debug.assert(enc.offset == buf.len);

    return buf;
}
const magicStr = "xxhash";
pub fn makeStreamID(tenantID: u64, encodedStream: []const u8) SID {
    var hasher = std.hash.XxHash64.init(0);
    hasher.update(encodedStream);
    const first = hasher.final();
    hasher.update(magicStr);
    const second = hasher.final();
    const id = @as(u128, first) << 64 | second;

    return SID{
        .tenantID = tenantID,
        .id = id,
    };
}

pub const SID = struct {
    tenantID: u64,
    // TODO: didn't measure overhead yet, but perhaps worth making it as 2 u64 to reduce the size
    // making it as a U128 struct with 2 u64 also works
    id: u128,

    pub fn order(one: SID, another: SID) std.math.Order {
        if (one.lessThan(another)) {
            return .lt;
        } else if (one.eql(another)) {
            return .eq;
        }

        return .gt;
    }

    pub inline fn eql(self: SID, another: SID) bool {
        return self.tenantID == another.tenantID and self.id == another.id;
    }

    pub fn sortLessThan(_: void, one: SID, another: SID) bool {
        return one.lessThan(another);
    }

    pub inline fn lessThan(self: SID, another: SID) bool {
        return self.tenantID < another.tenantID or
            (self.tenantID == another.tenantID and self.id < another.id);
    }

    pub const encodeBound = 24; // 8 bytes (u64) + 16 bytes (u128)
    pub fn encode(self: *const SID, enc: *Encoder) void {
        enc.writeInt(u64, self.tenantID);
        enc.writeInt(u128, self.id);
    }

    pub fn encodeTenantWithPrefix(self: *const SID, enc: *Encoder, prefix: u8) void {
        enc.writeInt(u8, prefix);
        enc.writeInt(u64, self.tenantID);
    }

    pub fn decode(buf: []const u8) SID {
        var decoder = Decoder.init(buf);
        const tenantID = decoder.readInt(u64);
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

pub const LinesSize = struct {
    size: u16,
};
pub const defaultMaxFieldsPerLine: usize = 1024;
pub const defaultMaxFieldKeySize: usize = 128;
pub const defaultMaxLineSize: usize = 32 * 1024 - 1;

pub const LinesSizeError = error{
    MaxFieldsPerLineExceeded,
    MaxFieldKeySizeExceeded,
    MaxLineSizeExceeded,
};

// TODO: it's a common case to copy fields, move it to lines,
// same happens in the processor and data shard
pub fn copyFields(alloc: std.mem.Allocator, fields: []const Field) ![]Field {
    const copiedFields = try alloc.alloc(Field, fields.len);
    var copied: usize = 0;
    errdefer {
        for (copiedFields[0..copied]) |field| {
            alloc.free(field.key);
            alloc.free(field.value);
        }
        alloc.free(copiedFields);
    }

    for (fields, 0..) |field, i| {
        const key = try alloc.dupe(u8, field.key);
        errdefer alloc.free(key);
        const value = try alloc.dupe(u8, field.value);
        errdefer alloc.free(value);

        copiedFields[i] = .{ .key = key, .value = value };
        copied += 1;
    }

    return copiedFields;
}

pub fn freeFields(alloc: std.mem.Allocator, fields: []Field) void {
    for (fields) |field| {
        alloc.free(field.key);
        alloc.free(field.value);
    }
    alloc.free(fields);
}

pub fn deinitLinesFull(alloc: std.mem.Allocator, lines: *std.ArrayList(Line)) void {
    for (lines.items) |line| {
        freeFields(alloc, line.fields);
    }
    lines.deinit(alloc);
}

// Line is an internal representation of a log line,
pub const Line = struct {
    timestampNs: u64,
    // field.key can be empty meaning it's a message field (msgKey by fefault in the API)
    // can't be const because we reorder fields
    fields: []Field,

    pub fn stringifyJSON(self: Line, jw: *std.json.Stringify) !void {
        const inst: zeit.Instant = .{ .timestamp = self.timestampNs, .timezone = &zeit.utc };
        var timeBuf: [32]u8 = undefined;
        const timestamp = try inst.time().bufPrint(&timeBuf, .rfc3339);

        try jw.beginObject();
        try jw.objectField(timestampKey);
        try jw.write(timestamp);
        for (self.fields) |field| {
            if (field.value.len == 0) {
                continue;
            }

            const key = if (field.key.len == 0) msgKey else field.key;
            try jw.objectField(key);
            try jw.write(field.value);
        }
        try jw.endObject();
    }

    pub fn rawSize(self: Line) u32 {
        var res: u32 = 0;
        for (self.fields) |field| {
            res += @intCast(field.key.len);
            res += @intCast(field.value.len);
        }
        return res;
    }

    pub fn fieldsSize(self: Line) u32 {
        return sizing.fieldsJsonSize(self);
    }
};

pub fn rawFieldsSizeValidate(fields: []const Field) LinesSizeError!LinesSize {
    if (fields.len > defaultMaxFieldsPerLine) {
        return error.MaxFieldsPerLineExceeded;
    }

    var res: u32 = 0;
    for (fields) |field| {
        if (field.key.len > defaultMaxFieldKeySize) {
            return error.MaxFieldKeySizeExceeded;
        }

        res += @intCast(field.key.len);
        res += @intCast(field.value.len);
    }

    if (res > defaultMaxLineSize) {
        return error.MaxLineSizeExceeded;
    }

    return .{
        .size = @intCast(res),
    };
}

pub fn putJsonArrayLines(jw: *std.json.Stringify, lines: []const Line) !void {
    try jw.beginArray();
    for (lines) |line| {
        try line.stringifyJSON(jw);
    }
    try jw.endArray();
}

pub fn writeLines(writer: *std.Io.Writer, lines: []const Line) !void {
    var jw: std.json.Stringify = .{ .writer = writer };
    try putJsonArrayLines(&jw, lines);
}

// TODO: we might want to introduce BlockLines type that holds a struct similar to
// Block state: slice of timestamps, slice of columns, and slice of all the fields,
// it allows us to allocate slice of fields all at once instead of per Line,
// if this type gets wider usage worth naming it Lines instead,
// the difference to MultiArray is []Fields is also a flat array for all the lines

/// this is a default ingestion sorting
pub fn lineLessThan(_: void, one: Line, another: Line) bool {
    return one.timestampNs < another.timestampNs;
}

/// this is a sorting for querying, latest lines are on top of the view
pub fn lineLatestFirst(_: void, a: Line, b: Line) bool {
    return a.timestampNs > b.timestampNs;
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

test "Line.stringifyJSON emits query response line object" {
    const Case = struct {
        timestampNs: u64,
        fields: []Field,
        expected: []const u8,
    };

    var normalFields = [_]Field{
        .{ .key = "x", .value = "y" },
        .{ .key = "", .value = "hello" },
        .{ .key = "skip", .value = "" },
    };
    var escapedFields = [_]Field{
        .{ .key = "quote\"key", .value = "line\nvalue" },
        .{ .key = "slash\\key", .value = "tab\tvalue" },
    };

    const cases = [_]Case{
        .{
            .timestampNs = 0,
            .fields = normalFields[0..],
            .expected = "{\"" ++ timestampKey ++ "\":\"1970-01-01T00:00:00.000Z\",\"x\":\"y\",\"" ++ msgKey ++ "\":\"hello\"}",
        },
        .{
            .timestampNs = 1_234_567_890,
            .fields = escapedFields[0..],
            .expected = "{\"" ++ timestampKey ++ "\":\"1970-01-01T00:00:01.234Z\",\"quote\\\"key\":\"line\\nvalue\",\"slash\\\\key\":\"tab\\tvalue\"}",
        },
    };

    for (cases) |case| {
        var writer = try std.Io.Writer.Allocating.initCapacity(testing.allocator, 128);
        defer writer.deinit();

        var jw: std.json.Stringify = .{ .writer = &writer.writer };
        const line = Line{ .timestampNs = case.timestampNs, .fields = case.fields };
        try line.stringifyJSON(&jw);

        try testing.expectEqualStrings(case.expected, writer.written());
    }
}
