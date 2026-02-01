/// TagRecordsParseState: Parses and encodes tagToSids index records.
///
/// Use cases:
/// - Parsing raw index bytes into structured fields (tenantID, tag, streamIDs)
/// - Re-encoding merged records back to binary format
///
/// Constraints:
/// - Input must be a valid tagToSids record: kind(1) + tenantID(16) + encodedTag + streamIDs
/// - setup() modifies the input buffer in-place for tag unescaping
/// - parseStreamIDs() must be called to populate streamIDs list from streamsRaw
/// - Each streamID is 16 bytes (u128)
const std = @import("std");
const Allocator = std.mem.Allocator;

const encoding = @import("encoding");
const Encoder = encoding.Encoder;
const Decoder = encoding.Decoder;

const IndexKind = @import("Index.zig").IndexKind;

const maxTenantIDLen = @import("../lines.zig").maxTenantIDLen;
const Field = @import("../lines.zig").Field;

const Self = @This();

streamIDs: std.ArrayList(u128) = .empty,
tenantID: []const u8 = undefined,
tag: Field = undefined,
streamsRaw: []const u8 = undefined,

pub fn init(alloc: Allocator) !*Self {
    const s = try alloc.create(Self);
    s.* = .{};
    return s;
}

pub fn deinit(self: *Self, alloc: Allocator) void {
    self.streamIDs.deinit(alloc);
    alloc.destroy(self);
}

pub fn setup(self: *Self, item: []const u8) !void {
    const kind = item[0];
    const tenantOffset = 1 + maxTenantIDLen;
    self.tenantID = item[1..tenantOffset];

    std.debug.assert(kind == @intFromEnum(IndexKind.tagToSids));

    // We need to modify the buffer in-place for unescaping
    // This is safe because we're only unescaping (making it shorter)
    const tagPortion = @constCast(item[tenantOffset..]);
    const offset = self.tag.decodeIndexTag(tagPortion);

    self.streamsRaw = item[tenantOffset + offset ..];
}

pub fn streamsLen(self: *const Self) usize {
    return self.streamsRaw.len / 16;
}

pub fn parseStreamIDs(self: *Self, alloc: Allocator) !void {
    if (self.streamsRaw.len == 0) {
        return;
    }
    const n = self.streamsRaw.len / 16;
    try self.streamIDs.ensureUnusedCapacity(alloc, n);
    for (0..n) |i| {
        const idBuf = self.streamsRaw[i * 16 .. (i + 1) * 16];
        var dec = Decoder.init(idBuf);
        const v = dec.readInt(u128);
        self.streamIDs.appendAssumeCapacity(v);
    }
    // it's a slice from the item, so it's safe to override the len;
    self.streamsRaw.len = 0;
}

pub fn encodePrefixBound(self: *const Self) usize {
    return 1 + maxTenantIDLen + self.tag.encodeIndexTagBound();
}

pub fn encodePrefix(self: *const Self, dst: []u8) void {
    dst[0] = @intFromEnum(IndexKind.tagToSids);
    var enc = Encoder.init(dst[1..]);
    enc.writePadded(self.tenantID, maxTenantIDLen);
    _ = self.tag.encodeIndexTag(enc.buf[enc.offset..]);
}

pub fn encodeRecordBound(tag: Field, streamIDsLen: usize) usize {
    return 1 + maxTenantIDLen + tag.encodeIndexTagBound() + streamIDsLen * @sizeOf(u128);
}

pub fn encodeRecord(buf: []u8, tenantID: []const u8, tag: Field, streamIDs: []const u128) usize {
    var enc = Encoder.init(buf);

    enc.writeInt(u8, @intFromEnum(IndexKind.tagToSids));
    enc.writePadded(tenantID, maxTenantIDLen);
    const tagOffset = tag.encodeIndexTag(enc.buf[enc.offset..]);
    var streamEnc = Encoder.init(enc.buf[enc.offset + tagOffset ..]);
    for (streamIDs) |sid| {
        streamEnc.writeInt(u128, sid);
    }
    return 1 + maxTenantIDLen + tagOffset + streamEnc.offset;
}

const testing = std.testing;

test "setup parses tag record" {
    const alloc = testing.allocator;
    const state = try Self.init(alloc);
    defer state.deinit(alloc);

    const tag = Field{ .key = "env", .value = "prod" };
    const streamIDs = &[_]u128{ 100, 200 };
    const bufSize = encodeRecordBound(tag, streamIDs.len);
    const buf = try alloc.alloc(u8, bufSize);
    defer alloc.free(buf);
    const totalLen = encodeRecord(buf, "tenant1", tag, streamIDs);

    try state.setup(buf[0..totalLen]);

    try testing.expectEqualStrings("tenant1", std.mem.trimRight(u8, state.tenantID, &[_]u8{0}));
    try testing.expectEqualStrings("env", state.tag.key);
    try testing.expectEqualStrings("prod", state.tag.value);
    try testing.expectEqual(@as(usize, 2), state.streamsLen());

    // Test parsing stream IDs
    try state.parseStreamIDs(alloc);

    try testing.expectEqual(@as(usize, 2), state.streamIDs.items.len);
    try testing.expectEqual(@as(u128, 100), state.streamIDs.items[0]);
    try testing.expectEqual(@as(u128, 200), state.streamIDs.items[1]);

    // Test encodePrefix
    const prefixLen = state.encodePrefixBound();
    var outBuf: [128]u8 = undefined;
    state.encodePrefix(&outBuf);

    try testing.expectEqualSlices(u8, buf[0..prefixLen], outBuf[0..prefixLen]);
}

test "parseStreamIDs empty" {
    const alloc = testing.allocator;
    const state = try Self.init(alloc);
    defer state.deinit(alloc);

    const tag = Field{ .key = "k", .value = "v" };
    const streamIDs = &[_]u128{};
    const bufSize = encodeRecordBound(tag, streamIDs.len);
    const buf = try alloc.alloc(u8, bufSize);
    defer alloc.free(buf);
    const totalLen = encodeRecord(buf, "t", tag, streamIDs);

    try state.setup(buf[0..totalLen]);
    try state.parseStreamIDs(alloc);

    try testing.expectEqual(@as(usize, 0), state.streamIDs.items.len);
}
