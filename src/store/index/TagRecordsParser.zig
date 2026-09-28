/// TagRecordsParser: Parses and encodes tagToSids index records.
///
/// Use cases:
/// - Parsing raw index bytes into structured fields (tenantID, tag, streamIDs)
/// - Re-encoding merged records back to binary format
///
/// Constraints:
/// - Input must be a valid tagToSids record: kind(1) + tenantID(8) + encodedTag + streamIDs
/// - setup() modifies the input buffer in-place for tag unescaping
/// - parseStreamIDs() must be called to populate streamIDs list from streamsRaw
/// - Each streamID is 16 bytes (u128)
const std = @import("std");
const Allocator = std.mem.Allocator;

const encoding = @import("encoding");
const Encoder = encoding.Encoder;
const Decoder = encoding.Decoder;

const IndexKind = @import("Index.zig").IndexKind;

const Field = @import("../lines.zig").Field;

const TagRecordsParser = @This();

streamIDs: std.ArrayList(u128) = .empty,
tenantID: u64 = 0,
tag: Field = undefined,
streamsRaw: []const u8 = undefined,

pub fn deinit(self: *TagRecordsParser, alloc: Allocator) void {
    self.streamIDs.deinit(alloc);
}

pub fn setup(self: *TagRecordsParser, item: []u8) !void {
    self.streamIDs.clearRetainingCapacity();

    const kind = item[0];
    const tenantOffset = 1 + @sizeOf(u64);
    var dec = Decoder.init(item[1..tenantOffset]);
    self.tenantID = dec.readInt(u64);

    std.debug.assert(kind == @intFromEnum(IndexKind.tagToSids));

    // We need to modify the buffer in-place for unescaping
    // This is safe because we're only unescaping (making it shorter)
    const tagPortion = item[tenantOffset..];
    const offset = self.tag.decodeIndexTag(tagPortion);

    self.streamsRaw = item[tenantOffset + offset ..];
}

pub fn setupStreamsRaw(self: *TagRecordsParser, streamsRaw: []const u8) !void {
    self.streamIDs.clearRetainingCapacity();

    self.streamsRaw = streamsRaw;
}

pub fn streamsLen(self: *const TagRecordsParser) usize {
    return self.streamsRaw.len / 16;
}

pub fn parseStreamIDs(self: *TagRecordsParser, alloc: Allocator) !void {
    if (self.streamsRaw.len == 0) {
        return;
    }
    std.debug.assert(self.streamsRaw.len % 16 == 0);
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

pub fn encodePrefixBound(tag: Field) usize {
    return 1 + @sizeOf(u64) + tag.encodeIndexTagBound();
}

pub fn encodePrefix(dst: []u8, tenantID: u64, tag: Field) void {
    dst[0] = @intFromEnum(IndexKind.tagToSids);
    var enc = Encoder.init(dst[1..]);
    enc.writeInt(u64, tenantID);
    _ = tag.encodeIndexTag(enc.buf[enc.offset..]);
}

pub fn encodeRecordBound(tag: Field, streamIDsLen: usize) usize {
    return 1 + @sizeOf(u64) + tag.encodeIndexTagBound() + streamIDsLen * @sizeOf(u128);
}

pub fn encodeRecord(buf: []u8, tenantID: u64, tag: Field, streamIDs: []const u128) usize {
    var enc = Encoder.init(buf);

    enc.writeInt(u8, @intFromEnum(IndexKind.tagToSids));
    enc.writeInt(u64, tenantID);
    const tagOffset = tag.encodeIndexTag(enc.buf[enc.offset..]);
    var streamEnc = Encoder.init(enc.buf[enc.offset + tagOffset ..]);
    for (streamIDs) |sid| {
        streamEnc.writeInt(u128, sid);
    }
    return 1 + @sizeOf(u64) + tagOffset + streamEnc.offset;
}

test {
    _ = @import("TagRecordsParser_test.zig");
}
