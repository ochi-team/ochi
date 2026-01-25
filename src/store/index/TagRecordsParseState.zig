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

