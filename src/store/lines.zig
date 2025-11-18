const std = @import("std");

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

    pub fn encode(self: *SID, buf: *std.ArrayList(u8)) !void {
        if (self.tenantID.len > 16) {
            @panic("tenant id can't be larger than 16 bytes");
        }

        if (buf.capacity - buf.items.len < 16) unreachable;
        const tenantBuf = buf.unusedCapacitySlice()[0..16];
        @memset(tenantBuf, 0x00);
        @memcpy(tenantBuf[0..self.tenantID.len], self.tenantID);
        buf.items.len += 16;

        var intBuf: [16]u8 = @bitCast(self.id);
        try buf.appendSliceBounded(&intBuf);
    }
};

pub const Field = struct {
    key: []const u8,
    value: []const u8,
};

pub const Line = struct {
    timestampNs: u64,
    sid: SID,
    fields: []Field,
    encodedTags: [][]const u8,

    pub fn fieldsLen(self: *const Line) u32 {
        // TODO: implement real calculation depending on the format we store data in
        var res: u32 = 0;
        for (self.fields) |field| {
            res += @intCast(field.key.len);
            res += @intCast(field.value.len);
        }
        return res;
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
