const std = @import("std");

const Encoder = @import("encoding").Encoder;
const Decoder = @import("encoding").Decoder;

const sizing = @import("inmem/sizing.zig");

pub const SID = struct {
    // TODO: make it [16]const u8
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

    pub fn encode(self: *SID, enc: *Encoder) void {
        if (self.tenantID.len > 16) {
            @panic("tenant id can't be larger than 16 bytes");
        }

        enc.writePadded(self.tenantID, 16);
        enc.writeInt(u128, self.id);
    }

    pub fn decode(buf: []const u8) !SID {
        if (buf.len < 32) {
            return error.InsufficientBuffer;
        }
        var decoder = Decoder.init(buf);
        const tenantID = try decoder.readPadded(16);
        const id = try decoder.readInt(u128);
        return .{
            .tenantID = tenantID,
            .id = id,
        };
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
