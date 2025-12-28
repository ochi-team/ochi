const std = @import("std");

const Encoder = @import("encoding").Encoder;
const Decoder = @import("encoding").Decoder;

const sizing = @import("inmem/sizing.zig");

const maxTenantIDLen = 16;

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

pub const Field = struct {
    key: []const u8,
    value: []const u8,
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
