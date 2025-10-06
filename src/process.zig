const std = @import("std");

pub const Field = struct {
    name: []const u8,
    value: []const u8,
};

pub const Params = struct {
    tenant: Tenant,
    streamFields: std.StringHashMap(void),
    extraStreamFields: []const Field,
};

// Tenant defines a tenant id model
// TODO: implement its usage, not it's a placeholder and ever empty
// for Loki it's a header X-Scope-OrgID
pub const Tenant = struct {
    accountID: u32,
    projectID: u32,
};

const SID = struct {
    id: u128,
    tenant: Tenant,
};

pub const Processor = struct {
    pub fn pushLine(self: *Processor, allocator: std.mem.Allocator, timestamp: i128, fields: []const Field, params: []const Params) !void {
        // TODO: controll how many fields a single line may contain
        // add a config value and validate fields length
        // 1000 is a default limit

        const streamFields = std.ArrayList(Field).initCapacity(allocator, fields.len + params.extraStreamFields.len);
        errdefer streamFields.deinit();

        // TODO: consider storing stream fields in a flat buffer pre-encoded
        for (fields) |f| {
            if (params.streamFields.contains(f.name)) {
                try streamFields.add(f.name, f.value);
            }
        }

        for (params.extraStreamFields) |f| {
            try streamFields.add(f.name, f.value);
        }

        // TODO: encode stream fields
        const encodedStreamFields: []const u8 = undefined;
        // TODO: create sid calculating hash
        const sid = SID{};
        // const sid = SID{.id = hash(encoded), .tenant = params.Tenant};
        // TODO: add sid using addStreamID
        self.addStreamID(allocator, sid, timestamp, fields, encodedStreamFields);
    }
    fn addStreamID(_: *Processor, _: std.mem.Allocator, _: SID, _: i128, _: []const Field, _: []const u8) !void {}
    pub fn flush(_: *Processor) !void {}
};
