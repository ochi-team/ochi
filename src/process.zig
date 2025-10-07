const std = @import("std");

pub const Field = struct {
    name: []const u8,
    value: []const u8,
};

pub const Params = struct {
    tenant: Tenant,
    // TODO: consider using []const u8
    streamFields: ?std.StringHashMap(void),
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
    encodedFields: std.ArrayList([]const u8),

    pub fn init(allocator: std.mem.Allocator) *Processor {
        const processor = allocator.create(*Processor);
        processor.* = Processor{
            .encodedFields = std.ArrayList([]u8).initCapacity(allocator, 0),
        };
        return processor;
    }

    pub fn pushLine(self: *Processor, allocator: std.mem.Allocator, timestamp: i128, fields: []const Field, params: Params) !void {
        // TODO: controll how many fields a single line may contain
        // add a config value and validate fields length
        // 1000 is a default limit

        var streamFields = try std.ArrayList(Field).initCapacity(allocator, fields.len + params.extraStreamFields.len);
        errdefer streamFields.deinit(allocator);

        // TODO: consider storing stream fields in a flat buffer pre-encoded
        if (params.streamFields != null) {
            for (fields) |f| {
                if (params.streamFields.?.contains(f.name)) {
                    try streamFields.append(allocator, .{ .name = f.name, .value = f.value });
                }
            }
        }

        for (params.extraStreamFields) |f| {
            try streamFields.append(allocator, .{ .name = f.name, .value = f.value });
        }

        // TODO: encode stream fields
        const encodedStreamFields: []const u8 = undefined;
        // TODO: create sid calculating hash
        const sid = SID{ .id = 0, .tenant = params.tenant };
        // const sid = SID{.id = hash(encoded), .tenant = params.tenant};
        // TODO: add sid using addStreamID
        try self.addStreamID(allocator, sid, timestamp, fields, encodedStreamFields);
    }
    fn addStreamID(self: *Processor, allocator: std.mem.Allocator, _: SID, _: i128, _: []const Field, encodedStreamFields: []const u8) !void {
        // TODO: benchmark if we can copy only a pointer instead of the entire value
        if (std.mem.eql(u8, self.encodedFields.items[self.encodedFields.items.len], encodedStreamFields)) {
            try self.encodedFields.append(allocator, self.encodedFields.items[self.encodedFields.items.len]);
        } else {
            try self.encodedFields.append(allocator, encodedStreamFields);
        }
        // TODO: append timestamp
        // TODO: append sid
        // TODO: append fields
        // TODO: optionally append default _msg value if params.defaultMsgValue has it
        // TODO: append []fields to eventual set of log lines, [][]Field}
    }
    pub fn flush(_: *Processor) !void {}
};
