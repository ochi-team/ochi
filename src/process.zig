const std = @import("std");

pub const Field = struct {
    key: []const u8,
    value: []const u8,
};

pub const Params = struct {
    tenant: Tenant,
};

// Tenant defines a tenant id model
pub const Tenant = struct {
    // id is a tenant id, limited to 16 symbols
    id: []const u8,
};

const SID = struct {
    id: u128,
    tenant: Tenant,
};

pub const Processor = struct {
    pub fn init(allocator: std.mem.Allocator) !*Processor {
        const processor = try allocator.create(Processor);
        return processor;
    }
    pub fn deinit(self: *Processor, allocator: std.mem.Allocator) void {
        allocator.destroy(self);
    }

    pub fn pushLine(self: *Processor, allocator: std.mem.Allocator, timestamp: i128, fields: []const Field, params: Params) !void {
        // TODO: controll how many fields a single line may contain
        // add a config value and validate fields length
        // 1000 is a default limit

        // TODO: add an option  to accept stream fields, so as not to put to stream all the fields
        // it requires 2 fields:
        // streamFields: list of keys to retrieve from fields to identify as a stream
        // presetStream: list of fields to append to streamFields
        // const streamFields = fields[0 .. fields.len - 2]; // -1 is _msg, -2 cuts _msg off

        // TODO: add an option to accep extra stream fields

        // TODO: add an option to accep extra fields

        // TODO: add an option to accept ignore fields
        // doesn't impact stream fields, to narrow set of stream fields better to use stream fields option

        // const encodedStreamFields = encodeStream(streamFields);
        // const sid = hash(encodedStream);
        const encodedStreamFields: []const u8 = undefined;
        const sid = SID{ .id = 0, .tenant = params.tenant };

        try self.addStreamID(allocator, sid, timestamp, fields, encodedStreamFields);
    }
    fn addStreamID(self: *Processor, allocator: std.mem.Allocator, sid: SID, ts: i128, fields: []const Field, encodedStreamFields: []const u8) !void {
        _ = self;
        _ = allocator;
        _ = sid;
        _ = ts;
        _ = fields;
        _ = encodedStreamFields;
    }
    pub fn flush(_: *Processor) !void {}
};
