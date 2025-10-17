const std = @import("std");

pub const Field = struct {
    key: []const u8,
    value: []const u8,
};

pub const Params = struct {
    tenantID: []const u8,
};

pub const Line = struct {
    timestamp: i128,
    tenantID: []const u8,
    streamFields: []const Field,
    fields: []const Field,
};

pub const Processor = struct {
    lines: [1]Line,

    pub fn init(allocator: std.mem.Allocator) !*Processor {
        const processor = try allocator.create(Processor);
        return processor;
    }
    pub fn deinit(self: *Processor, allocator: std.mem.Allocator) void {
        allocator.destroy(self);
    }

    pub fn pushLine(self: *Processor, _: std.mem.Allocator, timestamp: i128, fields: []const Field, params: Params) !void {
        // TODO: controll how many fields a single line may contain
        // add a config value and validate fields length
        // 1000 is a default limit

        // TODO: add an option  to accept stream fields, so as not to put to stream all the fields
        // it requires 2 fields:
        // streamFields: list of keys to retrieve from fields to identify as a stream
        // presetStream: list of fields to append to streamFields

        // TODO: add an option to accep extra stream fields

        // TODO: add an option to accep extra fields

        // TODO: add an option to accept ignore fields
        // doesn't impact stream fields, to narrow set of stream fields better to use stream fields option

        const line = Line{
            .timestamp = timestamp,
            .tenantID = params.tenantID,
            .streamFields = fields[0 .. fields.len - 1], // -1 cuts _msg off
            .fields = fields[fields.len - 1 ..],
        };
        self.lines[0] = line;
    }
    pub fn mustFlush(_: *Processor) bool {
        return true;
    }
    pub fn flush(_: *Processor) !void {}
};
