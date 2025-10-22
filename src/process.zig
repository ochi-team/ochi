const std = @import("std");

const Store = @import("store.zig").Store;

pub const Field = struct {
    key: []const u8,
    value: []const u8,
};

pub const Params = struct {
    tenantID: []const u8,
};

// sid defines stream id,
pub const SID = struct {
    tenantID: []const u8,
    // low and high describe a stream id itself,
    // a hash from encoded stream fields
    // split to 2 u64 to align data layout to 8
    low: u64,
    high: u64,
};

pub const Line = struct {
    timestampNs: u64,
    tenantID: []const u8,
    streamFields: []const Field,
    fields: []const Field,

    pub fn makeEncodedStream(self: *const Line, allocator: std.mem.Allocator) ![][]const u8 {
        // TODO: the implementation is fake, fix it
        // it also may required changing the structure in future, now it's a slice of slices,
        // because it allows not to copy the underlying key/value
        const encoded = try allocator.alloc([]const u8, self.streamFields.len * 2);
        var i: u16 = 0;
        for (self.streamFields) |f| {
            encoded[i] = f.key;
            i += 1;
            encoded[i] = f.value;
            i += 1;
        }

        return encoded;
    }

    pub fn makeStreamID(self: *const Line, encodedStream: [][]const u8) SID {
        // TODO: implement, calculate the fastest hash from encodedStream
        return SID{
            .tenantID = self.tenantID,
            .low = encodedStream.len,
            .high = 1,
        };
    }
};

pub const Lines = std.ArrayList(*const Line);

pub const LinesToDay = std.AutoHashMap(u64, Lines);

const dayNs: u64 = std.time.ns_per_day;

// TODO: make it configurable
const retention: u64 = 30 * std.time.ns_per_day;

fn sortStreamFields(_: void, one: Field, another: Field) bool {
    return std.mem.order(u8, one.key, another.key) == .lt;
}

pub const Processor = struct {
    lines: [1]Line,

    store: *Store,

    pub fn init(allocator: std.mem.Allocator, store: *Store) !*Processor {
        const processor = try allocator.create(Processor);
        processor.store = store;
        return processor;
    }
    pub fn deinit(self: *Processor, allocator: std.mem.Allocator) void {
        allocator.destroy(self);
    }

    pub fn pushLine(self: *Processor, _: std.mem.Allocator, timestampNs: u64, fields: []const Field, params: Params) !void {
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

        const streamFields = fields[0 .. fields.len - 1]; // -1 cuts _msg off
        // use unstable sort because we don't expect duplicated keys
        std.mem.sortUnstable(Field, @constCast(streamFields), {}, sortStreamFields);

        const line = Line{
            .timestampNs = timestampNs,
            .tenantID = params.tenantID,
            .streamFields = streamFields,
            .fields = fields[fields.len - 1 ..],
        };
        self.lines[0] = line;
    }
    pub fn mustFlush(_: *Processor) bool {
        return true;
    }
    pub fn flush(self: *Processor, allocator: std.mem.Allocator) !void {
        // TODO: add to hot partition if possible

        const now: u64 = @intCast(std.time.nanoTimestamp());
        const minDay = (now - retention) / dayNs;

        var linesByInterval = LinesToDay.init(allocator);
        for (self.lines) |line| {
            const day = line.timestampNs / dayNs;
            if (day < minDay) {
                // TODO: log a warning
                // TODO: produce a metric to understand if its worth to validate,
                // perhaps easier to insert and clean after
                continue;
            }

            if (linesByInterval.getPtr(day)) |list| {
                try list.append(allocator, &line);
                continue;
            }
            var list = try Lines.initCapacity(allocator, self.lines.len);
            try linesByInterval.put(day, list);
            try list.append(allocator, &line);
        }

        try self.store.addLines(allocator, linesByInterval);
    }
};
