const std = @import("std");

const Store = @import("store.zig").Store;
const Field = @import("store/lines.zig").Field;
const Lines = @import("store/lines.zig").Lines;
const Line = @import("store/lines.zig").Line;
const SID = @import("store/lines.zig").SID;

pub const Params = struct {
    tenantID: []const u8,
};

fn encodeTags(allocator: std.mem.Allocator, tags: []const Field) ![][]const u8 {
    // TODO: the implementation is fake, fix it
    // it also may required changing the structure in future, now it's a slice of slices,
    // because it allows not to copy the underlying key/value
    const encoded = try allocator.alloc([]const u8, tags.len * 2);
    var i: u16 = 0;
    for (tags) |f| {
        encoded[i] = f.key;
        i += 1;
        encoded[i] = f.value;
        i += 1;
    }

    return encoded;
}

fn makeStreamID(tenantID: []const u8, encodedStream: [][]const u8) SID {
    // TODO: implement, calculate the fastest hash from encodedStream
    return SID{
        .tenantID = tenantID,
        .id = encodedStream.len,
    };
}

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

    pub fn pushLine(self: *Processor, allocator: std.mem.Allocator, timestampNs: u64, fields: []Field, params: Params) !void {
        // TODO: controll how many fields a single line may contain
        // add a config value and validate fields length
        // 1000 is a default limit

        // TODO: add an option  to accept stream fields, so as not to put to stream all the fields
        // it requires 2 fields:
        // tags: list of keys to retrieve from fields to identify as a stream
        // presetStream: list of fields to append to tags

        // TODO: add an option to accep extra stream fields

        // TODO: add an option to accep extra fields

        // TODO: add an option to accept ignore fields
        // doesn't impact stream fields, to narrow set of stream fields better to use stream fields option

        const tags = fields[0 .. fields.len - 1]; // -1 cuts _msg off
        // use unstable sort because we don't expect duplicated keys
        std.mem.sortUnstable(Field, @constCast(tags), {}, sortStreamFields);

        const encodedTags = try encodeTags(allocator, tags);
        const streamID = makeStreamID(params.tenantID, encodedTags);
        const line = Line{
            .timestampNs = timestampNs,
            .sid = streamID,
            .fields = fields[fields.len - 1 ..],
            .encodedTags = encodedTags,
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

        var linesByInterval = std.AutoHashMap(u64, Lines).init(allocator);
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
