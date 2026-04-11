const std = @import("std");

const Store = @import("store.zig").Store;
const Encoder = @import("encoding").Encoder;
const Field = @import("store/lines.zig").Field;
const Line = @import("store/lines.zig").Line;
const SID = @import("store/lines.zig").SID;

pub const Params = struct {
    tenantID: []const u8,
};

const EncodedTags = struct {
    buf: []u8,
    offset: usize,
};

fn encodeTags(allocator: std.mem.Allocator, tags: []const Field) !EncodedTags {
    // [10:len] + tags.len * [10:key len][key][10:value.len][value]
    var size: usize = 10;
    for (tags) |tag| {
        size += 20;
        size += tag.key.len + tag.value.len;
    }
    const buf = try allocator.alloc(u8, size);

    var enc = Encoder.init(buf);
    enc.writeVarInt(tags.len);
    for (tags) |tag| {
        enc.writeString(tag.key);
        enc.writeString(tag.value);
    }

    return .{
        .buf = buf,
        .offset = enc.offset,
    };
}

const magic = "xxhash";
fn makeStreamID(tenantID: []const u8, encodedStream: []const u8) SID {
    var hasher = std.hash.XxHash64.init(0);
    hasher.update(encodedStream);
    const first = hasher.final();
    hasher.update(magic);
    const second = hasher.final();
    const id = @as(u128, first) << 64 | second;

    return SID{
        .tenantID = tenantID,
        .id = id,
    };
}

// TODO: make it configurable
const retentionNs: u64 = 30 * std.time.ns_per_day;

fn sortStreamFields(_: void, one: Field, another: Field) bool {
    return std.mem.order(u8, one.key, another.key) == .lt;
}

pub const Processor = struct {
    lines: [1]Line,
    tags: [1][]Field,
    encodedTags: EncodedTags,

    store: *Store,

    pub fn init(allocator: std.mem.Allocator, store: *Store) !*Processor {
        const processor = try allocator.create(Processor);
        processor.store = store;
        processor.encodedTags = .{ .buf = undefined, .offset = 0 };
        return processor;
    }
    pub fn deinit(self: *Processor, allocator: std.mem.Allocator) void {
        if (self.encodedTags.offset != 0) {
            allocator.free(self.encodedTags.buf);
        }
        allocator.destroy(self);
    }

    pub fn pushLine(
        self: *Processor,
        allocator: std.mem.Allocator,
        timestampNs: u64,
        fields: []Field,
        tags: []Field,
        params: Params,
    ) !void {
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

        // use unstable sort because we don't expect duplicated keys
        std.mem.sortUnstable(Field, tags, {}, sortStreamFields);

        const encodedTags = try encodeTags(allocator, tags);
        const streamID = makeStreamID(params.tenantID, encodedTags.buf[0..encodedTags.offset]);

        const line = Line{
            .timestampNs = timestampNs,
            .sid = streamID,
            .fields = fields,
        };
        self.lines[0] = line;
        self.tags[0] = tags;

        if (self.encodedTags.offset != 0) {
            allocator.free(self.encodedTags.buf);
        }
        self.encodedTags = encodedTags;
    }
    pub fn mustFlush(_: *Processor) bool {
        return true;
    }
    pub fn flush(self: *Processor, allocator: std.mem.Allocator) !void {
        // TODO: add to hot partition if possible

        // TODO: make partition interval configurable
        // in order to being able to test shorter partitions: 1, 2, 3, 6, 12 hours
        const nowNs: u64 = @intCast(std.time.nanoTimestamp());
        const minDay = (nowNs - retentionNs) / std.time.ns_per_day;
        // limit the incoming logs to now + 1 day,
        // in case an ingestor sends data with broken timezone or timestamp
        const maxDay = (nowNs + std.time.ns_per_day) / std.time.ns_per_day;

        var linesByInterval = std.AutoHashMap(u64, std.ArrayList(Line)).init(allocator);

        for (self.lines) |line| {
            const day = line.timestampNs / std.time.ns_per_day;
            if (day < minDay) {
                // TODO: log a warning
                continue;
            }
            if (day > maxDay) {
                // TODO: log a warning
                continue;
            }

            if (linesByInterval.getPtr(day)) |list| {
                try list.append(allocator, line);
                continue;
            }
            var list = try std.ArrayList(Line).initCapacity(allocator, self.lines.len);
            try linesByInterval.put(day, list);
            try list.append(allocator, line);
        }

        try self.store.addLines(
            allocator,
            linesByInterval,
            self.tags[0],
            self.encodedTags.buf[0..self.encodedTags.offset],
        );
    }
};
