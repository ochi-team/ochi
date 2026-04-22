const std = @import("std");
const Allocator = std.mem.Allocator;

const Store = @import("Store.zig").Store;
const Encoder = @import("encoding").Encoder;
const Field = @import("store/lines.zig").Field;
const Line = @import("store/lines.zig").Line;
const SID = @import("store/lines.zig").SID;

pub const Params = struct {
    tenantID: []const u8,
};

fn encodeTags(allocator: std.mem.Allocator, tags: []const Field) ![]u8 {
    var size: usize = Encoder.varIntBound(tags.len);
    for (tags) |tag| {
        size += Encoder.varIntBound(tag.key.len) + Encoder.varIntBound(tag.value.len);
        size += tag.key.len + tag.value.len;
    }
    const buf = try allocator.alloc(u8, size);

    var enc = Encoder.init(buf);
    enc.writeVarInt(tags.len);
    for (tags) |tag| {
        enc.writeString(tag.key);
        enc.writeString(tag.value);
    }

    std.debug.assert(enc.offset == buf.len);

    return buf;
}

const magicStr = "xxhash";
fn makeStreamID(tenantID: []const u8, encodedStream: []const u8) SID {
    var hasher = std.hash.XxHash64.init(0);
    hasher.update(encodedStream);
    const first = hasher.final();
    hasher.update(magicStr);
    const second = hasher.final();
    const id = @as(u128, first) << 64 | second;

    return SID{
        .tenantID = tenantID,
        .id = id,
        .buf = tenantID,
    };
}

fn sortStreamFields(_: void, one: Field, another: Field) bool {
    return std.mem.order(u8, one.key, another.key) == .lt;
}

pub const Processor = struct {
    store: *Store,

    size: u32 = 0,
    lines: std.ArrayList(Line) = .empty,
    tags: []Field,
    encodedTags: []const u8,
    sid: SID,

    pub fn empty(store: *Store) Processor {
        return Processor{
            .store = store,
            .size = 0,
            .lines = std.ArrayList(Line).empty,
            .tags = &[_]Field{},
            .encodedTags = "",
            .sid = SID{ .tenantID = "", .id = 0 },
        };
    }

    pub fn reinit(
        self: *Processor,
        alloc: std.mem.Allocator,
        tags: []Field,
        tenantID: []const u8,
    ) !void {
        // use unstable sort because we don't expect duplicated keys
        std.mem.sortUnstable(Field, tags, {}, sortStreamFields);

        const encodedTags = try encodeTags(alloc, tags);
        const tenantIDOwned = try alloc.dupe(u8, tenantID);
        const streamID = makeStreamID(tenantIDOwned, encodedTags);

        if (self.encodedTags.len > 0) {
            alloc.free(self.encodedTags);
        }
        self.lines.clearRetainingCapacity();
        self.size = 0;

        self.tags = tags;
        self.encodedTags = encodedTags;
        self.sid.deinit(alloc);
        self.sid = streamID;
    }

    pub fn deinit(self: *Processor, alloc: std.mem.Allocator) void {
        if (self.encodedTags.len > 0) {
            alloc.free(self.encodedTags);
        }
        self.lines.clearRetainingCapacity();
        self.sid.deinit(alloc);
        self.lines.deinit(alloc);
        self.size = 0;
        self.* = undefined;
    }

    pub fn pushLine(
        self: *Processor,
        alloc: std.mem.Allocator,
        timestampNs: u64,
        fields: []Field,
    ) !void {
        const line = Line{
            .timestampNs = timestampNs,
            .sid = self.sid,
            .fields = fields,
        };

        const size = line.rawSizeValidate() catch |err| {
            switch (err) {
                error.MaxFieldsPerLineExceeded => {
                    // TODO: log error
                    return;
                },
                error.MaxFieldKeySizeExceeded => {
                    // TODO: log error
                    return;
                },
                error.MaxLineSizeExceeded => {
                    // TODO: log error
                    return;
                },
            }
        };

        self.size += size.size;
        try self.lines.append(alloc, line);

        if (self.mustFlush()) {
            try self.flush(alloc);
        }
    }

    // threshold as 90% of a max block size
    const flushSizeThreshold = 9 * (2 * 1024 * 1024 / 10);
    // TODO: make size limit configurable
    // TODO: this threshold is used in DataRecorder too,
    // make it configurable and extract from both
    pub fn mustFlush(self: *Processor) bool {
        return self.size >= flushSizeThreshold;
    }

    pub fn flush(self: *Processor, alloc: std.mem.Allocator) !void {
        try self.store.addLines(alloc, self.lines.items, self.tags, self.encodedTags);
        self.lines.clearRetainingCapacity();
        self.size = 0;
    }
};
