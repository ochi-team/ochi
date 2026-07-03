const std = @import("std");
const Allocator = std.mem.Allocator;
const FixedBufferAllocator = std.heap.FixedBufferAllocator;
const Io = std.Io;

const Store = @import("Store.zig").Store;
const Field = @import("store/lines.zig").Field;
const Line = @import("store/lines.zig").Line;
const SID = @import("store/lines.zig").SID;
const rawFieldsSizeValidate = @import("store/lines.zig").rawFieldsSizeValidate;
const copyFields = @import("store/lines.zig").copyFields;
const encodeTags = @import("store/lines.zig").encodeTags;
const makeStreamID = @import("store/lines.zig").makeStreamID;

const Consts = @import("Consts.zig");

const maxBlockSize = Consts.maxBlockSize;
const flushSizeThreshold = Consts.flushSizeThreshold;

const Logger = @import("logging");

pub const Params = struct {
    tenantID: u64,
};

fn sortStreamFields(_: void, one: Field, another: Field) bool {
    return std.mem.order(u8, one.key, another.key) == .lt;
}

pub const Processor = struct {
    store: *Store,

    lines: std.ArrayList(Line) = .empty,
    // TODO: buffer lines twice is idiotic,
    // we have to document why or make it onyl once in a DataShard
    buffer: FixedBufferAllocator,
    tags: []Field,
    encodedTags: []const u8,
    sid: SID,

    pub fn init(alloc: Allocator, store: *Store) !Processor {
        const buf = try alloc.alloc(u8, maxBlockSize);
        errdefer alloc.free(buf);

        return Processor{
            .store = store,
            .buffer = FixedBufferAllocator.init(buf),
            .lines = std.ArrayList(Line).empty,
            .tags = &[_]Field{},
            .encodedTags = "",
            .sid = SID{ .tenantID = 0, .id = 0 },
        };
    }

    fn resetBuffered(self: *Processor) void {
        self.lines.clearRetainingCapacity();
        self.buffer.reset();
        // we don't free tags due to arena.reset
        self.tags = &[_]Field{};
    }

    pub fn reinit(
        self: *Processor,
        alloc: Allocator,
        tags: []Field,
        tenantID: u64,
    ) !void {
        // use unstable sort because we don't expect duplicated keys
        std.sort.pdq(Field, tags, {}, sortStreamFields);

        const encodedTags = try encodeTags(alloc, tags);
        errdefer alloc.free(encodedTags);

        const streamID = makeStreamID(tenantID, encodedTags);

        if (self.encodedTags.len > 0) {
            alloc.free(self.encodedTags);
            self.encodedTags = "";
        }
        self.resetBuffered();

        // TODO: we probably can avoid the copy, the issue is if the Field array grows to much
        // the borrowed slice becomes dangling, so we can shift the time when we borrow,
        // if we can't - document why
        self.tags = try copyFields(self.buffer.allocator(), tags);
        self.encodedTags = encodedTags;
        self.sid = streamID;
    }

    pub fn deinit(self: *Processor, alloc: Allocator) void {
        if (self.encodedTags.len > 0) {
            alloc.free(self.encodedTags);
        }
        self.resetBuffered();
        self.lines.deinit(alloc);
        alloc.free(self.buffer.buffer);
        self.* = undefined;
    }

    pub fn tryAppendLine(
        self: *Processor,
        io: Io,
        alloc: Allocator,
        timestampNs: u64,
        fields: []Field,
    ) !void {
        self.appendLine(io, alloc, timestampNs, fields) catch |err| {
            switch (err) {
                Allocator.Error.OutOfMemory => {
                    Logger.log(.warn, "processor: buffer overflow, decrease flush threashold", .{});
                    try self.flush(io, alloc);
                    try self.appendLine(io, alloc, timestampNs, fields);
                },
                else => return err,
            }
        };
    }

    fn appendLine(
        self: *Processor,
        io: Io,
        alloc: Allocator,
        timestampNs: u64,
        fields: []Field,
    ) !void {
        const bufferAlloc = self.buffer.allocator();
        const fieldsCopy = try bufferAlloc.alloc(Field, fields.len);
        for (fields, 0..) |field, i| {
            fieldsCopy[i] = .{
                .key = try bufferAlloc.dupe(u8, field.key),
                .value = try bufferAlloc.dupe(u8, field.value),
            };
        }
        const line = Line{
            .timestampNs = timestampNs,
            .fields = fieldsCopy,
        };

        _ = rawFieldsSizeValidate(fields) catch |err| {
            switch (err) {
                error.MaxFieldsPerLineExceeded => {
                    Logger.log(.warn, "max fields per line exceeded", .{});
                    return;
                },
                error.MaxFieldKeySizeExceeded => {
                    Logger.log(.warn, "max field key size exceeded", .{});
                    return;
                },
                error.MaxLineSizeExceeded => {
                    Logger.log(.warn, "max line size exceeded", .{});
                    return;
                },
            }
        };

        try self.lines.append(alloc, line);

        if (self.mustFlush()) {
            try self.flush(io, alloc);
        }
    }

    pub fn mustFlush(self: *Processor) bool {
        return self.buffer.end_index >= flushSizeThreshold;
    }

    pub fn flush(self: *Processor, io: Io, alloc: Allocator) !void {
        try self.store.addLines(io, alloc, self.lines.items, self.tags, self.encodedTags, self.sid);
        self.resetBuffered();
    }
};

const testing = std.testing;

test "Processor.reinit owns stream tags after caller reuses tag storage" {
    const alloc = testing.allocator;
    var store: Store = undefined;
    var processor = try Processor.init(alloc, &store);
    defer processor.deinit(alloc);

    var tags = try std.ArrayList(Field).initCapacity(testing.allocator, 2);
    defer tags.deinit(testing.allocator);
    tags.appendAssumeCapacity(.{ .key = "app", .value = "api" });
    tags.appendAssumeCapacity(.{ .key = "env", .value = "prod" });

    try processor.reinit(testing.allocator, tags.items, 0);

    tags.items[0] = .{ .key = "id", .value = "line-1" };
    tags.items[1] = .{ .key = "", .value = "message" };

    const expected = [_]Field{
        .{ .key = "app", .value = "api" },
        .{ .key = "env", .value = "prod" },
    };
    try testing.expectEqualDeep(expected[0..], processor.tags);
}
