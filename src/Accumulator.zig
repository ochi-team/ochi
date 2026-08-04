const std = @import("std");
const Allocator = std.mem.Allocator;
const FixedBufferAllocator = std.heap.FixedBufferAllocator;
const Io = std.Io;

const Store = @import("Store.zig").Store;
const Field = @import("store/lines.zig").Field;
const Line = @import("store/lines.zig").Line;
const SID = @import("store/lines.zig").SID;
const validate = @import("store/lines.zig").validate;
const copyFields = @import("store/lines.zig").copyFields;
const freeFields = @import("store/lines.zig").freeFields;
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

const Accumulator = @This();

// Accumulator can buffer several streams
// before flushing them all in one shot,
// so it splits them in different sections
pub const Checkpoint = struct {
    sid: SID,
    // tags are borrowed from the parental buffer
    tags: []Field,
    encodedTags: []const u8,
    i: usize,
};

pub const maxCheckpoints = 16;

store: *Store,

lines: std.ArrayList(Line) = .empty,
buffer: FixedBufferAllocator,

checkpoints: [maxCheckpoints]Checkpoint = undefined,
checkpointsLen: u16 = 0,
currentCheckpoint: u16 = 0,

// identity of the stream currently being appended to since
// checkpoint buffers don't survives flush,
// it allows us to recreate a checkpoint
currentTags: []Field = &[_]Field{},
currentEncodedTags: []const u8 = "",
currentSid: SID = .{ .tenantID = 0, .id = 0 },

// flush deadline in microseconds, set the first time a pooled accumulator
// receives data and cleared on flush; see AccumulatorPool
flushAtUs: ?u64 = null,

pub fn init(alloc: Allocator, store: *Store) !Accumulator {
    const buf = try alloc.alloc(u8, maxBlockSize);
    errdefer alloc.free(buf);

    return Accumulator{
        .store = store,
        .buffer = FixedBufferAllocator.init(buf),
    };
}

fn resetBuffered(self: *Accumulator) void {
    self.lines.clearRetainingCapacity();
    self.buffer.reset();
    self.checkpointsLen = 0;
    self.currentCheckpoint = 0;
    self.flushAtUs = null;
}

pub fn reinit(
    self: *Accumulator,
    alloc: Allocator,
    tags: []Field,
    tenantID: u64,
) !void {
    // use unstable sort because we don't expect duplicated keys
    std.sort.pdq(Field, tags, {}, sortStreamFields);

    const encodedTags = try encodeTags(alloc, tags);
    errdefer alloc.free(encodedTags);

    const streamID = makeStreamID(tenantID, encodedTags);

    const tagsCopy = try copyFields(alloc, tags);
    errdefer freeFields(alloc, tagsCopy);

    if (self.currentTags.len > 0) freeFields(alloc, self.currentTags);
    if (self.currentEncodedTags.len > 0) alloc.free(self.currentEncodedTags);

    self.currentTags = tagsCopy;
    self.currentEncodedTags = encodedTags;
    self.currentSid = streamID;
}

pub fn deinit(self: *Accumulator, alloc: Allocator) void {
    if (self.currentEncodedTags.len > 0) alloc.free(self.currentEncodedTags);
    if (self.currentTags.len > 0) freeFields(alloc, self.currentTags);
    self.resetBuffered();
    self.lines.deinit(alloc);
    alloc.free(self.buffer.buffer);
    self.* = undefined;
}

// ensureActiveCheckpoint makes sure currentCheckpoint tracks the current stream
// or creates a new checkpoint whenever the active
// stream changed since the last append.
fn ensureActiveCheckpoint(self: *Accumulator, io: Io, alloc: Allocator) !void {
    if (self.checkpointsLen > 0 and self.checkpoints[self.checkpointsLen - 1].sid.eql(self.currentSid)) {
        self.currentCheckpoint = self.checkpointsLen - 1;
        return;
    }

    if (self.checkpointsLen == maxCheckpoints) {
        try self.flush(io, alloc);
    }

    const bufferAlloc = self.buffer.allocator();
    // since checkpoint doesn't survive flush we must restore it
    self.checkpoints[self.checkpointsLen] = .{
        .sid = self.currentSid,
        .tags = try copyFields(bufferAlloc, self.currentTags),
        .encodedTags = try bufferAlloc.dupe(u8, self.currentEncodedTags),
        .i = self.lines.items.len,
    };
    self.currentCheckpoint = self.checkpointsLen;
    self.checkpointsLen += 1;
}

pub fn tryAppendLine(
    self: *Accumulator,
    io: Io,
    alloc: Allocator,
    timestampNs: u64,
    fields: []Field,
) !void {
    self.appendLine(io, alloc, timestampNs, fields) catch |err| {
        switch (err) {
            Allocator.Error.OutOfMemory => {
                Logger.log(.warn, "accumulator: buffer overflow, decrease flush threashold", .{});
                try self.flush(io, alloc);
                try self.appendLine(io, alloc, timestampNs, fields);
            },
            else => return err,
        }
    };
}

fn appendLine(
    self: *Accumulator,
    io: Io,
    alloc: Allocator,
    timestampNs: u64,
    fields: []Field,
) !void {
    try self.ensureActiveCheckpoint(io, alloc);

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

    validate(fields) catch |err| {
        switch (err) {
            error.MaxFieldsPerLineExceeded => {
                Logger.log(.warn, "max fields per line exceeded", .{});
                return;
            },
            error.MaxFieldKeySizeExceeded => {
                Logger.log(.warn, "max field key size exceeded", .{});
                return;
            },
            error.MaxFieldValueSizeExceeded => {
                Logger.log(.warn, "DataShard: max field value size exceeded", .{});
                return;
            },
            error.MaxLineSizeExceeded => {
                Logger.log(.warn, "max line size exceeded", .{});
                return;
            },
        }
    };

    try self.lines.append(alloc, line);
    self.checkpoints[self.currentCheckpoint].i = self.lines.items.len;

    if (self.mustFlush(io)) {
        try self.flush(io, alloc);
    }
}

pub fn mustFlush(self: *Accumulator, io: Io) bool {
    if (self.flushAtUs) |flushAtUs| {
        if (std.Io.Timestamp.now(io, .real).toMicroseconds() >= flushAtUs) return true;
    }
    return self.buffer.end_index >= flushSizeThreshold;
}

pub fn flush(self: *Accumulator, io: Io, alloc: Allocator) !void {
    var start: usize = 0;
    for (self.checkpoints[0..self.checkpointsLen]) |checkpoint| {
        const chunk = self.lines.items[start..checkpoint.i];
        start = checkpoint.i;
        if (chunk.len == 0) continue;

        try self.store.addLines(io, alloc, chunk, checkpoint.tags, checkpoint.encodedTags, checkpoint.sid);
    }
    self.resetBuffered();
}

const testing = std.testing;

test "Accumulator.reinit owns stream tags after caller reuses tag storage" {
    const alloc = testing.allocator;
    var store: Store = undefined;
    var accumulator = try Accumulator.init(alloc, &store);
    defer accumulator.deinit(alloc);

    var tags = try std.ArrayList(Field).initCapacity(testing.allocator, 2);
    defer tags.deinit(testing.allocator);
    tags.appendAssumeCapacity(.{ .key = "app", .value = "api" });
    tags.appendAssumeCapacity(.{ .key = "env", .value = "prod" });

    try accumulator.reinit(testing.allocator, tags.items, 0);

    tags.items[0] = .{ .key = "id", .value = "line-1" };
    tags.items[1] = .{ .key = "", .value = "message" };

    const expected = [_]Field{
        .{ .key = "app", .value = "api" },
        .{ .key = "env", .value = "prod" },
    };
    try testing.expectEqualDeep(expected[0..], accumulator.currentTags);
}

test "Accumulator buffers multiple streams and flushes them as separate checkpoints" {
    const alloc = testing.allocator;
    var store: Store = undefined;
    var accumulator = try Accumulator.init(alloc, &store);
    defer accumulator.deinit(alloc);

    var tagsA = [_]Field{.{ .key = "app", .value = "a" }};
    var tagsB = [_]Field{.{ .key = "app", .value = "b" }};

    var lineA1 = [_]Field{.{ .key = "", .value = "line-a1" }};
    var lineA2 = [_]Field{.{ .key = "", .value = "line-a2" }};
    var lineB1 = [_]Field{.{ .key = "", .value = "line-b1" }};

    try accumulator.reinit(alloc, tagsA[0..], 0);
    try accumulator.tryAppendLine(testing.io, alloc, 1, lineA1[0..]);
    try accumulator.tryAppendLine(testing.io, alloc, 2, lineA2[0..]);

    try accumulator.reinit(alloc, tagsB[0..], 0);
    try accumulator.tryAppendLine(testing.io, alloc, 3, lineB1[0..]);

    try testing.expectEqual(2, accumulator.checkpointsLen);
    try testing.expectEqual(2, accumulator.checkpoints[0].i);
    try testing.expectEqual(3, accumulator.checkpoints[1].i);
}
