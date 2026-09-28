/// TagRecordsMerger: Merges consecutive tagToSids index records with the same prefix.
///
/// Use cases:
/// - Compacting inverted index entries: multiple (tenant:tag -> streamID) records
///   are merged into a single (tenant:tag -> [streamIDs]) record
/// - Reducing index size by deduplicating stream IDs
///
/// Constraints:
/// - Operates on sorted input; records must be processed in order
/// - Uses two TagRecordsParser instances for comparing consecutive records
/// - Output streamIDs are sorted and deduplicated
/// - Caller must call writeState() before switching to a different prefix
const std = @import("std");
const Allocator = std.mem.Allocator;

const TagRecordsParser = @import("TagRecordsParser.zig");
const MemBlock = @import("MemBlock.zig");

const TagRecordsMerger = @This();

streamIDs: std.ArrayList(u128) = .empty,
state: TagRecordsParser = .{},
prevState: TagRecordsParser = .{},

pub fn deinit(self: *TagRecordsMerger, alloc: Allocator) void {
    self.streamIDs.deinit(alloc);
    self.state.deinit(alloc);
    self.prevState.deinit(alloc);
}

pub fn writeState(self: *TagRecordsMerger, alloc: Allocator, target: *MemBlock) !void {
    if (self.streamIDs.items.len == 0) {
        return;
    }

    std.sort.pdq(u128, self.streamIDs.items, {}, std.sort.asc(u128));
    self.removeDuplicatedStreams();

    const bound = TagRecordsParser.encodeRecordBound(self.prevState.tag, self.streamIDs.items.len);
    try target.buf.ensureUnusedCapacity(alloc, bound);
    const start: u16 = @intCast(target.buf.items.len);
    const slice = target.buf.unusedCapacitySlice();

    const recordLen = TagRecordsParser.encodeRecord(
        slice,
        self.prevState.tenantID,
        self.prevState.tag,
        self.streamIDs.items,
    );
    target.buf.items.len += recordLen;

    self.streamIDs.clearRetainingCapacity();
    target.addOwned(.{ .start = start, .end = @intCast(target.buf.items.len) });
}

pub fn removeDuplicatedStreams(self: *TagRecordsMerger) void {
    if (self.streamIDs.items.len < 2) return;

    var write: usize = 1;
    var prev = self.streamIDs.items[0];

    var i: usize = 1;
    while (i < self.streamIDs.items.len) : (i += 1) {
        const v = self.streamIDs.items[i];
        if (v != prev) {
            self.streamIDs.items[write] = v;
            write += 1;
            prev = v;
        }
    }
    self.streamIDs.items.len = write;
}

pub fn statesPrefixEqual(self: *const TagRecordsMerger) bool {
    if (self.state.tenantID != self.prevState.tenantID) return false;

    if (!self.state.tag.eql(self.prevState.tag)) return false;

    return true;
}

pub fn moveParsedState(self: *TagRecordsMerger, alloc: Allocator) !void {
    try self.streamIDs.appendSlice(alloc, self.state.streamIDs.items);
    std.mem.swap(TagRecordsParser, &self.state, &self.prevState);
}

test {
    _ = @import("TagRecordsMerger_test.zig");
}
