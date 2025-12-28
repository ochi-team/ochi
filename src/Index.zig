const std = @import("std");

const SID = @import("store/lines.zig").SID;
const IndexTable = @import("IndexTable.zig");

const Self = @This();

table: *IndexTable,

pub fn init(allocator: std.mem.Allocator, table: *IndexTable) !*Self {
    const i = try allocator.create(Self);
    i.* = .{
        .table = table,
    };
    return i;
}

pub fn hasStream(self: *Self, streamID: SID) bool {
    _ = self;
    _ = streamID;
    unreachable;
}
pub fn registerStream(self: *Self, allocator: std.mem.Allocator, streamID: SID, encodedTags: []const u8) !void {
    _ = self;
    _ = allocator;
    _ = streamID;
    _ = encodedTags;
    unreachable;
}
