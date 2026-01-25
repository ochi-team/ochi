const std = @import("std");
const Allocator = std.mem.Allocator;

const fs = @import("../../fs.zig");

const filenameMeta = "metadata.json";

const TableHeader = @This();

itemsCount: u64 = 0,
blocksCount: u64 = 0,
firstItem: []const u8 = undefined,
lastItem: []const u8 = undefined,

pub fn writeMeta(self: *const TableHeader, alloc: Allocator, tablePath: []const u8) !void {
    const json = try std.json.Stringify.valueAlloc(alloc, .{
        .itemsCount = self.itemsCount,
        .blocksCount = self.blocksCount,
        .firstItem = self.firstItem,
        .lastItem = self.lastItem,
    }, .{ .whitespace = .minified });
    defer alloc.free(json);

    const metadataPath = try std.fs.path.join(alloc, &[_][]const u8{ tablePath, filenameMeta });
    defer alloc.free(metadataPath);

    try fs.writeBufferValToFile(metadataPath, json);
}
