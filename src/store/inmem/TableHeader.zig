const std = @import("std");
const Allocator = std.mem.Allocator;

const Self = @This();

version: u8,
uncompressedSize: u32,
compressedSize: u32,
len: u32,
blocksCount: u32,
minTimestamp: u64,
maxTimestamp: u64,
bloomValuesBuffersAmount: u32,

pub fn init(alloc: Allocator) !*Self {
    return alloc.create(Self);
}

pub fn deinit(self: *Self, alloc: Allocator) void {
    alloc.destroy(self);
}

pub fn flushMetadata(
    self: *Self,
    allocator: std.mem.Allocator,
    path: []const u8,
    metadata_filename: []const u8,
) !void {
    const metadata = try std.json.Stringify.valueAlloc(
        allocator,
        self,
        .{},
    );
    defer allocator.free(metadata);

    const metadata_path = try std.fs.path.join(
        allocator,
        &.{ path, metadata_filename },
    );
    defer allocator.free(metadata_path);

    var file = try std.fs.createFileAbsolute(
        metadata_path,
        .{ .truncate = true },
    );
    defer file.close();

    try file.writeAll(metadata);
}
