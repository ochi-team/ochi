const std = @import("std");

const TableHeader = @This();

// TODO: find out whether we can do them u32
minTimestamp: u64 = 0,
maxTimestamp: u64 = 0,
uncompressedSize: u32 = 0,
compressedSize: u32 = 0,
len: u32 = 0,
blocksCount: u32 = 0,
bloomValuesBuffersAmount: u32 = 0,

/// flush writes header file to disk,
/// header is saved as a json structure
pub fn flush(
    self: *const TableHeader,
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
