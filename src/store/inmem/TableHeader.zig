const std = @import("std");

const fs = @import("../../fs.zig");
const filenames = @import("../../filenames.zig");

const maxFileBytes = 16 * 1024 * 1024;

// adding new fields take into account the header must own them,
// follow the pattern of the index table header
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
pub fn writeFile(
    self: *const TableHeader,
    allocator: std.mem.Allocator,
    path: []const u8,
) !void {
    const json = try std.json.Stringify.valueAlloc(
        allocator,
        self,
        .{},
    );
    defer allocator.free(json);

    const metadataPath = try std.fs.path.join(
        allocator,
        &.{ path, filenames.header },
    );
    defer allocator.free(metadataPath);

    try fs.writeBufferValToFile(metadataPath, json);
}

pub fn readFile(
    allocator: std.mem.Allocator,
    path: []const u8,
) !TableHeader {
    var fba = std.heap.stackFallback(1024, allocator);
    const fbaAlloc = fba.get();

    const metadataPath = try std.fs.path.join(
        fbaAlloc,
        &.{ path, filenames.header },
    );
    defer fbaAlloc.free(metadataPath);

    var file = try std.fs.openFileAbsolute(metadataPath, .{});
    defer file.close();

    const data = try file.readToEndAlloc(fbaAlloc, maxFileBytes);
    defer fbaAlloc.free(data);

    const parsed = try std.json.parseFromSlice(TableHeader, fbaAlloc, data, .{});
    defer parsed.deinit();

    return parsed.value;
}

const testing = std.testing;

test "roundtrip file read/write" {
    const alloc = testing.allocator;
    const io = testing.io;

    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    try tmp.dir.makePath("table");
    const tablePath = try tmp.dir.realPathFileAlloc(io, alloc, "table");
    defer alloc.free(tablePath);

    const header = TableHeader{
        .minTimestamp = 10,
        .maxTimestamp = 25,
        .uncompressedSize = 1024,
        .compressedSize = 512,
        .len = 3,
        .blocksCount = 2,
        .bloomValuesBuffersAmount = 7,
    };

    try header.writeFile(alloc, tablePath);

    const readHeader = try TableHeader.readFile(alloc, tablePath);
    try testing.expectEqualDeep(header, readHeader);
}
