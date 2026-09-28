const std = @import("std");
const Io = std.Io;
const Dir = Io.Dir;

const fs = @import("../../fs.zig");
const filenames = @import("../../filenames.zig");

// adding new fields take into account the header must own them,
// follow the pattern of the index table header
const TableHeader = @This();

const headerEncodeBufferSize = 256;

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
    io: Io,
    path: []const u8,
) !void {
    var buf: [headerEncodeBufferSize]u8 = undefined;
    var w = std.Io.Writer.fixed(&buf);
    try std.json.Stringify.value(self, .{}, &w);

    var metadataPathBuf: [std.fs.max_path_bytes]u8 = undefined;
    var metadataPathWriter = std.Io.Writer.fixed(&metadataPathBuf);
    try std.fs.path.fmtJoin(&.{ path, filenames.header }).format(&metadataPathWriter);

    try fs.writeBufferValToFile(io, metadataPathWriter.buffered(), w.buffered());
}

pub fn readFile(
    io: Io,
    path: []const u8,
) !TableHeader {
    var metadataPathBuf: [std.fs.max_path_bytes]u8 = undefined;
    var metadataPathWriter = std.Io.Writer.fixed(&metadataPathBuf);
    try std.fs.path.fmtJoin(&.{ path, filenames.header }).format(&metadataPathWriter);

    var file = try Dir.openFileAbsolute(io, metadataPathWriter.buffered(), .{});
    defer file.close(io);

    var rawBuf: [headerEncodeBufferSize]u8 = undefined;
    var fileReader = file.reader(io, &.{});
    const n = try fileReader.interface.readSliceShort(&rawBuf);

    var jsonBuf: [headerEncodeBufferSize]u8 = undefined;
    var stackAlloc = std.heap.FixedBufferAllocator.init(&jsonBuf);
    const alloc = stackAlloc.allocator();
    const parsed = try std.json.parseFromSlice(TableHeader, alloc, rawBuf[0..n], .{});
    defer parsed.deinit();

    return parsed.value;
}

test {
    _ = @import("TableHeader_test.zig");
}
