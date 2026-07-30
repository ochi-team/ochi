const std = @import("std");
const Allocator = std.mem.Allocator;
const Io = std.Io;
const Dir = Io.Dir;

const fs = @import("../../fs.zig");
const filenames = @import("../../filenames.zig");
const maxEntrySize = @import("Entries.zig").maxEntrySize;

const headerBufferSize = 4096;

const TableHeader = @This();

entriesCount: u64 = 0,
blocksCount: u64 = 0,
firstEntry: []const u8 = "",
lastEntry: []const u8 = "",

pub fn deinit(self: TableHeader, alloc: Allocator) void {
    alloc.free(self.firstEntry);
    alloc.free(self.lastEntry);
}

pub fn readFile(io: Io, alloc: Allocator, path: []const u8) !TableHeader {
    var metadataBuf: [std.fs.max_path_bytes]u8 = undefined;
    var metadataPathWriter = std.Io.Writer.fixed(&metadataBuf);
    try std.fs.path.fmtJoin(&.{ path, filenames.header }).format(&metadataPathWriter);

    var file = Dir.openFileAbsolute(io, metadataPathWriter.buffered(), .{}) catch |err| {
        std.debug.panic("can't open table header '{s}': {s}", .{ path, @errorName(err) });
    };
    defer file.close(io);

    var rawBuf: [headerBufferSize]u8 = undefined;
    var fileReader = file.reader(io, &metadataBuf);
    const n = try fileReader.interface.readSliceShort(&rawBuf);

    var jsonBuf: [headerBufferSize]u8 = undefined;
    var bufferAlloc = std.heap.FixedBufferAllocator.init(&jsonBuf);
    const stackAlloc = bufferAlloc.allocator();
    const parsed = try std.json.parseFromSlice(TableHeader, stackAlloc, rawBuf[0..n], .{});
    defer parsed.deinit();

    const firstEntry = try alloc.dupe(u8, parsed.value.firstEntry);
    errdefer alloc.free(firstEntry);
    const lastEntry = try alloc.dupe(u8, parsed.value.lastEntry);
    errdefer alloc.free(lastEntry);

    return .{
        .blocksCount = parsed.value.blocksCount,
        .entriesCount = parsed.value.entriesCount,
        .firstEntry = firstEntry,
        .lastEntry = lastEntry,
    };
}

pub fn writeFile(self: *const TableHeader, io: Io, tablePath: []const u8) !void {
    var buf: [headerBufferSize]u8 = undefined;
    var w: std.Io.Writer = .fixed(&buf);
    try std.json.Stringify.value(.{
        .entriesCount = self.entriesCount,
        .blocksCount = self.blocksCount,
        .firstEntry = self.firstEntry,
        .lastEntry = self.lastEntry,
    }, .{ .whitespace = .minified }, &w);

    var metadataPathBuf: [std.fs.max_path_bytes]u8 = undefined;
    var metadataPathWriter = std.Io.Writer.fixed(&metadataPathBuf);
    try std.fs.path.fmtJoin(&.{ tablePath, filenames.header }).format(&metadataPathWriter);

    try fs.writeBufferValToFile(io, metadataPathWriter.buffered(), w.buffered());
}

const testing = std.testing;

test "roundtrip file read/write" {
    const alloc = testing.allocator;
    const io = testing.io;
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    const Case = struct {
        header: TableHeader,
    };

    var largeFirstEntry: [maxEntrySize]u8 = undefined;
    @memset(&largeFirstEntry, 'a');
    var largeLastEntry: [maxEntrySize]u8 = undefined;
    @memset(&largeLastEntry, 'x');

    const cases = &[_]Case{
        .{
            .header = .{
                .blocksCount = 5,
                .entriesCount = 12,
                .firstEntry = "alpha",
                .lastEntry = "omega",
            },
        },
        .{
            .header = .{
                .blocksCount = std.math.maxInt(u64),
                .entriesCount = std.math.maxInt(u64),
                .firstEntry = &largeFirstEntry,
                .lastEntry = &largeLastEntry,
            },
        },
    };
    for (cases) |case| {
        try tmp.dir.createDirPath(io, "table");
        var pathBuf: [std.fs.max_path_bytes]u8 = undefined;
        const n = try tmp.dir.realPathFile(io, "table", &pathBuf);
        const tablePath = pathBuf[0..n];

        const header = case.header;

        try header.writeFile(io, tablePath);

        var readTb = try TableHeader.readFile(io, alloc, tablePath);
        defer readTb.deinit(alloc);

        try testing.expectEqualDeep(header, readTb);
    }
}
