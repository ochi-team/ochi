const std = @import("std");

const maxEntrySize = @import("Entries.zig").maxEntrySize;

const TableHeader = @import("TableHeader.zig");

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
