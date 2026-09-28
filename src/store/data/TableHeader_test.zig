const std = @import("std");

// adding new fields take into account the header must own them,
// follow the pattern of the index table header
const TableHeader = @import("TableHeader.zig");

const testing = std.testing;

test "roundtrip file read/write" {
    const io = testing.io;

    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    const Case = struct {
        header: TableHeader,
    };
    const cases = &[_]Case{
        .{
            .header = .{
                .minTimestamp = 10,
                .maxTimestamp = 25,
                .uncompressedSize = 1024,
                .compressedSize = 512,
                .len = 3,
                .blocksCount = 2,
                .bloomValuesBuffersAmount = 7,
            },
        },
        .{
            .header = .{
                .minTimestamp = std.math.maxInt(u64),
                .maxTimestamp = std.math.maxInt(u64),
                .uncompressedSize = std.math.maxInt(u32),
                .compressedSize = std.math.maxInt(u32),
                .len = std.math.maxInt(u32),
                .blocksCount = std.math.maxInt(u32),
                .bloomValuesBuffersAmount = std.math.maxInt(u32),
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

        const readHeader = try TableHeader.readFile(io, tablePath);
        try testing.expectEqualDeep(header, readHeader);
    }
}
