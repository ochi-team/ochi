const std = @import("std");
const Allocator = std.mem.Allocator;

// Source is a draft for a file / mem reader,
// potentially can become a proper reader,
// for now only has slice API that returns the full read buffer,
// slice could potentially become readAll()

pub const SourceOpts = struct {
    bufioSize: comptime_int = 4096,
};

pub fn Source(opts: SourceOpts) type {
    return union(enum) {
        memory: MemorySource,
        file: FileSource,

        const Self = @This();

        const MemorySource = struct {
            buf: std.ArrayList(u8),

            pub fn deinit(self: *MemorySource, alloc: Allocator) void {
                self.buf.deinit(alloc);
            }
        };

        const FileSource = struct {
            file: std.fs.File,
            size: u64,
            target: []u8 = &[_]u8{},

            buf: [opts.bufioSize]u8 = undefined,

            pub fn deinit(self: *FileSource) void {
                if (self.target.len > 0) self.alloc.free(self.target);
                self.file.close();
            }
        };

        pub fn initMem(buf: std.ArrayList(u8)) Self {
            return .{
                .memory = .{
                    .buf = buf,
                },
            };
        }

        pub fn initFile(alloc: Allocator, path: []const u8) !Self {
            var file = try std.fs.openFileAbsolute(path, .{});
            errdefer file.close();

            const stat = try file.stat();

            const target = try alloc.alloc(u8, stat.size);
            errdefer alloc.free(target);

            var buf: [4096]u8 = undefined;
            var fileReader = file.reader(&buf);
            var reader = &fileReader.interface;
            try reader.readSliceAll(target);

            return .{
                .file = .{
                    .file = file,
                    .size = stat.size,
                    .target = target,
                },
            };
        }

        pub fn deinit(self: *Self, alloc: Allocator) void {
            switch (self.*) {
                .memory => |*src| src.buf.deinit(alloc),
                .file => |*src| {
                    src.file.close();
                    if (src.target.len > 0) alloc.free(src.target);
                },
            }
        }

        // slice is technically a readAll for the data source,
        // but there is no need in reader api yet
        pub fn slice(self: *Self) []u8 {
            switch (self.*) {
                .memory => |*src| return src.buf.items,
                .file => |*src| return src.target,
            }
        }
    };
}

const testing = std.testing;

test "Source.slice returns original content from file source" {
    const alloc = testing.allocator;
    const file_name = "reader_slice_tmp_test.txt";
    const expected = "reader slice file content";

    var dir = testing.tmpDir(.{});
    defer dir.cleanup();
    {
        const file = try dir.dir.createFile(file_name, .{ .truncate = true });
        defer file.close();
        try file.writeAll(expected);
    }

    const fileNameAbs = try dir.dir.realpathAlloc(alloc, file_name);
    defer alloc.free(fileNameAbs);
    var src = try Source(.{}).initFile(alloc, fileNameAbs);
    defer src.deinit(alloc);

    try testing.expect(src == .file);
    try testing.expectEqualSlices(u8, expected, src.slice());
}

test "Source.slice returns original content from memory list source" {
    const alloc = testing.allocator;
    const expected = "reader slice memory content";

    var buf = std.ArrayList(u8).empty;
    try buf.appendSlice(alloc, expected);

    var src = Source(.{}).initMem(buf);
    defer src.deinit(alloc);

    try testing.expectEqualSlices(u8, expected, src.slice());
}
