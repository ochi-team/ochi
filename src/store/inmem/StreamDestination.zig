const std = @import("std");
const Allocator = std.mem.Allocator;
const Io = std.Io;
const Dir = Io.Dir;

pub const FileDestination = struct {
    file: Io.File,
    len: usize,
    /// buffer holds allocated chunk to reuse between write operations
    buf: std.ArrayList(u8) = .empty,
};

pub const Tag = enum {
    buffer,
    file,
};

// TODO: it is what it is, but we must migrate to io.Writer
pub const StreamDestination = union(Tag) {
    buffer: std.ArrayList(u8),
    file: FileDestination,

    const Self = @This();

    pub fn initBuffer(allocator: Allocator, capacity: usize) !Self {
        return .{ .buffer = try std.ArrayList(u8).initCapacity(allocator, capacity) };
    }

    pub fn initFile(io: Io, file: Io.File) !Self {
        const stat = try file.stat(io);
        const initialLen: usize = @intCast(stat.size);

        // TODO AUDIT
        var buffer: [4096]u8 = undefined;
        var file_reader = file.reader(io, &buffer);
        try file_reader.seekTo(stat.size);
        return .{ .file = .{ .file = file, .len = initialLen } };
    }

    pub fn deinit(self: *Self, io: Io, allocator: Allocator) void {
        switch (self.*) {
            .buffer => |*buf| buf.deinit(allocator),
            .file => |*f| {
                f.buf.deinit(allocator);
                f.file.close(io);
            },
        }
    }

    pub fn len(self: *const Self) usize {
        return switch (self.*) {
            .buffer => |buf| buf.items.len,
            .file => |f| f.len,
        };
    }

    pub fn appendSlice(self: *Self, io: Io, allocator: Allocator, src: []const u8) !void {
        switch (self.*) {
            .buffer => |*buf| try buf.appendSlice(allocator, src),
            .file => |*f| {
                try f.file.writeStreamingAll(io, src);
                f.len += src.len;
            },
        }
    }

    fn readAll(self: *Self, io: Io, allocator: Allocator) ![]u8 {
        switch (self.*) {
            .buffer => |*buf| return allocator.dupe(u8, buf.items),
            .file => |*f| {
                const size: usize = @intCast((try f.file.stat(
                    io,
                )).size);
                const dst = try allocator.alloc(u8, size);
                errdefer allocator.free(dst);

                // TODO AUDIT
                var buffer: [4096]u8 = undefined;
                var file_reader = f.file.reader(io, &buffer);
                try file_reader.seekTo(0);

                const readN = try file_reader.interface.readSliceShort(dst);
                std.debug.assert(readN == size);

                try file_reader.seekTo(@intCast(f.len));
                return dst;
            },
        }
    }

    pub fn asSliceAssumeBuffer(self: *const Self) []const u8 {
        return switch (self.*) {
            .buffer => |buf| buf.items,
            .file => std.debug.panic("not a buffer destination", .{}),
        };
    }

    pub fn allocSlice(self: *Self, alloc: Allocator, cap: usize) ![]u8 {
        return switch (self.*) {
            .buffer => |*buf| {
                try buf.ensureUnusedCapacity(alloc, cap);
                return buf.unusedCapacitySlice()[0..cap];
            },
            .file => |*f| {
                // TODO: reuse the buffer across different write destinations,
                // it doesn't make much sense have different buffered writes since we write everything sequentially
                f.buf.clearRetainingCapacity();
                try f.buf.ensureUnusedCapacity(alloc, cap);
                return f.buf.unusedCapacitySlice()[0..cap];
            },
        };
    }

    /// moves items for a buffer, assumes writing to unused capacity slice happened via allocSlice
    pub fn appendAllocated(self: *Self, io: Io, slice: []const u8, cap: usize) !void {
        switch (self.*) {
            .buffer => |*buf| buf.items.len += cap,
            .file => |*f| {
                try f.file.writeStreamingAll(io, slice[0..cap]);
                f.len += cap;
            },
        }
    }
};

test "StreamDestination buffer destination" {
    const alloc = std.testing.allocator;
    const io = std.testing.io;

    var dst = try StreamDestination.initBuffer(alloc, 8);
    defer dst.deinit(io, alloc);

    try dst.appendSlice(io, alloc, "abc");
    try dst.appendSlice(io, alloc, "1234");

    try std.testing.expectEqual(7, dst.len());

    const all = try dst.readAll(io, alloc);
    defer alloc.free(all);
    try std.testing.expectEqualStrings("abc1234", all);
}

test "StreamDestination file destination" {
    const alloc = std.testing.allocator;
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const rootPath = try tmp.dir.realPathFileAlloc(io, ".", alloc);
    defer alloc.free(rootPath);
    const filePath = try std.fs.path.join(alloc, &.{ rootPath, "timestamps.bin" });
    defer alloc.free(filePath);

    const file = try Dir.createFileAbsolute(io, filePath, .{ .truncate = true, .read = true });
    var dst = try StreamDestination.initFile(io, file);
    defer dst.deinit(io, alloc);

    const res = "hello-world";
    try dst.appendSlice(io, alloc, "hello");
    try dst.appendSlice(io, alloc, "-world");
    try std.testing.expectEqual(res.len, dst.len());

    const all = try dst.readAll(io, alloc);
    defer alloc.free(all);
    try std.testing.expectEqualStrings(res, all);

    var verify = try Dir.openFileAbsolute(io, filePath, .{});
    defer verify.close(io);

    var verify_reader = file.reader(io, &.{});
    const onDisk = try verify_reader.interface.allocRemaining(alloc, .limited(std.math.maxInt(usize)));
    defer alloc.free(onDisk);
    try std.testing.expectEqualStrings(res, onDisk);
}
