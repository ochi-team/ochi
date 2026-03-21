const std = @import("std");
const Allocator = std.mem.Allocator;

pub const FileDestination = struct {
    file: std.fs.File,
    len: usize,
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

    pub fn initFile(file: std.fs.File) !Self {
        const stat = try file.stat();
        const initialLen: usize = @intCast(stat.size);
        try file.seekTo(stat.size);
        return .{ .file = .{ .file = file, .len = initialLen } };
    }

    pub fn deinit(self: *Self, allocator: Allocator) void {
        switch (self.*) {
            .buffer => |*buf| buf.deinit(allocator),
            .file => |*f| f.file.close(),
        }
    }

    pub fn len(self: *const Self) usize {
        return switch (self.*) {
            .buffer => |buf| buf.items.len,
            .file => |f| f.len,
        };
    }

    pub fn appendSlice(self: *Self, allocator: Allocator, src: []const u8) !void {
        switch (self.*) {
            .buffer => |*buf| try buf.appendSlice(allocator, src),
            .file => |*f| {
                try f.file.writeAll(src);
                f.len += src.len;
            },
        }
    }

    fn readAll(self: *Self, allocator: Allocator) ![]u8 {
        switch (self.*) {
            .buffer => |*buf| return allocator.dupe(u8, buf.items),
            .file => |*f| {
                const size: usize = @intCast((try f.file.stat()).size);
                const dst = try allocator.alloc(u8, size);
                errdefer allocator.free(dst);

                try f.file.seekTo(0);
                const readN = try f.file.readAll(dst);
                std.debug.assert(readN == size);
                try f.file.seekTo(@intCast(f.len));
                return dst;
            },
        }
    }

    pub fn asSliceAssumeBuffer(self: *const Self) []const u8 {
        return switch (self.*) {
            .buffer => |buf| buf.items,
            .file => unreachable,
        };
    }

    pub fn allocSlice(self: *Self, alloc: Allocator, cap: usize) ![]u8 {
        return switch (self.*) {
            .buffer => |*buf| {
                try buf.ensureUnusedCapacity(alloc, cap);
                return buf.unusedCapacitySlice()[0..cap];
            },
            .file => |_| {
                // TODO: this is broken,
                // it returns a slice and if we return an error in between allocSLice and appendAllocated
                // we leak the memory:
                // 1. create a state to hold allocated buffer, it allows to reuse it across write operations
                // and reliably free
                // 2. reuse the buffer across different write destinations,
                // it doesn't make much sense have different buffered writes since we write everything sequentially
                return alloc.alloc(u8, cap);
            },
        };
    }

    /// moves items for a buffer, assumes writing to unused capacity slice happened via allocSlice
    pub fn appendAllocated(self: *Self, alloc: Allocator, slice: []const u8, cap: usize) !void {
        switch (self.*) {
            .buffer => |*buf| buf.items.len += cap,
            .file => |*f| {
                defer alloc.free(slice);
                try f.file.writeAll(slice[0..cap]);
                f.len += cap;
            },
        }
    }
};

test "StreamDestination buffer destination" {
    const alloc = std.testing.allocator;
    var dst = try StreamDestination.initBuffer(alloc, 8);
    defer dst.deinit(alloc);

    try dst.appendSlice(alloc, "abc");
    try dst.appendSlice(alloc, "1234");

    try std.testing.expectEqual(7, dst.len());

    const all = try dst.readAll(alloc);
    defer alloc.free(all);
    try std.testing.expectEqualStrings("abc1234", all);
}

test "StreamDestination file destination" {
    const alloc = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const rootPath = try tmp.dir.realpathAlloc(alloc, ".");
    defer alloc.free(rootPath);
    const filePath = try std.fs.path.join(alloc, &.{ rootPath, "timestamps.bin" });
    defer alloc.free(filePath);

    const file = try std.fs.createFileAbsolute(filePath, .{ .truncate = true, .read = true });
    var dst = try StreamDestination.initFile(file);
    defer dst.deinit(alloc);

    const res = "hello-world";
    try dst.appendSlice(alloc, "hello");
    try dst.appendSlice(alloc, "-world");
    try std.testing.expectEqual(res.len, dst.len());

    const all = try dst.readAll(alloc);
    defer alloc.free(all);
    try std.testing.expectEqualStrings(res, all);

    var verify = try std.fs.openFileAbsolute(filePath, .{});
    defer verify.close();
    const onDisk = try verify.readToEndAlloc(alloc, std.math.maxInt(usize));
    defer alloc.free(onDisk);
    try std.testing.expectEqualStrings(res, onDisk);
}
