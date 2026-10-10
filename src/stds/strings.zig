const std = @import("std");

pub fn contains(strings: []const []const u8, item: []const u8) bool {
    for (strings) |string| {
        if (std.mem.eql(u8, string, item)) return true;
    }
    return false;
}

pub fn findPrefix(first: []const u8, second: []const u8) []const u8 {
    const n = @min(first.len, second.len);
    if (n == 0) return "";
    var i: usize = 0;
    while (i < n and first[i] == second[i]) : (i += 1) {}
    return first[0..@intCast(i)];
}

pub const StringsTable = struct {
    const Error = error{
        BoundError,
    };
    // TODO: bench against null terminated strings,
    // it takes looping over the entire buffer on reading, but doesn't require items array
    const Slice = struct {
        start: u32,
        end: u32,
    };

    buf: std.ArrayList(u8),
    items: std.ArrayList(Slice),

    pub fn init(alloc: std.mem.Allocator, size: usize, len: usize) !StringsTable {
        var buf: std.ArrayList(u8) = try .initCapacity(alloc, size);
        errdefer buf.deinit(alloc);
        var items: std.ArrayList(Slice) = try .initCapacity(alloc, len);
        errdefer items.deinit(alloc);

        return .{
            .buf = buf,
            .items = items,
        };
    }

    pub fn deinit(self: *StringsTable, alloc: std.mem.Allocator) void {
        self.items.deinit(alloc);
        self.buf.deinit(alloc);
    }

    pub fn intern(self: *StringsTable, alloc: std.mem.Allocator, buf: []const u8) !Slice {
        if (self.buf.items.len + buf.len >= std.math.maxInt(u32)) return Error.BoundError;

        const start: u32 = @intCast(self.buf.items.len);
        try self.buf.appendSlice(alloc, buf);
        const end: u32 = @intCast(self.buf.items.len);

        const slice: Slice = .{ .start = start, .end = end };
        try self.items.append(alloc, slice);
        return slice;
    }

    pub fn writer(self: *StringsTable) std.Io.Writer {
        return std.Io.Writer.fromArrayList(&self.buf);
    }

    pub fn flushWriter(self: *StringsTable, alloc: std.mem.Allocator, w: *const std.Io.Writer) !Slice {
        if (self.buf.items.len + w.end >= std.math.maxInt(u32)) return Error.BoundError;

        const start: u32 = @intCast(self.buf.items.len - w.end);
        const end: u32 = @intCast(self.buf.items.len);

        const slice: Slice = .{ .start = start, .end = end };
        try self.items.append(alloc, slice);
        return slice;
    }
};
