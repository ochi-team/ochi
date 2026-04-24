const std = @import("std");
const Allocator = std.mem.Allocator;

const fs = @import("../../fs.zig");
const filenames = @import("../../filenames.zig");

const maxFileBytes = 16 * 1024 * 1024;

const TableHeader = @This();

// TODO: try making both values u32
entriesCount: u64 = 0,
blocksCount: u64 = 0,
firstEntry: []const u8 = "",
lastEntry: []const u8 = "",

pub fn deinit(self: TableHeader, alloc: Allocator) void {
    alloc.free(self.firstEntry);
    alloc.free(self.lastEntry);
}

pub fn dupe(self: TableHeader, alloc: Allocator) !TableHeader {
    const firstEntry = try alloc.dupe(u8, self.firstEntry);
    errdefer alloc.free(firstEntry);
    const lastEntry = try alloc.dupe(u8, self.lastEntry);
    errdefer alloc.free(lastEntry);

    return .{
        .blocksCount = self.blocksCount,
        .entriesCount = self.entriesCount,
        .firstEntry = firstEntry,
        .lastEntry = lastEntry,
    };
}

// TODO: TableHeader could have a static bound value to give a stack buffer
pub fn readFile(alloc: Allocator, path: []const u8) !TableHeader {
    var fba = std.heap.stackFallback(1024, alloc);
    const fbaAlloc = fba.get();

    const metadataPath = try std.fs.path.join(fbaAlloc, &[_][]const u8{ path, filenames.header });
    defer fbaAlloc.free(metadataPath);

    var file = std.fs.openFileAbsolute(metadataPath, .{}) catch |err| {
        std.debug.panic("can't open table header '{s}': {s}", .{ metadataPath, @errorName(err) });
    };
    defer file.close();

    const data = file.readToEndAlloc(fbaAlloc, maxFileBytes) catch |err| {
        std.debug.panic("can't read table header '{s}': {s}", .{ metadataPath, @errorName(err) });
    };
    defer fbaAlloc.free(data);

    const parsed = std.json.parseFromSlice(TableHeader, fbaAlloc, data, .{}) catch |err| {
        std.debug.panic("can't parse table header '{s}': {s}", .{ metadataPath, @errorName(err) });
    };
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

pub fn writeFile(self: *const TableHeader, alloc: Allocator, tablePath: []const u8) !void {
    const json = try std.json.Stringify.valueAlloc(alloc, .{
        .entriesCount = self.entriesCount,
        .blocksCount = self.blocksCount,
        .firstEntry = self.firstEntry,
        .lastEntry = self.lastEntry,
    }, .{ .whitespace = .minified });
    defer alloc.free(json);

    const metadataPath = try std.fs.path.join(alloc, &[_][]const u8{ tablePath, filenames.header });
    defer alloc.free(metadataPath);

    try fs.writeBufferValToFile(metadataPath, json);
}

const testing = std.testing;

test "roundtrip file read/write" {
    const alloc = testing.allocator;
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    try tmp.dir.makePath("table");
    const tablePath = try tmp.dir.realPathFileAlloc(io, alloc, "table");
    defer alloc.free(tablePath);

    var tb = TableHeader{
        .blocksCount = 5,
        .entriesCount = 12,
        .firstEntry = "alpha",
        .lastEntry = "omega",
    };

    try tb.writeFile(alloc, tablePath);

    var readTb = try TableHeader.readFile(alloc, tablePath);
    defer readTb.deinit(alloc);

    try testing.expectEqualDeep(tb, readTb);
}
