const std = @import("std");
const Allocator = std.mem.Allocator;

const fs = @import("../../fs.zig");
const Filenames = @import("../../Filenames.zig");

const maxFileBytes = 16 * 1024 * 1024;

const TableHeader = @This();

// TODO: try making both values u32
itemsCount: u64 = 0,
blocksCount: u64 = 0,
firstItem: []const u8 = undefined,
lastItem: []const u8 = undefined,

pub fn deinit(self: TableHeader, alloc: Allocator) void {
    alloc.free(self.firstItem);
    alloc.free(self.lastItem);
}

pub fn readFile(alloc: Allocator, path: []const u8) !TableHeader {
    var fba = std.heap.stackFallback(1024, alloc);
    const fbaAlloc = fba.get();

    const metadataPath = try std.fs.path.join(fbaAlloc, &[_][]const u8{ path, Filenames.header });
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

    const firstItem = try alloc.dupe(u8, parsed.value.firstItem);
    errdefer alloc.free(firstItem);
    const lastItem = try alloc.dupe(u8, parsed.value.lastItem);

    return .{
        .blocksCount = parsed.value.blocksCount,
        .itemsCount = parsed.value.itemsCount,
        .firstItem = firstItem,
        .lastItem = lastItem,
    };
}

pub fn writeFile(self: *const TableHeader, alloc: Allocator, tablePath: []const u8) !void {
    const json = try std.json.Stringify.valueAlloc(alloc, .{
        .itemsCount = self.itemsCount,
        .blocksCount = self.blocksCount,
        .firstItem = self.firstItem,
        .lastItem = self.lastItem,
    }, .{ .whitespace = .minified });
    defer alloc.free(json);

    const metadataPath = try std.fs.path.join(alloc, &[_][]const u8{ tablePath, Filenames.header });
    defer alloc.free(metadataPath);

    try fs.writeBufferValToFile(metadataPath, json);
}

const testing = std.testing;

test "roundtrip file read/write" {
    const alloc = testing.allocator;
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    try tmp.dir.makePath("table");
    const tablePath = try tmp.dir.realpathAlloc(alloc, "table");
    defer alloc.free(tablePath);

    var tb = TableHeader{
        .blocksCount = 5,
        .itemsCount = 12,
        .firstItem = "alpha",
        .lastItem = "omega",
    };

    try tb.writeFile(alloc, tablePath);

    var readTb = try TableHeader.readFile(alloc, tablePath);
    defer readTb.deinit(alloc);

    try testing.expectEqualDeep(tb, readTb);
}
