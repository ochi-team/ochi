const std = @import("std");
const Allocator = std.mem.Allocator;
const Io = std.Io;
const Dir = Io.Dir;

const fs = @import("../../fs.zig");
const Logger = @import("logging");
const strings = @import("../../stds/strings.zig");

// nothing specific, we simply don't expected a small json file to be larger than that
const maxFileBytes = 16 * 1024 * 1024;

pub fn readNames(
    io: Io,
    alloc: Allocator,
    tablesFilePath: []const u8,
    comptime validate: bool,
) !std.ArrayList([]const u8) {
    if (Dir.openFileAbsolute(io, tablesFilePath, .{})) |file| {
        defer file.close(io);

        var file_reader = file.reader(io, &.{});
        const data = try file_reader.interface.allocRemaining(alloc, .limited(maxFileBytes));
        defer alloc.free(data);

        const parsed = try std.json.parseFromSlice(std.json.Value, alloc, data, .{});
        defer parsed.deinit();

        if (parsed.value != .array) {
            return error.TablesFileExpectedArray;
        }

        var tableNames = try std.ArrayList([]const u8).initCapacity(alloc, parsed.value.array.items.len);
        errdefer {
            for (tableNames.items) |name| alloc.free(name);
            tableNames.deinit(alloc);
        }
        for (parsed.value.array.items) |item| {
            if (item != .string) {
                return error.TablesFileExpectedStringItems;
            }
            const nameCopy = try alloc.dupe(u8, item.string);
            try tableNames.append(alloc, nameCopy);
        }

        return tableNames;
    } else |err| switch (err) {
        error.FileNotFound => {
            if (validate) {
                const parentPath = std.fs.path.dirname(tablesFilePath) orelse return error.TableParentDirNotFound;
                var parentDir = Dir.openDirAbsolute(io, parentPath, .{ .iterate = true }) catch |openErr| switch (openErr) {
                    error.FileNotFound => return error.TableParentDirNotFound,
                    else => return openErr,
                };
                defer parentDir.close(io);

                var it = parentDir.iterate();
                while (try it.next(io)) |entry| {
                    if (entry.kind == .directory or entry.kind == .sym_link) {
                        return error.TableFileExistsWithNoTableEntry;
                    }
                }
            }

            const f = try Dir.createFileAbsolute(io, tablesFilePath, .{});
            defer f.close(io);
            try f.writeStreamingAll(io, "[]");
            Logger.log(.info, "write initial table catalog state", .{ .path = tablesFilePath });
            return .empty;
        },
        else => return err,
    }
}

pub fn validateTablesExist(io: Io, path: []const u8, tableNames: []const []const u8) !void {
    var tablePathBuf: [std.fs.max_path_bytes]u8 = undefined;
    var tablePathWriter = std.Io.Writer.fixed(&tablePathBuf);

    for (tableNames) |tableName| {
        try std.fs.path.fmtJoin(&.{ path, tableName }).format(&tablePathWriter);
        Dir.accessAbsolute(io, tablePathWriter.buffered(), .{}) catch |err| switch (err) {
            error.FileNotFound => return error.TableDoesNotExist,
            else => return err,
        };
        tablePathWriter.end = 0;
    }
}

pub fn removeUnusedTables(io: Io, path: []const u8, tableNames: []const []const u8) !void {
    var dir = try std.Io.Dir.cwd().openDir(io, path, .{ .iterate = true });
    defer dir.close(io);

    var it = dir.iterate();
    while (try it.next(io)) |entry| {
        if (entry.kind != .directory and entry.kind != .sym_link) continue;
        if (strings.contains(tableNames, entry.name)) continue;

        var pathToDeleteBuf: [std.fs.max_path_bytes]u8 = undefined;
        var pathToDeleteWriter = std.Io.Writer.fixed(&pathToDeleteBuf);
        try std.fs.path.fmtJoin(&.{ path, entry.name }).format(&pathToDeleteWriter);
        Logger.log(.info, "removing unused table path", .{ .path = pathToDeleteWriter.buffered() });
        try fs.deleteTreeAbsolute(io, pathToDeleteWriter.buffered());
    }
}

test {
    _ = @import("catalog_test.zig");
}
