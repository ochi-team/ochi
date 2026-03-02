const std = @import("std");
const Allocator = std.mem.Allocator;

const Filenames = @import("../../Filenames.zig");
const fs = @import("../../fs.zig");

// nothing specific, we simply don't expected a small json file to be larger than that
const maxFileBytes = 16 * 1024 * 1024;

pub fn readNames(alloc: Allocator, tablesFilePath: []const u8, comptime validate: bool) !std.ArrayList([]const u8) {
    if (std.fs.openFileAbsolute(tablesFilePath, .{})) |file| {
        defer file.close();

        const data = try file.readToEndAlloc(alloc, maxFileBytes);
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
                var parentDir = std.fs.openDirAbsolute(parentPath, .{ .iterate = true }) catch |openErr| switch (openErr) {
                    error.FileNotFound => return error.TableParentDirNotFound,
                    else => return openErr,
                };
                defer parentDir.close();

                var it = parentDir.iterate();
                while (try it.next()) |entry| {
                    if (entry.kind == .directory or entry.kind == .sym_link) {
                        return error.TableFileExistsWithNoTableEntry;
                    }
                }
            }

            const f = try std.fs.createFileAbsolute(tablesFilePath, .{});
            defer f.close();
            try f.writeAll("[]");
            std.debug.print("write initial state to '{s}'\n", .{tablesFilePath});
            return .empty;
        },
        else => return err,
    }
}

const testing = std.testing;

test "readNames" {
    const Case = struct {
        content: []const u8,
        expected: []const []const u8,
        expectedErr: ?anyerror = null,
    };

    const alloc = testing.allocator;
    const cases = [_]Case{
        .{
            .content = "[\"table-a\",\"table-b\"]",
            .expected = &.{ "table-a", "table-b" },
        },
        .{
            .content = "not-json",
            .expected = &.{},
            .expectedErr = error.SyntaxError,
        },
        .{
            .content = "{\"name\":\"table-a\"}",
            .expected = &.{},
            .expectedErr = error.TablesFileExpectedArray,
        },
        .{
            .content = "[\"table-a\",42]",
            .expected = &.{},
            .expectedErr = error.TablesFileExpectedStringItems,
        },
    };

    for (cases) |case| {
        var tmp = testing.tmpDir(.{});
        defer tmp.cleanup();

        const rootPath = try tmp.dir.realpathAlloc(alloc, ".");
        defer alloc.free(rootPath);
        const tablesFilePath = try std.fs.path.join(alloc, &.{ rootPath, "tables.json" });
        defer alloc.free(tablesFilePath);

        try fs.writeBufferToFileAtomic(alloc, tablesFilePath, case.content, true);

        if (case.expectedErr) |expectedErr| {
            try testing.expectError(expectedErr, readNames(alloc, tablesFilePath, false));
            continue;
        }

        var tableNames = try readNames(alloc, tablesFilePath, false);
        defer {
            for (tableNames.items) |name| alloc.free(name);
            tableNames.deinit(alloc);
        }
        try testing.expectEqual(case.expected.len, tableNames.items.len);
        for (case.expected, 0..) |expected, i| {
            try testing.expectEqualStrings(expected, tableNames.items[i]);
        }
    }
}

test "readNames creates empty file when missing" {
    const alloc = testing.allocator;

    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    const rootPath = try tmp.dir.realpathAlloc(alloc, ".");
    defer alloc.free(rootPath);
    const tablesFilePath = try std.fs.path.join(alloc, &.{ rootPath, "tables.json" });
    defer alloc.free(tablesFilePath);

    var tableNames = try readNames(alloc, tablesFilePath, false);
    defer tableNames.deinit(alloc);
    try testing.expectEqual(@as(usize, 0), tableNames.items.len);

    const data = try fs.readAll(alloc, tablesFilePath);
    defer alloc.free(data);
    try testing.expectEqualStrings("[]", data);
}

test "readNames returns error when missing parent path cannot be created" {
    const alloc = testing.allocator;

    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    const rootPath = try tmp.dir.realpathAlloc(alloc, ".");
    defer alloc.free(rootPath);
    const missingDirTablesPath = try std.fs.path.join(alloc, &.{ rootPath, "missing", "tables.json" });
    defer alloc.free(missingDirTablesPath);

    try testing.expectError(error.FileNotFound, readNames(alloc, missingDirTablesPath, false));
}

test "readNames returns error in validate mode when tables file is missing but table dirs exist" {
    const alloc = testing.allocator;

    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    const rootPath = try tmp.dir.realpathAlloc(alloc, ".");
    defer alloc.free(rootPath);
    const tablesFilePath = try std.fs.path.join(alloc, &.{ rootPath, Filenames.tables });
    defer alloc.free(tablesFilePath);
    const tableDirPath = try std.fs.path.join(alloc, &.{ rootPath, "table-a" });
    defer alloc.free(tableDirPath);

    try std.fs.makeDirAbsolute(tableDirPath);

    // Validate mode must fail if tables.json is missing while table dirs already exist
    try testing.expectError(error.TableFileExistsWithNoTableEntry, readNames(alloc, tablesFilePath, true));
    // Validate mode must not auto-create tables.json in this corruption-like state
    try testing.expectError(error.FileNotFound, std.fs.accessAbsolute(tablesFilePath, .{}));
}
