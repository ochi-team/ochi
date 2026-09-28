const std = @import("std");
const Io = std.Io;
const Dir = Io.Dir;

const filenames = @import("../../filenames.zig");
const fs = @import("../../fs.zig");
const strings = @import("../../stds/strings.zig");

const catalog = @import("catalog.zig");

const testing = std.testing;

test "readNames" {
    const Case = struct {
        content: []const u8,
        expected: []const []const u8,
        expectedErr: ?anyerror = null,
    };

    const alloc = testing.allocator;
    const io = testing.io;
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

        const rootPath = try tmp.dir.realPathFileAlloc(io, ".", alloc);
        defer alloc.free(rootPath);
        var tablesFilePathBuf: [std.fs.max_path_bytes]u8 = undefined;
        var tablesFilePathWriter = std.Io.Writer.fixed(&tablesFilePathBuf);
        try std.fs.path.fmtJoin(&.{ rootPath, "tables.json" }).format(&tablesFilePathWriter);
        const tablesFilePath = tablesFilePathWriter.buffered();

        try fs.writeBufferToFileAtomic(io, tablesFilePath, case.content, true);

        if (case.expectedErr) |expectedErr| {
            try testing.expectError(expectedErr, catalog.readNames(io, alloc, tablesFilePath, false));
            continue;
        }

        var tableNames = try catalog.readNames(io, alloc, tablesFilePath, false);
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

test "readNames handles missing file path cases" {
    const Case = struct {
        pathParts: []const []const u8,
        expectedErr: ?anyerror = null,
        expectedFileContent: ?[]const u8 = null,
    };

    const cases = [_]Case{
        .{
            .pathParts = &.{"tables.json"},
            .expectedFileContent = "[]",
        },
        .{
            .pathParts = &.{ "missing", "tables.json" },
            .expectedErr = error.FileNotFound,
        },
    };

    const alloc = testing.allocator;
    const io = testing.io;
    for (cases) |case| {
        var tmp = testing.tmpDir(.{});
        defer tmp.cleanup();

        const rootPath = try tmp.dir.realPathFileAlloc(io, ".", alloc);
        defer alloc.free(rootPath);

        var pathSegments = try std.ArrayList([]const u8).initCapacity(alloc, 1 + case.pathParts.len);
        defer pathSegments.deinit(alloc);
        try pathSegments.append(alloc, rootPath);
        try pathSegments.appendSlice(alloc, case.pathParts);

        var tablesFilePathBuf: [std.fs.max_path_bytes]u8 = undefined;
        var tablesFilePathWriter = std.Io.Writer.fixed(&tablesFilePathBuf);
        try std.fs.path.fmtJoin(pathSegments.items).format(&tablesFilePathWriter);
        const tablesFilePath = tablesFilePathWriter.buffered();

        if (case.expectedErr) |expectedErr| {
            try testing.expectError(expectedErr, catalog.readNames(io, alloc, tablesFilePath, false));
            continue;
        }

        var tableNames = try catalog.readNames(io, alloc, tablesFilePath, false);
        defer tableNames.deinit(alloc);
        try testing.expectEqual(@as(usize, 0), tableNames.items.len);

        if (case.expectedFileContent) |content| {
            const data = try fs.readAll(io, alloc, tablesFilePath);
            defer alloc.free(data);
            try testing.expectEqualStrings(content, data);
        }
    }
}

test "readNames returns error in validate mode when tables file is missing but table dirs exist" {
    const alloc = testing.allocator;
    const io = testing.io;

    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    const rootPath = try tmp.dir.realPathFileAlloc(io, ".", alloc);
    defer alloc.free(rootPath);
    var tablesFilePathBuf: [std.fs.max_path_bytes]u8 = undefined;
    var tablesFilePathWriter = std.Io.Writer.fixed(&tablesFilePathBuf);
    try std.fs.path.fmtJoin(&.{ rootPath, filenames.tables }).format(&tablesFilePathWriter);
    const tablesFilePath = tablesFilePathWriter.buffered();

    var tableDirPathBuf: [std.fs.max_path_bytes]u8 = undefined;
    var tableDirPathWriter = std.Io.Writer.fixed(&tableDirPathBuf);
    try std.fs.path.fmtJoin(&.{ rootPath, "table-a" }).format(&tableDirPathWriter);
    const tableDirPath = tableDirPathWriter.buffered();

    try Dir.createDirAbsolute(io, tableDirPath, .default_dir);

    // Validate mode must fail if tables.json is missing while table dirs already exist
    try testing.expectError(error.TableFileExistsWithNoTableEntry, catalog.readNames(io, alloc, tablesFilePath, true));
    // Validate mode must not auto-create tables.json in this corruption-like state
    try testing.expectError(error.FileNotFound, Dir.accessAbsolute(io, tablesFilePath, .{}));
}

test "validateTablesExist" {
    const alloc = testing.allocator;
    const io = testing.io;
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    const rootPath = try tmp.dir.realPathFileAlloc(io, ".", alloc);
    defer alloc.free(rootPath);

    const tableName = "table-a";
    var tablePathBuf: [std.fs.max_path_bytes]u8 = undefined;
    var tablePathWriter = std.Io.Writer.fixed(&tablePathBuf);
    try std.fs.path.fmtJoin(&.{ rootPath, tableName }).format(&tablePathWriter);
    const tablePath = tablePathWriter.buffered();
    try Dir.createDirAbsolute(io, tablePath, .default_dir);

    const Case = struct {
        tableNames: []const []const u8,
        existingTableNames: []const []const u8,
        expectedErr: ?anyerror = null,
    };

    const cases = [_]Case{
        .{
            .tableNames = &.{},
            .existingTableNames = &.{},
        },
        .{
            .tableNames = &.{"table-a"},
            .existingTableNames = &.{tableName},
        },
        .{
            .tableNames = &.{ tableName, "table-b" },
            .existingTableNames = &.{tableName},
            .expectedErr = error.TableDoesNotExist,
        },
    };

    for (cases) |case| {
        if (case.expectedErr) |expectedErr| {
            try testing.expectError(expectedErr, catalog.validateTablesExist(io, rootPath, case.tableNames));
        } else {
            try catalog.validateTablesExist(io, rootPath, case.tableNames);
        }
    }
}

test "removeUnusedTables" {
    const Case = struct {
        existingTableNames: []const []const u8,
        usedTableNames: []const []const u8,
    };

    const cases = [_]Case{
        .{
            .existingTableNames = &.{"table-a"},
            .usedTableNames = &.{},
        },
        .{
            .existingTableNames = &.{ "table-a", "table-b" },
            .usedTableNames = &.{"table-a"},
        },
        .{
            .existingTableNames = &.{ "table-a", "table-b" },
            .usedTableNames = &.{ "table-a", "table-b" },
        },
    };

    const alloc = testing.allocator;
    const io = testing.io;
    for (cases) |case| {
        var tmp = testing.tmpDir(.{});
        defer tmp.cleanup();

        const rootPath = try tmp.dir.realPathFileAlloc(io, ".", alloc);
        defer alloc.free(rootPath);

        for (case.existingTableNames) |tableName| {
            var tablePathBuf: [std.fs.max_path_bytes]u8 = undefined;
            var tablePathWriter = std.Io.Writer.fixed(&tablePathBuf);
            try std.fs.path.fmtJoin(&.{ rootPath, tableName }).format(&tablePathWriter);
            const tablePath = tablePathWriter.buffered();
            try Dir.createDirAbsolute(io, tablePath, .default_dir);
        }

        try catalog.removeUnusedTables(io, rootPath, case.usedTableNames);

        for (case.existingTableNames) |tableName| {
            var tablePathBuf: [std.fs.max_path_bytes]u8 = undefined;
            var tablePathWriter = std.Io.Writer.fixed(&tablePathBuf);
            try std.fs.path.fmtJoin(&.{ rootPath, tableName }).format(&tablePathWriter);
            const tablePath = tablePathWriter.buffered();
            if (strings.contains(case.usedTableNames, tableName)) {
                try Dir.accessAbsolute(io, tablePath, .{});
            } else {
                try testing.expectError(error.FileNotFound, Dir.accessAbsolute(io, tablePath, .{}));
            }
        }
    }
}
