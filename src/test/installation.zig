const std = @import("std");
const Io = std.Io;
const Allocator = std.mem.Allocator;
const builtin = @import("builtin");
const build = @import("build");
const test_server = @import("server.zig");

const OchiClient = test_server.OchiClient;

const tenant: u64 = 17;
const expectedIDs = [_][]const u8{"install-001"};
const query = "[-60m,now] {env=installation AND service=release} id=install-001";

fn getLatestGitTag(alloc: Allocator, io: Io) ![]const u8 {
    const result = try std.process.run(alloc, io, .{
        .argv = &.{
            "git",
            "describe",
            "--tags",
            "--abbrev=0",
        },
    });
    defer alloc.free(result.stdout);
    defer alloc.free(result.stderr);

    switch (result.term) {
        .exited => |code| {
            if (code != 0) return error.CodeNotZero;
            return alloc.dupe(u8, std.mem.trim(u8, result.stdout, " \t\r\n"));
        },
        else => return error.UnknownResult,
    }
}

test "installationScriptInstallsReleaseBinaryAndDataPersistsAcrossRestart" {
    if (!build.testInstallation) return error.SkipZigTest;

    const alloc = std.testing.allocator;
    const io = std.testing.io;

    var repoPathBuf: [std.fs.max_path_bytes]u8 = undefined;
    var n = try std.Io.Dir.cwd().realPathFile(io, ".", &repoPathBuf);
    const repoPath = repoPathBuf[0..n];

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var tmpPathBuf: [std.fs.max_path_bytes]u8 = undefined;
    n = try tmp.dir.realPathFile(io, ".", &tmpPathBuf);
    const tmpPath = tmpPathBuf[0..n];

    const scriptPath = try std.fs.path.join(alloc, &.{ repoPath, "scripts", "install.sh" });
    defer alloc.free(scriptPath);
    const version = try getLatestGitTag(alloc, io);
    defer alloc.free(version);
    var binaryPathBuf: [std.fs.max_path_bytes]u8 = undefined;
    const binaryPath = try installRelease(
        alloc,
        io,
        tmpPath,
        scriptPath,
        &binaryPathBuf,
        version,
        @tagName(builtin.os.tag),
        @tagName(builtin.cpu.arch),
    );

    const dataPath = try std.fs.path.join(alloc, &.{ tmpPath, ".ochi" });
    defer alloc.free(dataPath);

    var client: OchiClient = .{
        .host = "http://localhost:9014",
        .client = .{
            .allocator = alloc,
            .io = io,
        },
    };
    defer client.client.deinit();

    const nowNs: u64 = @intCast(std.Io.Timestamp.now(io, .real).nanoseconds);
    const body = try std.fmt.allocPrint(
        alloc,
        "{{\"streams\":[{{\"stream\":{{\"env\":\"installation\",\"service\":\"release\"}},\"values\":[[\"{d}\",\"installation persistence smoke\",{{\"id\":\"install-001\"}}]]}}]}}",
        .{nowNs},
    );
    defer alloc.free(body);

    {
        var child = try startOchi(io, binaryPath, dataPath);
        defer child.kill(io);

        try client.waitUntilReady(io, alloc, .fromSeconds(5));
        try client.ingestLokiJson(alloc, tenant, body);
        try client.flush(alloc, tenant);
        try client.expectQueryIDs(alloc, tenant, query, &expectedIDs);

        child.kill(io);
    }

    {
        var child = try startOchi(io, binaryPath, dataPath);
        defer child.kill(io);

        try client.waitUntilReady(io, alloc, .fromSeconds(5));
        try client.expectQueryIDs(alloc, tenant, query, &expectedIDs);

        child.kill(io);
    }
}

fn installRelease(
    alloc: std.mem.Allocator,
    io: std.Io,
    cwd: []const u8,
    scriptPath: []const u8,
    buf: []u8,
    version: []const u8,
    os: []const u8,
    arch: []const u8,
) ![]u8 {
    const result = try std.process.run(alloc, io, .{
        .argv = &.{ "sh", scriptPath, version, os, arch },
        .cwd = .{ .path = cwd },
        .stdout_limit = .limited(2 * 1024),
        .stderr_limit = .limited(2 * 1024),
    });
    defer alloc.free(result.stdout);
    defer alloc.free(result.stderr);

    try expectExited(result.term);
    const binaryPath = std.mem.trim(u8, result.stdout, " \t\r\n");
    if (binaryPath.len == 0) {
        return error.InstallationScriptDidNotReturnBinaryPath;
    }

    @memcpy(buf, binaryPath);
    return buf[0..binaryPath.len];
}

fn startOchi(io: std.Io, binaryPath: []const u8, cwd: []const u8) !std.process.Child {
    return std.process.spawn(io, .{
        .argv = &.{binaryPath},
        .cwd = .{ .path = cwd },
        .stdin = .ignore,
        .stdout = .ignore,
        .stderr = .ignore,
    });
}

fn expectExited(term: std.process.Child.Term) !void {
    switch (term) {
        .exited => |code| try std.testing.expectEqual(0, code),
        else => return error.ProcessFailed,
    }
}
