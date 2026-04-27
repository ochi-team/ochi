const std = @import("std");
const Allocator = std.mem.Allocator;
const Io = std.Io;

pub fn gitHasNoMergeCommits(io: Io, alloc: Allocator) !bool {
    const result = try std.process.run(alloc, io, .{
        .argv = &.{
            "git",
            "log",
            "--merges",
            "--oneline",
            "-1",
        },
    });
    defer alloc.free(result.stdout);
    defer alloc.free(result.stderr);

    return switch (result.term) {
        .exited => |code| code == 0 and std.mem.trim(u8, result.stdout, " \t\r\n").len == 0,
        else => false,
    };
}

pub fn projectIsFormatted(io: Io, alloc: Allocator) !bool {
    const result = try std.process.run(alloc, io, .{
        .argv = &.{ "zig", "fmt", "--check", "." },
    });
    defer alloc.free(result.stdout);
    defer alloc.free(result.stderr);

    return switch (result.term) {
        .exited => |code| code == 0,
        else => false,
    };
}

// TODO: add linter
// TODO: validate git history has no large files (256kb+)
// TODO: ensure the licenses are ok and there are no AGPL
// TODO: restrict constCast usage
// TODO: restrict Self = @This(), use proper type name
// TODO: restrict std.debug.print
// TODO: restrict TODO / FIXME comments, must be the last rule to fix them all
