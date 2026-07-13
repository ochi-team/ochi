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
        .argv = &.{ "zig", "fmt", "--check", ".", "--exclude", "zig-pkg" },
    });
    defer alloc.free(result.stdout);
    defer alloc.free(result.stderr);

    return switch (result.term) {
        .exited => |code| code == 0,
        else => false,
    };
}

const testing = std.testing;

test gitHasNoMergeCommits {
    const alloc = testing.allocator;
    const io = testing.io;

    const noMergeCommits = try gitHasNoMergeCommits(io, alloc);
    try testing.expect(noMergeCommits);
}

test "projectIsFormatted" {
    const alloc = testing.allocator;
    const io = testing.io;

    const isFormatted = try projectIsFormatted(io, alloc);
    try testing.expect(isFormatted);
}

// TODO: add linter
// TODO: limit the funfctions size to 45 lines, length to 120 (class 13 inch monitor)
// TODO: ban discarding returned param
// TODO: make an ast automation tool that is able to check *const must be used instead of *
// TODO: validate git history has no large files (256kb+)
// TODO: ensure the licenses are ok and there are no AGPL
// TODO: restrict constCast usage
// TODO: restrict Self = @This(), use proper type name
// TODO: restrict std.debug.print
// TODO: add a ast grep rule to use *const isntead of * everywhere as possible
// TODO: restrict short path to error.xxx, use only full path
// TODO: restrict TODO / FIXME comments, must be the last rule to fix them all
// TODO: after last TODO we audit the codebase for the following:
// 1. state writing, it must be properly re-cleaned, allocate first, encode, then append
// 2. refactor tests: all the test data and the fixtures must be in its package,
// 3. test all failures: allocations, io, disk, etc.
// 4. setup limits to everything
// 5. fix allocations
// 6. work on improved testing: failovers, branch testing, fuzz, properties
