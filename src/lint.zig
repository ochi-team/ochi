const std = @import("std");
const Allocator = std.mem.Allocator;

pub fn gitHasNoMergeCommits(alloc: Allocator) bool {
    const result = std.process.execv(alloc, &.{
        "git",
        "log",
        "--merges",
        "--oneline",
        "-1",
    }) catch return false;
    return result.stdout.len == 0;
}

// TODO: add formatter
// TODO: add linter
// TODO: validate git history has no large files (256kb+)
// TODO: ensure the licenses are ok and there are no AGPL
