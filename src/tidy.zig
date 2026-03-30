const std = @import("std");
const Allocator = std.mem.Allocator;

pub fn gitHasNoMergeCommits(alloc: Allocator) !bool {
    // TODO: implement it
    _ = alloc;
    return true;
    // const result = std.process.execv(alloc, &.{
    //     "git",
    //     "log",
    //     "--merges",
    //     "--oneline",
    //     "-1",
    // });
    // return result.stdout.len == 0;
}

// TODO: add formatter
// TODO: add linter
// TODO: validate git history has no large files (256kb+)
// TODO: ensure the licenses are ok and there are no AGPL
// TODO: restrict TODO / FIXME comments, must be the last rule to fix them all
