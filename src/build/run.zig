const std = @import("std");

pub const RunSpec = struct {
    artifact: *std.Build.Step.Compile,
    step: []const u8,
    description: []const u8,
    forwardArgs: bool,
};

pub fn addRuns(b: *std.Build, specs: []const RunSpec) void {
    for (specs) |spec| {
        const run = b.addRunArtifact(spec.artifact);
        if (spec.forwardArgs) {
            if (b.args) |args| run.addArgs(args);
        }
        b.step(spec.step, spec.description).dependOn(&run.step);
    }
}
