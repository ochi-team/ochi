const std = @import("std");

const testing = @import("testing.zig");

/// Coverage inputs, output paths, and public steps.
pub const CoverageSpec = struct {
    suites: []const testing.TestSuite,
    includePath: []const u8,
    outputPath: []const u8,
    step: []const u8,
    description: []const u8,
};

/// Wires per-suite kcov runs and their merged report.
pub fn addCoverage(b: *std.Build, spec: CoverageSpec) void {
    const step = b.step(spec.step, spec.description);

    const dataPath = b.fmt("{s}-data", .{spec.outputPath});
    const directory = b.addSystemCommand(&.{ "mkdir", "-p", dataPath });
    directory.has_side_effects = true;
    const merge = b.addSystemCommand(&.{ "kcov", "--merge", spec.outputPath });
    step.dependOn(&merge.step);

    for (spec.suites) |suite| {
        const output = b.pathJoin(&.{ dataPath, @tagName(suite.id) });
        const run = b.addSystemCommand(&.{
            "kcov",
            "--clean",
            "--skip-solibs",
            b.fmt("--include-path={s}", .{spec.includePath}),
            output,
        });
        run.addArtifactArg(suite.tests);
        run.step.dependOn(&directory.step);
        merge.addArg(output);
        merge.step.dependOn(&run.step);
    }
}
