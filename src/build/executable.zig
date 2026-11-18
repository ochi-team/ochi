const std = @import("std");
const options = @import("options.zig");

pub const Id = enum { ochi, scooby, sack };

pub const RootOptions = struct {
    version: []const u8,
    release: bool,
    pprof: bool,
};

pub const ExecutableSpec = struct {
    id: Id,
    root: std.Build.LazyPath,
    imports: []const std.Build.Module.Import = &.{},
    options: ?RootOptions = null,
};

pub const Artifacts = struct {
    byId: std.EnumArray(Id, ?*std.Build.Step.Compile) = .initFill(null),

    pub fn get(self: Artifacts, id: Id) *std.Build.Step.Compile {
        return self.byId.get(id).?;
    }
};

pub fn addExecutables(
    b: *std.Build,
    target: std.Build.ResolvedTarget,
    optimize: std.builtin.OptimizeMode,
    specs: []const ExecutableSpec,
) Artifacts {
    var artifacts: Artifacts = .{};
    for (specs) |spec| {
        const artifact = b.addExecutable(.{
            .name = @tagName(spec.id),
            .root_module = b.createModule(.{
                .root_source_file = spec.root,
                .target = target,
                .optimize = optimize,
                .imports = spec.imports,
            }),
        });
        if (spec.options) |rootOptions| {
            options.addRootOptions(b, artifact, rootOptions.version, rootOptions.release, rootOptions.pprof);
        }
        artifacts.byId.set(spec.id, artifact);
    }
    return artifacts;
}
