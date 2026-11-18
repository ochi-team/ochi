const std = @import("std");

/// an executable installation
pub const InstallSpec = struct {
    artifact: *std.Build.Step.Compile,
    options: std.Build.Step.InstallArtifact.Options = .{},
    destination: union(enum) {
        default,
        step: struct {
            name: []const u8,
            description: []const u8,
        },
    },
};

/// wires installations to the steps
pub fn addInstalls(b: *std.Build, specs: []const InstallSpec) void {
    for (specs) |spec| {
        const install = b.addInstallArtifact(spec.artifact, spec.options);
        const destination = switch (spec.destination) {
            .default => b.getInstallStep(),
            .step => |step| b.step(step.name, step.description),
        };
        destination.dependOn(&install.step);
    }
}
