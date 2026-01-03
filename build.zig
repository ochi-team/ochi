const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.graph.host;

    // 3d party dependencies
    const zeit = b.dependency("zeit", .{
        .target = target,
    });
    const httpz = b.dependency("httpz", .{
        .target = target,
    });
    const snappy = b.dependency("snappy", .{
        .target = target,
    });
    const ymlz = b.dependency("ymlz", .{});

    const cli = b.dependency("cli", .{
        .target = target,
    });
    const zint = b.dependency("zint", .{
        .target = target,
    });

    // C dependencies
    const zstd_dependency = b.dependency("zstd", .{
        .target = target,
    });

    // Create C bindings module
    const cModule = b.createModule(.{
        .root_source_file = b.path("src/lib/c/c.zig"),
        .target = target,
    });
    cModule.linkLibrary(zstd_dependency.artifact("zstd"));

    // inner modules
    const encodeModule = b.createModule(.{
        .root_source_file = b.path("src/lib/encoding/root.zig"),
        .target = target,
        .imports = &.{
            .{ .name = "c", .module = cModule },
        },
    });

    // all the projects imports, main bin and tests
    const imports = [_]std.Build.Module.Import{
        std.Build.Module.Import{ .name = "zeit", .module = zeit.module("zeit") },
        std.Build.Module.Import{ .name = "httpz", .module = httpz.module("httpz") },
        std.Build.Module.Import{ .name = "snappy", .module = snappy.module("snappy") },
        std.Build.Module.Import{ .name = "ymlz", .module = ymlz.module("root") },
        std.Build.Module.Import{ .name = "cli", .module = cli.module("cli") },
        std.Build.Module.Import{ .name = "zint", .module = zint.module("zint") },
        std.Build.Module.Import{ .name = "encoding", .module = encodeModule },
    };

    const exe = b.addExecutable(.{
        .name = "Seqor",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/main.zig"),
            .target = target,
            .imports = &imports,
        }),
    });
    b.installArtifact(exe);

    // add build options to runtime
    const options = b.addOptions();
    exe.root_module.addOptions("build", options);

    // build: version
    const args = &[_][]const u8{ "sh", "-c", "git describe --exact-match --tags HEAD 2>/dev/null || echo \"$(git rev-parse --abbrev-ref HEAD)-$(git rev-parse --short HEAD)\"" };
    const version = b.run(args);
    options.addOption([]const u8, "version", version);

    // run command
    const run_exe = b.addRunArtifact(exe);
    const run_step = b.step("run", "Run the application");
    run_step.dependOn(&run_exe.step);

    // prepare test
    const test_filter = b.option([]const []const u8, "test-filter", "Test filter");
    const unit_tests = b.addTest(.{
        .root_module = b.createModule(.{
            // unit test is server module, not main,
            // because main uses "build" info which is unavailable in unit tests for
            // std.testing.refAllDecls(@This())
            .root_source_file = b.path("src/server.zig"),
            .target = target,
            .imports = &imports,
        }),
        // example to run: zig build test -Dtest-filter="SIGTERM"
        .filters = if (test_filter) |filter| filter else &[_][]const u8{},
        .test_runner = .{ .path = b.path("test_runner.zig"), .mode = .simple },
    });

    // encoding module tests
    const encoding_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/lib/encoding/root.zig"),
            .target = target,
            .imports = &.{
                .{ .name = "c", .module = cModule },
            },
        }),
        .filters = if (test_filter) |filter| filter else &[_][]const u8{},
        .test_runner = .{ .path = b.path("test_runner.zig"), .mode = .simple },
    });

    // build test
    const install_tests = b.addInstallArtifact(unit_tests, .{});
    const install_encoding_tests = b.addInstallArtifact(encoding_tests, .{ .dest_sub_path = "encoding-test" });
    const btest_step = b.step("btest", "Build unit tests");
    btest_step.dependOn(&install_tests.step);
    btest_step.dependOn(&install_encoding_tests.step);

    // test command
    const test_step = b.step("test", "run unit tests");
    const run_unit_tests = b.addSystemCommand(&[_][]const u8{"zig-out/bin/test"});
    run_unit_tests.step.dependOn(&install_tests.step);
    const run_encoding_tests = b.addSystemCommand(&[_][]const u8{"zig-out/bin/encoding-test"});
    run_encoding_tests.step.dependOn(&install_encoding_tests.step);
    test_step.dependOn(&run_unit_tests.step);
    test_step.dependOn(&run_encoding_tests.step);

    // check command
    const check = b.step("check", "Check if compiles");
    check.dependOn(&exe.step);
    check.dependOn(&unit_tests.step);
}
