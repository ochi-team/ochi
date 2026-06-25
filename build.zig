const std = @import("std");

fn addOption(
    b: *std.Build,
    compile: *std.Build.Step.Compile,
    release: bool,
    testInstallation: bool,
) void {
    // add build options to runtime
    const options = b.addOptions();
    compile.root_module.addOptions("build", options);

    // build: version
    const args = &[_][]const u8{ "sh", "-c", "git describe --exact-match --tags HEAD 2>/dev/null || echo \"$(git rev-parse --abbrev-ref HEAD)-$(git rev-parse --short HEAD)\"" };
    const version = b.run(args);
    options.addOption([]const u8, "version", version);
    options.addOption(bool, "release", release);
    options.addOption(bool, "testInstallation", testInstallation);
}

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    // explicit release flag since we use ReleaseSafe for profiling
    const release = b.option(bool, "release", "Release") orelse false;
    // Allow the user to enable or disable Tracy support with a build flag
    const tracy_enabled = b.option(
        bool,
        "tracy",
        "Build with Tracy support.",
    ) orelse false;
    // test filter to run a specific set of tests
    const test_filter = b.option([]const []const u8, "test-filter", "Test filter");
    // test installation, designated post-release only test
    const testInstallation = b.option(
        bool,
        "testInstallation",
        "Run installation tests.",
    ) orelse false;

    // 3d party dependencies
    const zeit = b.dependency("zeit", .{
        .target = target,
        .optimize = optimize,
    });
    const httpz = b.dependency("httpz", .{
        .target = target,
        .optimize = optimize,
    });
    const snappy = b.dependency("snappy", .{
        .target = target,
        .optimize = optimize,
    });
    const zint = b.dependency("zint", .{
        .target = target,
        .optimize = optimize,
    });
    const metrics = b.dependency("metrics", .{
        .target = target,
        .optimize = optimize,
    });
    const logz = b.dependency("logz", .{
        .target = target,
        .optimize = optimize,
    });
    const tracy = b.dependency("tracy", .{
        .target = target,
        .optimize = optimize,
    });

    // C dependencies
    const zstd_dependency = b.dependency("zstd", .{
        .target = target,
        .optimize = optimize,
    });

    // Create C bindings module
    const cModule = b.createModule(.{
        .root_source_file = b.path("src/lib/c/c.zig"),
        .target = target,
        .optimize = optimize,
    });
    cModule.linkLibrary(zstd_dependency.artifact("zstd"));

    // inner modules
    const loggerModule = b.createModule(.{
        .root_source_file = b.path("src/observe/Logger.zig"),
        .target = target,
        .optimize = optimize,
        .imports = &.{
            .{ .name = "logz", .module = logz.module("logz") },
        },
    });
    const encodeModule = b.createModule(.{
        .root_source_file = b.path("src/lib/encoding/root.zig"),
        .target = target,
        .optimize = optimize,
        .imports = &.{
            .{ .name = "c", .module = cModule },
            .{ .name = "logging", .module = loggerModule },
        },
    });

    // all the projects imports, main bin and tests
    const tracyImpl = if (tracy_enabled) "tracy_impl_enabled" else "tracy_impl_disabled";
    const imports = [_]std.Build.Module.Import{
        std.Build.Module.Import{ .name = "zeit", .module = zeit.module("zeit") },
        std.Build.Module.Import{ .name = "httpz", .module = httpz.module("httpz") },
        std.Build.Module.Import{ .name = "snappy", .module = snappy.module("snappy") },
        std.Build.Module.Import{ .name = "zint", .module = zint.module("zint") },
        std.Build.Module.Import{ .name = "metrics", .module = metrics.module("metrics") },
        std.Build.Module.Import{ .name = "logz", .module = logz.module("logz") },
        std.Build.Module.Import{ .name = "logging", .module = loggerModule },
        std.Build.Module.Import{ .name = "tracy", .module = tracy.module("tracy") },
        std.Build.Module.Import{ .name = "tracy_impl", .module = tracy.module(tracyImpl) },
        std.Build.Module.Import{ .name = "c", .module = cModule },
        std.Build.Module.Import{ .name = "encoding", .module = encodeModule },
    };

    const exe = b.addExecutable(.{
        .name = "Ochi",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/main.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &imports,
            // .sanitize_thread = optimize == .Debug,
            // TODO: try using .strip option to reduce the size of the binary,
            // concern is only to collect debug information (logging, crash events)
        }),
    });

    b.installArtifact(exe);
    addOption(b, exe, release, testInstallation);

    const scooby_exe = b.addExecutable(.{
        .name = "scooby",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/scooby.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &imports,
        }),
    });

    b.installArtifact(scooby_exe);

    // run command
    const run_exe = b.addRunArtifact(exe);
    const run_step = b.step("run", "Run the application");
    run_step.dependOn(&run_exe.step);

    const run_scooby = b.addRunArtifact(scooby_exe);
    if (b.args) |args| {
        run_scooby.addArgs(args);
    }
    const scooby_step = b.step("scooby", "Run the Scooby data debug tool");
    scooby_step.dependOn(&run_scooby.step);

    // prepare test
    const unit_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/main.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &imports,
        }),
        // example to run: zig build test -Dtest-filter="SIGTERM"
        .filters = if (test_filter) |filter| filter else &[_][]const u8{},
        .test_runner = .{ .path = b.path("test_runner.zig"), .mode = .server },
    });
    addOption(b, unit_tests, release, testInstallation);

    // encoding module tests
    const encoding_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/lib/encoding/root.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "c", .module = cModule },
                .{ .name = "logging", .module = loggerModule },
            },
        }),
        .filters = if (test_filter) |filter| filter else &[_][]const u8{},
        .test_runner = .{ .path = b.path("test_runner.zig"), .mode = .server },
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
    check.dependOn(&scooby_exe.step);
    check.dependOn(&unit_tests.step);
}
