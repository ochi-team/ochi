// build - builds the ochi main server executable
// - release - flags the executable as the stable release
// - pprof - enable allocator pprof api, exposes allocations pprof endpoint
// zig build run - same as zig build, but runs the resulted executable
//
// build btest builds all the tests binary
// - test-filter - string fitler to run specific tests
// - allocationFailures - injects allocation failures, runs tests longer since runs more extensive failures
// build test - same as btest, but runs the resulted executable
//
// build test-main - runs only main app tests, supports all the test flags
// build test-{encoding,cli,sack,logging,etc.} - runs only designated package tests
//
// build sack - runs soak testing from apps/sack
//
// build kcov - runs unit tests and reports its coverage
//
// build check - editor command, used internally in zls

const std = @import("std");

const options = @import("src/build/options.zig");
const testing = @import("src/build/testing.zig");

pub fn build(b: *std.Build) void {
    // ------- options -------

    // TODO: find what is baseline and how to setup a proper target including v3/v4
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    // explicit release flag since we use ReleaseSafe for profiling
    const release = b.option(bool, "release", "Release") orelse false;
    // Allow the user to enable or disable Tracy support with a build flag
    const tracyEnabled = b.option(
        bool,
        "tracy",
        "Build with Tracy support.",
    ) orelse false;
    // pprof profile
    const pprofEnabled = b.option(
        bool,
        "pprof",
        "Enable pprof profile",
    ) orelse false;
    const versionArgs = &[_][]const u8{ "sh", "-c", "git describe --exact-match --tags HEAD 2>/dev/null || echo \"$(git rev-parse --abbrev-ref HEAD)-$(git rev-parse --short HEAD)\"" };
    const version = b.run(versionArgs);

    // test filter to run a specific set of tests
    const test_filter = b.option([]const []const u8, "test-filter", "Test filter");
    // enables checkAllAllocationFailures do real work,
    // otherwise it's noop since it makes the tests a way longer
    const allocationFailures = b.option(bool, "allocation-failures", "Enable exhaustive, slow allocation-failure sweeps") orelse false;

    // ------- dependencies -------

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
    const xev = b.dependency("xev", .{
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
    const cliModule = b.addModule("cli", .{
        .root_source_file = b.path("src/lib/cli/cli.zig"),
        .target = target,
        .optimize = optimize,
    });
    const loggerModule = b.createModule(.{
        .root_source_file = b.path("src/lib/logging/logging.zig"),
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

    // ------- executables -------

    // all the projects imports, main bin and tests
    const tracyImpl = if (tracyEnabled) "tracy_impl_enabled" else "tracy_impl_disabled";
    const imports = [_]std.Build.Module.Import{
        .{ .name = "cli", .module = cliModule },
        .{ .name = "zeit", .module = zeit.module("zeit") },
        .{ .name = "httpz", .module = httpz.module("httpz") },
        .{ .name = "snappy", .module = snappy.module("snappy") },
        .{ .name = "zint", .module = zint.module("zint") },
        .{ .name = "metrics", .module = metrics.module("metrics") },
        .{ .name = "logz", .module = logz.module("logz") },
        .{ .name = "logging", .module = loggerModule },
        .{ .name = "tracy", .module = tracy.module("tracy") },
        .{ .name = "tracy_impl", .module = tracy.module(tracyImpl) },
        .{ .name = "c", .module = cModule },
        .{ .name = "encoding", .module = encodeModule },
        .{ .name = "xev", .module = xev.module("xev") },
    };

    const exe = b.addExecutable(.{
        .name = "ochi",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/main.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &imports,
            // .sanitize_thread = optimize == .Debug,
        }),
    });

    options.addRootOptions(b, exe, version, release, pprofEnabled);
    b.installArtifact(exe);

    const scooby_exe = b.addExecutable(.{
        .name = "scooby",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/scooby.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &imports,
        }),
    });

    options.addRootOptions(b, scooby_exe, version, release, pprofEnabled);
    b.installArtifact(scooby_exe);

    const sack_module = b.createModule(.{
        .root_source_file = b.path("src/apps/sack/main.zig"),
        .target = target,
        .optimize = optimize,
        .imports = &.{
            .{ .name = "zeit", .module = zeit.module("zeit") },
            .{ .name = "cli", .module = cliModule },
        },
    });
    const sack_exe = b.addExecutable(.{ .name = "sack", .root_module = sack_module });
    b.installArtifact(sack_exe);
    const run_sack = b.addRunArtifact(sack_exe);
    if (b.args) |args| run_sack.addArgs(args);
    b.step("sack", "Run the external-server single-record smoke check").dependOn(&run_sack.step);
    const sack_tests = b.addTest(.{
        .root_module = sack_module,
        .filters = if (test_filter) |filter| filter else &.{},
    });

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
        .filters = if (test_filter) |filter| filter else &[_][]const u8{},
        .test_runner = .{ .path = b.path("src/build/test_runner.zig"), .mode = .server },
    });
    options.addTestOptions(b, unit_tests.root_module, allocationFailures);
    options.addRootOptions(b, unit_tests, version, release, pprofEnabled);

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
        .test_runner = .{ .path = b.path("src/build/test_runner.zig"), .mode = .server },
    });
    options.addTestOptions(b, encoding_tests.root_module, allocationFailures);

    const cli_tests = b.addTest(.{
        .root_module = cliModule,
        .filters = if (test_filter) |filter| filter else &.{},
    });

    // unit tests source tree for its discovery
    const suites = [_]testing.TestSuite{
        .{ .name = "app", .sourceDir = null, .tests = unit_tests },
        .{ .name = "encoding", .sourceDir = "src/lib/encoding/", .tests = encoding_tests },
        .{ .name = "cli", .sourceDir = "src/lib/cli/", .tests = cli_tests },
        .{ .name = "sack", .sourceDir = "src/apps/sack/", .tests = sack_tests },
    };
    const genTestEntryPointStep = testing.addAllTestsFile(b, &suites);
    unit_tests.step.dependOn(genTestEntryPointStep);

    const test_step = b.step("test", "Run all test suites (app, encoding, cli, sack)");
    const all_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("test_all.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &imports,
        }),
        .filters = if (test_filter) |filter| filter else &.{},
        .test_runner = .{ .path = b.path("src/build/test_runner.zig"), .mode = .server },
    });
    all_tests.root_module.addImport("cli", all_tests.root_module);
    all_tests.root_module.addImport("encoding", all_tests.root_module);
    options.addTestOptions(b, all_tests.root_module, allocationFailures);
    options.addRootOptions(b, all_tests, version, release, pprofEnabled);
    all_tests.step.dependOn(genTestEntryPointStep);
    const install_all_tests = b.addInstallArtifact(all_tests, .{});
    const btest_step = b.step("btest", "Build and install bin/test containing all suites without running it");
    btest_step.dependOn(&install_all_tests.step);
    for (suites) |suite| {
        const runner = b.addRunArtifact(suite.tests);
        const stepName = b.fmt("test-{s}", .{suite.name});
        b.step(stepName, b.fmt("Run only the {s} test suite", .{suite.name})).dependOn(&runner.step);
        test_step.dependOn(&runner.step);

        const install = b.addInstallArtifact(suite.tests, .{
            .dest_sub_path = b.fmt("{s}-test", .{suite.name}),
        });
        btest_step.dependOn(&install.step);
    }

    const run_cover = b.addSystemCommand(&.{
        "kcov",
        "--clean",
        "--skip-solibs",
        "--include-pattern=ochi/src",
        b.pathJoin(&.{ b.install_path, "cov" }),
    });
    run_cover.addArtifactArg(unit_tests);
    const cover_step = b.step("cover", "Generate test coverage report");
    cover_step.dependOn(&run_cover.step);

    // check command
    const check = b.step("check", "Check if compiles");
    check.dependOn(&exe.step);
    check.dependOn(&scooby_exe.step);
    check.dependOn(&sack_exe.step);
    check.dependOn(&unit_tests.step);
}
