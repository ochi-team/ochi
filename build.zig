// build - builds the ochi main server executable
// - release - flags the executable as the stable release
// - pprof - enable allocator pprof api, exposes allocations pprof endpoint
// zig build run - same as zig build, but runs the resulted executable
//
// zig build btest - builds one binary containing all tests into zig-out/bin/test
// - test-filter - string filter to run specific tests
// - allocation-failures - enable exhaustive allocation failures
// zig build test - runs the combined test binary
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

const TestRunner = std.Build.Step.Compile.TestRunner;

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

    // ------- test runner -------
    const testRunner: TestRunner = .{ .path = b.path("src/build/test_runner.zig"), .mode = .server };

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
    const run_sack = b.addRunArtifact(sack_exe);
    if (b.args) |args| run_sack.addArgs(args);
    b.step("sack", "Run the external-server single-record smoke check").dependOn(&run_sack.step);
    const sack_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/apps/sack/all_tests.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "zeit", .module = zeit.module("zeit") },
                .{ .name = "cli", .module = cliModule },
            },
        }),
        .filters = if (test_filter) |filter| filter else &.{},
        .test_runner = testRunner,
    });
    sack_tests.step.dependOn(testing.addTestsFile(b, "src/apps/sack/", &.{}));

    // run command
    const run_exe = b.addRunArtifact(exe);
    if (b.args) |args| run_exe.addArgs(args);
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
        .test_runner = testRunner,
    });
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
        .test_runner = testRunner,
    });

    const cli_tests = b.addTest(.{
        .root_module = cliModule,
        .filters = if (test_filter) |filter| filter else &.{},
        .test_runner = testRunner,
    });

    const logging_tests = b.addTest(.{
        .root_module = loggerModule,
        .filters = test_filter orelse &.{},
        .test_runner = testRunner,
    });

    // unit tests source tree for its discovery
    const suites = [_]testing.TestSuite{
        .{ .name = "main", .sourceDir = null, .tests = unit_tests },
        .{ .name = "encoding", .sourceDir = "src/lib/encoding/", .tests = encoding_tests },
        .{ .name = "cli", .sourceDir = "src/lib/cli/", .tests = cli_tests },
        .{ .name = "sack", .sourceDir = "src/apps/sack/", .tests = sack_tests },
        .{ .name = "logging", .sourceDir = "src/lib/logging/", .tests = logging_tests },
    };
    const genTestEntryPointStep = testing.addTestsFile(b, "src/", &suites);
    unit_tests.step.dependOn(genTestEntryPointStep);

    const all_tests = b.addTest(.{
        .name = "test",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/test_all.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &imports,
        }),
        .filters = test_filter orelse &.{},
        .test_runner = testRunner,
    });
    for ([_][]const u8{ "cli", "encoding", "logging" }) |name| {
        all_tests.root_module.addImport(name, all_tests.root_module);
    }
    options.addRootOptions(b, all_tests, version, release, pprofEnabled);
    options.addTestOptions(b, all_tests.root_module, allocationFailures);
    all_tests.step.dependOn(genTestEntryPointStep);
    all_tests.step.dependOn(testing.addTestsFile(b, "src/apps/sack/", &.{}));

    const test_step = b.step("test", "Run the combined test binary containing all suites");
    const all_runner = b.addRunArtifact(all_tests);
    test_step.dependOn(&all_runner.step);
    const btest_step = b.step("btest", "Build and install bin/test containing all suites without running it");
    const install_tests = b.addInstallArtifact(all_tests, .{});
    btest_step.dependOn(&install_tests.step);
    const cover_step = b.step("kcov", "Run all test suites and merge their coverage report");
    b.step("cover", "Alias for kcov").dependOn(cover_step);
    const coverage_data = b.pathJoin(&.{ b.install_path, "cov-data" });
    const coverage_dir = b.addSystemCommand(&.{ "mkdir", "-p", coverage_data });
    coverage_dir.has_side_effects = true;
    const merge_coverage = b.addSystemCommand(&.{
        "kcov",
        "--merge",
        b.pathJoin(&.{ b.install_path, "cov" }),
    });
    cover_step.dependOn(&merge_coverage.step);
    for (suites) |suite| {
        options.addTestOptions(b, suite.tests.root_module, allocationFailures);
        const runner = b.addRunArtifact(suite.tests);
        const stepName = b.fmt("test-{s}", .{suite.name});
        const suite_step = b.step(stepName, b.fmt("Run only the {s} test suite", .{suite.name}));
        suite_step.dependOn(&runner.step);
        if (std.mem.eql(u8, suite.name, "main")) {
            b.step("test-app", "Alias for test-main").dependOn(suite_step);
        }
        const run_cover = b.addSystemCommand(&.{
            "kcov",
            "--clean",
            "--skip-solibs",
            b.fmt("--include-path={s}", .{b.pathFromRoot("src")}),
            b.pathJoin(&.{ coverage_data, suite.name }),
        });
        run_cover.addArtifactArg(suite.tests);
        run_cover.step.dependOn(&coverage_dir.step);
        merge_coverage.addArg(b.pathJoin(&.{ coverage_data, suite.name }));
        merge_coverage.step.dependOn(&run_cover.step);
    }

    // check command
    const check = b.step("check", "Check if compiles");
    check.dependOn(&exe.step);
    check.dependOn(&scooby_exe.step);
    check.dependOn(&sack_exe.step);
    for (suites) |suite| check.dependOn(&suite.tests.step);
    check.dependOn(&all_tests.step);
}
