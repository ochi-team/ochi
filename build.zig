// build - builds the ochi main server executable
// - release - flags the executable as the stable release
// - pprof - enable allocator pprof api, exposes allocations pprof endpoint
// zig build run - same as zig build, but runs the resulted executable
//
// zig build btest - builds the main app test binary into zig-out/bin/test
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
// TODO: btest and test commands are inconsistent,
// test runs all the tests,
// btest builds only main tests,
// historically btest was introduced only for editor needs in order to debug app tests,
// but ideally we have either 2 btest (btest and btest-{main,etc.})
// or we make btest building all the binary, which is not easy to solve and performance wise not the best,
// therefore it hurts a human driving an IDE feedback loop

const std = @import("std");

const TestRunner = std.Build.Step.Compile.TestRunner;

const coverage = @import("src/build/coverage.zig");
const executable = @import("src/build/executable.zig");
const install = @import("src/build/install.zig");
const options = @import("src/build/options.zig");
const run = @import("src/build/run.zig");
const testing = @import("src/build/testing.zig");

pub fn build(b: *std.Build) void {
    // ------- options -------

    // TODO: find what is baseline and how to setup a proper target including v3/v4,
    // depends on the deployment target
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    // explicit release flag since we use ReleaseSafe for profiling
    const release = b.option(bool, "release", "Release") orelse false;

    const versionArgs = &[_][]const u8{ "sh", "-c", "git describe --exact-match --tags HEAD 2>/dev/null || echo \"$(git rev-parse --abbrev-ref HEAD)-$(git rev-parse --short HEAD)\"" };
    const version = b.run(versionArgs);

    const pprofEnabled = b.option(
        bool,
        "pprof",
        "Enable pprof profile",
    ) orelse false;

    const rootOptions: executable.RootOptions = .{
        .version = version,
        .release = release,
        .pprof = pprofEnabled,
    };

    // Allow the user to enable or disable Tracy support with a build flag
    const tracyEnabled = b.option(
        bool,
        "tracy",
        "Build with Tracy support.",
    ) orelse false;

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

    const executables = executable.addExecutables(b, target, optimize, &.{
        .{
            .id = .ochi,
            .name = "ochi",
            .root = b.path("src/main.zig"),
            .imports = &imports,
            .options = rootOptions,
        },
        .{
            .id = .scooby,
            .name = "scooby",
            .root = b.path("src/scooby.zig"),
            .imports = &imports,
            .options = rootOptions,
        },
        .{
            .id = .sack,
            .name = "sack",
            .root = b.path("src/apps/sack/main.zig"),
            .imports = &.{
                .{ .name = "zeit", .module = zeit.module("zeit") },
                .{ .name = "cli", .module = cliModule },
            },
        },
    });
    const exe = executables.get(.ochi);
    const scooby_exe = executables.get(.scooby);
    const sack_exe = executables.get(.sack);

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

    run.addRuns(b, &.{
        .{
            .artifact = exe,
            .step = "run",
            .description = "Run the application",
            .forwardArgs = true,
        },
        .{
            .artifact = scooby_exe,
            .step = "scooby",
            .description = "Run the Scooby data debug tool",
            .forwardArgs = true,
        },
        .{
            .artifact = sack_exe,
            .step = "sack",
            .description = "Run the external-server single-record smoke check",
            .forwardArgs = true,
        },
    });

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
            .root_source_file = b.path("src/lib/encoding/all_tests.zig"),
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
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/lib/cli/all_tests.zig"),
            .target = target,
            .optimize = optimize,
        }),
        .filters = if (test_filter) |filter| filter else &.{},
        .test_runner = testRunner,
    });

    const logging_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/lib/logging/all_tests.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "logz", .module = logz.module("logz") },
            },
        }),
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
    for (suites) |suite| {
        if (suite.sourceDir) |sourceDir| {
            suite.tests.step.dependOn(testing.addTestsFile(b, sourceDir, &.{}));
        }
    }

    const test_step = b.step("test", "Run all standalone test suites");
    install.addInstalls(b, &.{
        .{ .artifact = exe, .destination = .default },
        .{
            .artifact = unit_tests,
            .options = .{ .dest_sub_path = "test" },
            .destination = .{ .step = .{
                .name = "btest",
                .description = "Build and install bin/test for the main app without running tests",
            } },
        },
    });

    coverage.addCoverage(b, .{
        .suites = &suites,
        .includePath = b.pathFromRoot("src"),
        .outputPath = b.pathFromRoot("cov"),
        .step = "kcov",
        .description = "Run all test suites and merge their coverage report",
    });
    for (suites) |suite| {
        options.addTestOptions(b, suite.tests.root_module, allocationFailures);
        const runner = b.addRunArtifact(suite.tests);
        test_step.dependOn(&runner.step);
        const stepName = b.fmt("test-{s}", .{suite.name});
        const suite_step = b.step(stepName, b.fmt("Run only the {s} test suite", .{suite.name}));
        suite_step.dependOn(&runner.step);
        if (std.mem.eql(u8, suite.name, "main")) {
            b.step("test-app", "Alias for test-main").dependOn(suite_step);
        }
    }

    // check command
    const check = b.step("check", "Check if compiles");
    check.dependOn(&exe.step);
    check.dependOn(&scooby_exe.step);
    check.dependOn(&sack_exe.step);
    for (suites) |suite| check.dependOn(&suite.tests.step);
}
