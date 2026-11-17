const std = @import("std");

/// adds option to runtime build
pub fn addRootOptions(
    b: *std.Build,
    compile: *std.Build.Step.Compile,
    version: []const u8,
    release: bool,
    pprof: bool,
) void {
    // add build options to runtime
    const options = b.addOptions();
    compile.root_module.addOptions("build", options);

    // version, for obsevability
    options.addOption([]const u8, "version", version);
    // release flag, defines stricter check of the running envoronments,
    // necessary since ReleaseSafe is used in order to run tests on different non prod setups
    options.addOption(bool, "release", release);
    // enables pprof metrics
    options.addOption(bool, "pprof", pprof);
}

pub fn addTestOptions(
    b: *std.Build,
    module: *std.Build.Module,
    injectAllocationFailures: bool,
) void {
    const testOptions = b.addOptions();
    module.addOptions("test_options", testOptions);

    testOptions.addOption(bool, "allocationFailures", injectAllocationFailures);
}
