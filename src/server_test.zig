const std = @import("std");

const server = @import("server.zig");
const Conf = @import("conf.zig").Conf;

test "serverWithSIGTERM" {
    const allocator = std.testing.allocator;

    // Start the server in a separate thread
    const ServerThread = struct {
        fn run() void {
            server.startServer(allocator, allocator, Conf.default()) catch |err| {
                std.debug.print("Server error: {}\n", .{err});
            };
        }
    };

    const thread = try std.Thread.spawn(.{}, ServerThread.run, .{});

    // Give the server time to start
    std.Thread.sleep(100 * std.time.ns_per_ms);

    // Send SIGTERM to ourselves
    const posix = std.posix;
    const pid = std.c.getpid();
    try posix.kill(pid, posix.SIG.TERM);

    // Wait for the server thread to finish
    thread.join();
}
