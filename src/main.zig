const std = @import("std");

const zeit = @import("zeit");

const build = @import("build");
const Conf = @import("conf.zig");
const server = @import("server.zig");
const cli = @import("cli");

var cli_config = struct {
    config: []const u8 = "", // e.g. "seqor.yaml",
}{};

pub fn main() !void {
    var runner = try cli.AppRunner.init(std.heap.page_allocator);

    const app = cli.App{
        .author = "Seqor",
        .version = build.version,
        .command = cli.Command{
            .name = "run",
            .description = cli.Description{
                .one_line = "Start the Seqor server",
            },
            .options = try runner.allocOptions(&.{
                .{
                    .long_name = "config",
                    .short_alias = 'c',
                    .help = "file path to configuration file",
                    .value_ref = runner.mkRef(&cli_config.config),
                },
            }),
            .target = cli.CommandTarget{
                .action = cli.CommandAction{ .exec = runServer },
            },
        },
    };

    return runner.run(&app);
}

fn runServer() !void {
    std.debug.print("Seqor version {s}", .{build.version});

    const config = try Conf.init(std.heap.page_allocator, cli_config.config);
    const now = try zeit.instant(.{ .source = .now });
    var nowBuf: [32]u8 = undefined;
    const nowStr = try now.time().bufPrint(&nowBuf, .rfc3339);

    // TODO: introduce structured logger
    std.debug.print("Seqor in mono mode starting at port={d}, time={s}\n", .{ config.server.port, nowStr });

    try server.startServer(std.heap.page_allocator, std.heap.page_allocator, config);
}
