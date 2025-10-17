const std = @import("std");
const Ymlz = @import("ymlz").Ymlz;

pub const AppConfig = struct {
    maxRequestSize: u32,
};

pub const ServerConfig = struct {
    port: u16,
};

pub const Conf = struct {
    server: ServerConfig,

    app: AppConfig,

    pub fn default() Conf {
        return Conf{
            .server = .{
                .port = 9012,
            },
            .app = .{
                .maxRequestSize = 1024 * 1024 * 4,
            },
        };
    }

    pub fn init(allocator: std.mem.Allocator, path: []const u8) !Conf {
        const yml_path = try std.fs.cwd().realpathAlloc(
            allocator,
            path,
        );
        defer allocator.free(yml_path);

        var ymlz = try Ymlz(Conf).init(allocator);
        const result = try ymlz.loadFile(yml_path);
        defer ymlz.deinit(result);

        return Conf{
            .server = result.server,
            .app = result.app,
        };
    }
};
