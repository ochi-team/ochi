const std = @import("std");
const Ymlz = @import("ymlz").Ymlz;

fn calculatePools() PoolsConfig {
    // TODO: log warning if can't get cpus, no clue why getCpuCount may fail,
    // perhaps due to a weird CPU architecture
    const cpus = std.Thread.getCpuCount() catch 4;
    // 4 is a minimum amount of threads for workers
    const totalThreads: u16 = @intCast(@max(cpus, 8));
    // TODO: the numbers must be tuned further to balance between http and workers
    const workers = totalThreads / 2;
    // TODO: http threads are not used yet
    const https = totalThreads - workers;
    return .{
        .httpThreads = https,
        .workerThreads = workers,
        .cpus = @intCast(cpus),
    };
}

pub const AppConfig = struct {
    maxRequestSize: u32,
};

pub const ServerConfig = struct {
    port: u16,
    pools: PoolsConfig,
};

pub const PoolsConfig = struct {
    httpThreads: u16,
    workerThreads: u16,
    cpus: u16,
};

var conf: Conf = undefined;
pub fn getConf() Conf {
    return conf;
}

pub const Conf = struct {
    server: ServerConfig,

    app: AppConfig,

    pub fn default() Conf {
        const pools = calculatePools();
        conf = Conf{
            .server = .{
                .port = 9012,
                .pools = pools,
            },
            .app = .{
                .maxRequestSize = 1024 * 1024 * 4,
            },
        };
        return conf;
    }

    pub fn init(allocator: std.mem.Allocator, path: []const u8) !Conf {
        if (path.len == 0) return default();

        // TODO: this is broken,
        // we must override default configuration
        // otherwise yaml config must contain all the field
        const yml_path = try std.fs.cwd().realpathAlloc(
            allocator,
            path,
        );
        defer allocator.free(yml_path);

        var ymlz = try Ymlz(Conf).init(allocator);
        const result = try ymlz.loadFile(yml_path);
        defer ymlz.deinit(result);

        conf = Conf{
            .server = result.server,
            .app = result.app,
        };
        return conf;
    }
};
