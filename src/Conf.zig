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
    maxRequestSize: u32 = 1024 * 1024 * 4,
    /// maxIndexMemBlockSize is a size of the mem block for index before start flushing the chunk,
    /// must be cache friendly, depending on used CPU model must be changed according its L1 cache size
    maxIndexMemBlockSize: u32 = 32 * 1024,
};

pub const ServerConfig = struct {
    port: u16 = 9800,
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

server: ServerConfig,

app: AppConfig,

const Conf = @This();

// TODO: thiis is currently the only way to setup the cofnig,
// ideal solution would be:
// 1. have a global config instannce
// 2. easy override per test, so another runnig parallel test doesn't impact it
// probably the config gonna be define per package here and the package intry point accepts it,
// which further distributes its values to the dependencies
pub fn default() Conf {
    const pools = calculatePools();
    conf = Conf{
        .server = .{
            .port = 9012,
            .pools = pools,
        },
        .app = .{},
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
