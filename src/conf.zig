const std = @import("std");
const builtin = @import("builtin");
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
    // time interval in microseconds to flush mem tables to disk
    flushIntervalUs: i64 = 5 * std.time.us_per_s,
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

pub const Sys = struct {
    maxMem: u64,
    maxCachePortion: f64 = 0.5,
    cacheSize: u64,
};

var conf: Conf = undefined;
pub fn getConf() Conf {
    return conf;
}

// server config
server: ServerConfig,

// app config, defines application level settings
app: AppConfig,

// system config, defines system resources
sys: Sys,

const Conf = @This();

fn makeSys() !Sys {
    switch (builtin.os.tag) {
        .macos => {
            var memsize: u64 = 0;
            var len: usize = @sizeOf(u64);
            try std.posix.sysctlbynameZ("hw.memsize", &memsize, &len, null, 0);

            var sys = Sys{
                .maxMem = memsize,
                .cacheSize = 0,
            };
            const maxMemF: f64 = @floatFromInt(sys.maxMem);
            sys.cacheSize = @intFromFloat(maxMemF * sys.maxCachePortion);
            return sys;
        },
        .linux => {
            var info: std.os.linux.Sysinfo = undefined;
            const result: usize = std.os.linux.sysinfo(&info);
            if (std.os.linux.E.init(result) != .SUCCESS) {
                return error.UnknownTotalSystemMemory;
            }

            const maxMem: u64 = @as(u64, info.totalram) * info.mem_unit;
            var sys = Sys{
                .maxMem = maxMem,
                .cacheSize = 0,
            };
            const maxMemF: f64 = @floatFromInt(sys.maxMem);
            sys.cacheSize = @intFromFloat(maxMemF * sys.maxCachePortion);
            return sys;
        },
        else => return error.UnsupportedOS,
    }
}

// TODO: thiis is currently the only way to setup the cofnig,
// ideal solution would be:
// 1. have a global config instannce
// 2. easy override per test, so another runnig parallel test doesn't impact it
// probably the config gonna be define per package here and the package intry point accepts it,
// which further distributes its values to the dependencies
pub fn default() !Conf {
    const pools = calculatePools();
    const sys = try makeSys();
    conf = Conf{
        .server = .{
            .port = 9012,
            .pools = pools,
        },
        .app = .{},
        .sys = sys,
    };

    std.debug.assert(conf.app.flushIntervalUs >= std.time.us_per_s);
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
        .sys = result.sys,
    };
    return conf;
}
