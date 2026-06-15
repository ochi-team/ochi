const std = @import("std");
const Allocator = std.mem.Allocator;

// TODO: investigate the sysmte's limits
// Potential aspects to limit:
// ingestion
// request size
// log lines per request
// rate limit
// log line fields
// log line size
// query
// max response size
// timeouts
// pagination cap
// max concurrent queries
// rate limit / throttling
// storage
// retention period
// max disk space per tenant
// concurrency
// thread pool size
// max background jobs (e.g. merges)
// configure the limits and apply them.

// 50 GB
// TODO: make it a part of the config
pub const maxTableSize: u64 = 50 << 30;

pub const AppConfig = struct {
    maxRequestSize: u32 = 4 * 1024 * 1024,
    /// maxIndexMemBlockSize is a size of the mem block for index before start flushing the chunk,
    /// must be cache friendly, depending on used CPU model must be changed according its L1 cache size
    /// TODO: add a max clients connections
    maxIndexMemBlockSize: u32 = 32 * 1024,
    // max portion of RAM to use for caching, between 0 and 1
    maxCachePortion: f64 = 0.5,

    // TODO: make it supporting absolute path
    storePath: []const u8 = ".ochi",
    storeRetention: u64 = 30 * std.time.ns_per_day,
    // TODO: confogure max cache size,
    // this pool can be preallocated and given away only for the caches:
    // - index queries
    // - index ingestions
    // - small tables page caches
    // then document the list of use cases for all the caches
    // in order to distributed it evenly
};

pub const ServerConfig = struct {
    port: u16 = 9014,
};

const Conf = @This();

var conf: Conf = .{};

pub fn getConf() Conf {
    return conf;
}

pub fn default(_: Allocator) Conf {
    conf = .{};
    return conf;
}

// server config
server: ServerConfig = .{},

// app config, defines application level settings
app: AppConfig = .{},

// TODO: ideal solution would be:
// 1. have a global config instannce
// 2. easy override per test, so another runnig parallel test doesn't impact it
