const std = @import("std");
const Allocator = std.mem.Allocator;

const Runtime = @import("Runtime.zig");

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
    // TODO: make it 16 x cpus
    maxConnections: u32 = 8,
    maxRequestSize: u32 = 4 * 1024 * 1024,
    /// maxIndexMemBlockSize is a size of the mem block for index before start flushing the chunk,
    /// must be cache friendly, depending on used CPU model must be changed according its L1 cache size
    /// TODO: add a max clients connections
    maxIndexMemBlockSize: u32 = 32 * 1024,
    // max portion of RAM to use for caching, between 0 and 1
    maxCachePortion: f64 = 0.5,

    // TODO: make it supporting absolute path
    storePath: []const u8 = ".ochi",
    storeRetentionDays: u16 = 30,

    queryTimeoutMs: u64 = std.time.ms_per_s * 10,
    // TODO: confogure max cache size,
    // this pool can be preallocated and given away only for the caches:
    // - index queries
    // - index ingestions
    // - small tables page caches
    // then document the list of use cases for all the caches
    // in order to distributed it evenly

    pub fn storeRetentionNs(self: *const AppConfig) u64 {
        return std.time.ns_per_day * @as(u64, @intCast(self.storeRetentionDays));
    }

    // defiens max concurrent queries running simultaneously
    pub fn maxQueryConnectionsLimit(_: *const AppConfig, runtime: *const Runtime) u16 {
        const defaultMin = 4;
        const defaultMax = 16;
        return @min(defaultMax, @max(defaultMin, runtime.cpus));
    }
};

pub const ServerConfig = struct {
    port: u16 = 9014,
};

const Conf = @This();

var conf: Conf = .{};

pub fn getConf() Conf {
    return conf;
}

pub fn default() Conf {
    conf = .{};
    return conf;
}

// server config
server: ServerConfig = .{},

// app config, defines application level settings
app: AppConfig = .{},

const testing = std.testing;
test "maxQueryConnectionsLimit" {
    const c = default();
    const r1: Runtime = .{ .cpus = 1, .maxMem = undefined, .cacheSize = undefined, .diskSpace = undefined, .path = undefined };
    const r2: Runtime = .{ .cpus = 4, .maxMem = undefined, .cacheSize = undefined, .diskSpace = undefined, .path = undefined };
    const r3: Runtime = .{ .cpus = 8, .maxMem = undefined, .cacheSize = undefined, .diskSpace = undefined, .path = undefined };
    const r4: Runtime = .{ .cpus = 16, .maxMem = undefined, .cacheSize = undefined, .diskSpace = undefined, .path = undefined };
    const r5: Runtime = .{ .cpus = 32, .maxMem = undefined, .cacheSize = undefined, .diskSpace = undefined, .path = undefined };
    try testing.expectEqual(4, c.app.maxQueryConnectionsLimit(&r1));
    try testing.expectEqual(4, c.app.maxQueryConnectionsLimit(&r2));
    try testing.expectEqual(8, c.app.maxQueryConnectionsLimit(&r3));
    try testing.expectEqual(16, c.app.maxQueryConnectionsLimit(&r4));
    try testing.expectEqual(16, c.app.maxQueryConnectionsLimit(&r5));
}
