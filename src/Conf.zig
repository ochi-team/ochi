const std = @import("std");
const Allocator = std.mem.Allocator;

const Ymlz = @import("ymlz").Ymlz;

pub const AppConfig = struct {
    maxRequestSize: u32 = 4 * 1024 * 1024,
    /// maxIndexMemBlockSize is a size of the mem block for index before start flushing the chunk,
    /// must be cache friendly, depending on used CPU model must be changed according its L1 cache size
    maxIndexMemBlockSize: u32 = 32 * 1024,
    // time interval in microseconds to flush mem tables to disk
    flushIntervalUs: i64 = 5 * std.time.us_per_s,
    // max portion of RAM to use for caching, between 0 and 1
    maxCachePortion: f64 = 0.5,
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
    std.debug.assert(conf.app.flushIntervalUs >= std.time.us_per_s);
    return conf;
}

// server config
server: ServerConfig = .{},

// app config, defines application level settings
app: AppConfig = .{},

// TODO: ideal solution would be:
// 1. have a global config instannce
// 2. easy override per test, so another runnig parallel test doesn't impact it
