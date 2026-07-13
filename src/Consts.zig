const std = @import("std");

// time interval in microseconds to flush data shards and mem tables
pub const dataFlushIntervalUs = std.time.us_per_s;

// time interval in microseconds to flush mem tables to disk
pub const indexFlushIntervalUs = 5 * std.time.us_per_s;

// validate flush interval can't be larger than 10s in case it becomes configurable
comptime {
    std.debug.assert(dataFlushIntervalUs <= 10 * std.time.us_per_s);
    std.debug.assert(indexFlushIntervalUs <= 10 * std.time.us_per_s);
}

pub const maxBlockSize = 2 * 1024 * 1024;

// threshold as 90% of a max block size
pub const flushSizeThreshold = 36 * (maxBlockSize / 40);

// we need to balance throughput and memory limits
// this number is just a guess
pub const amountOfTablesToMerge = 16;
