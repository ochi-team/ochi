const std = @import("std");

pub const dataFlushIntervalNs = std.time.ns_per_s;

// time interval in microseconds to flush mem tables to disk
pub const indexFlushIntervalUs = 5 * std.time.us_per_s;
