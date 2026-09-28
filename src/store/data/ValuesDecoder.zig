const std = @import("std");
const Io = std.Io;
const Allocator = std.mem.Allocator;

const zeit = @import("zeit");

const ColumnType = @import("ColumnHeader.zig").ColumnType;

/// ValuesDecoder decodes values encoded by ValuesEncoder back to string representations.
const Self = @This();

// buf holds the currently decoded column's bytes.
// the previously processed columns are collected in the decodedBuffer
buf: std.ArrayList(u8) = .empty,
decodedBuffer: std.ArrayList([]u8) = .empty,
values: std.ArrayList([]const u8) = .empty,
dictStrings: ?[]const []const u8 = null,

/// resetArena must be called whenever the arena backing allocator is reset,
/// it doesn't use .clearRetainingCapacity in order not to retain dangling memory
pub fn resetArena(self: *Self) void {
    self.buf = .empty;
    self.decodedBuffer = .empty;
    self.values = .empty;
    self.dictStrings = null;
}

pub fn deinit(self: *Self, alloc: Allocator) void {
    if (self.dictStrings) |ds| {
        alloc.free(ds);
    }
    for (self.decodedBuffer.items) |b| alloc.free(b);
    self.decodedBuffer.deinit(alloc);
    self.buf.deinit(alloc);
    self.values.deinit(alloc);
}

fn refreshBuf(self: *Self, alloc: Allocator, capacity: usize) !void {
    if (self.buf.capacity > 0) {
        try self.decodedBuffer.append(alloc, self.buf.allocatedSlice());
        self.buf = .empty;
    }
    // TODO: although it's all on arena it compplicates the development,
    // this buffer must not be freshed,
    // but rather popped from decodedBuffer if we know the capacity in advance and passed further to the decoding API,
    // so that it allows to remove self.buf usage.
    // in general implementation must be reconsidered from scratch to use the allocations api less as possible
    // to cause less `try`
    self.buf = try std.ArrayList(u8).initCapacity(alloc, capacity);
}

pub fn decode(
    self: *Self,
    io: Io,
    alloc: Allocator,
    values: [][]const u8,
    vt: ColumnType,
    dictValues: []const []const u8,
) !void {
    switch (vt) {
        .string => {
            // values are already decoded
        },
        .dict => {
            if (self.dictStrings) |ds| {
                if (ds.len > 0) alloc.free(ds);
            }
            // TODO: maybe move instead of dupe? document why if we can’t
            self.dictStrings = try alloc.dupe([]const u8, dictValues);
            if (self.dictStrings) |ds| {
                for (values, 0..) |v, i| {
                    if (v.len < 1) {
                        return error.InvalidDictValue;
                    }
                    const id: usize = @intCast(v[0]);
                    if (id >= ds.len) {
                        return error.DictIdOutOfRange;
                    }
                    values[i] = ds[id];
                }
            }
        },
        .uint8 => {
            try self.refreshBuf(alloc, values.len * 3);
            for (values, 0..) |v, i| {
                if (v.len < 1) {
                    return error.InvalidValueLength;
                }
                const n = decodeInt(u8, v);
                const start = self.buf.items.len;
                self.decodeUint8String(n);
                values[i] = self.buf.items[start..];
            }
        },
        .uint16 => {
            try self.refreshBuf(alloc, values.len * 5);
            for (values, 0..) |v, i| {
                if (v.len < 2) {
                    return error.InvalidValueLength;
                }
                const n = decodeInt(u16, v);
                const start = self.buf.items.len;
                try self.decodeUint64String(alloc, n);
                values[i] = self.buf.items[start..];
            }
        },
        .uint32 => {
            try self.refreshBuf(alloc, values.len * 10);
            for (values, 0..) |v, i| {
                if (v.len < 4) {
                    return error.InvalidValueLength;
                }
                const n = decodeInt(u32, v);
                const start = self.buf.items.len;
                try self.decodeUint64String(alloc, n);
                values[i] = self.buf.items[start..];
            }
        },
        .uint64 => {
            try self.refreshBuf(alloc, values.len * 20);
            for (values, 0..) |v, i| {
                if (v.len < 8) {
                    return error.InvalidValueLength;
                }
                const n = decodeInt(u64, v);
                const start = self.buf.items.len;
                try self.decodeUint64String(alloc, n);
                values[i] = self.buf.items[start..];
            }
        },
        .int64 => {
            try self.refreshBuf(alloc, values.len * 20);
            for (values, 0..) |v, i| {
                if (v.len < 8) {
                    return error.InvalidValueLength;
                }
                const n = decodeInt(i64, v);
                const start = self.buf.items.len;
                try self.decodeInt64String(alloc, n);
                values[i] = self.buf.items[start..];
            }
        },
        .float64 => {
            try self.refreshBuf(alloc, values.len * 64);
            for (values, 0..) |v, i| {
                if (v.len < 8) {
                    return error.InvalidValueLength;
                }
                const f = decodeFloat64(v);
                const start = self.buf.items.len;
                try self.decodeFloat64String(alloc, f);
                values[i] = self.buf.items[start..];
            }
        },
        .ipv4 => {
            try self.refreshBuf(alloc, values.len * 15);
            for (values, 0..) |v, i| {
                if (v.len < 4) {
                    return error.InvalidValueLength;
                }
                const ip = decodeIPv4(v);
                const start = self.buf.items.len;
                self.decodeIPv4String(ip);
                values[i] = self.buf.items[start..];
            }
        },
        .timestampIso8601 => {
            try self.refreshBuf(alloc, values.len * 32);
            for (values, 0..) |v, i| {
                if (v.len < 8) {
                    return error.InvalidValueLength;
                }
                const timestamp = decodeTimestampISO8601(v);
                const start = self.buf.items.len;
                try self.decodeTimestampISO8601String(io, alloc, timestamp);
                values[i] = self.buf.items[start..];
            }
        },
        else => {
            return error.UnknownValueType;
        },
    }
}

pub fn decodeUint8String(self: *Self, n: u8) void {
    if (n < 10) {
        self.buf.appendAssumeCapacity('0' + n);
        return;
    }
    if (n < 100) {
        self.buf.appendAssumeCapacity('0' + n / 10);
        self.buf.appendAssumeCapacity('0' + n % 10);
        return;
    }

    if (n < 200) {
        self.buf.appendAssumeCapacity('1');
        const rem = n - 100;
        if (rem < 10) {
            self.buf.appendAssumeCapacity('0');
            self.buf.appendAssumeCapacity('0' + rem);
        } else {
            self.buf.appendAssumeCapacity('0' + rem / 10);
            self.buf.appendAssumeCapacity('0' + rem % 10);
        }
    } else {
        self.buf.appendAssumeCapacity('2');
        const rem = n - 200;
        if (rem < 10) {
            self.buf.appendAssumeCapacity('0');
            self.buf.appendAssumeCapacity('0' + rem);
        } else {
            self.buf.appendAssumeCapacity('0' + rem / 10);
            self.buf.appendAssumeCapacity('0' + rem % 10);
        }
    }
}

fn decodeUint64String(self: *Self, alloc: Allocator, n: u64) !void {
    var tmp: [20]u8 = undefined;
    const str = try std.fmt.bufPrint(&tmp, "{d}", .{n});
    try self.buf.appendSlice(alloc, str);
}

fn decodeInt64String(self: *Self, alloc: Allocator, n: i64) !void {
    var tmp: [21]u8 = undefined;
    const str = try std.fmt.bufPrint(&tmp, "{d}", .{n});
    try self.buf.appendSlice(alloc, str);
}

fn decodeFloat64String(self: *Self, alloc: Allocator, f: f64) !void {
    var tmp: [64]u8 = undefined;
    const str = try std.fmt.bufPrint(&tmp, "{d}", .{f});
    try self.buf.appendSlice(alloc, str);
}

pub fn decodeIPv4String(self: *Self, n: u32) void {
    self.decodeUint8String(@intCast((n >> 24) & 0xFF));
    self.buf.appendAssumeCapacity('.');
    self.decodeUint8String(@intCast((n >> 16) & 0xFF));
    self.buf.appendAssumeCapacity('.');
    self.decodeUint8String(@intCast((n >> 8) & 0xFF));
    self.buf.appendAssumeCapacity('.');
    self.decodeUint8String(@intCast(n & 0xFF));
}

fn decodeTimestampISO8601String(self: *Self, io: Io, alloc: Allocator, nsecs: i64) !void {
    const instant = try zeit.instant(io, .{ .source = .{ .unix_nano = nsecs } });
    const time = instant.time();

    const nsecsInSecond = @mod(nsecs, 1_000_000_000);

    // Cast year to unsigned to avoid '+' prefix in formatting
    const year: u16 = if (time.year >= 0) @intCast(time.year) else 0;

    var tmp: [32]u8 = undefined;
    const str = if (@mod(nsecsInSecond, 1_000_000) == 0)
        try std.fmt.bufPrint(&tmp, "{d:0>4}-{d:0>2}-{d:0>2}T{d:0>2}:{d:0>2}:{d:0>2}.{d:0>3}Z", .{
            year,
            time.month,
            time.day,
            time.hour,
            time.minute,
            time.second,
            @as(u16, @intCast(@divTrunc(nsecsInSecond, 1_000_000))),
        })
    else if (@mod(nsecsInSecond, 1_000) == 0)
        try std.fmt.bufPrint(&tmp, "{d:0>4}-{d:0>2}-{d:0>2}T{d:0>2}:{d:0>2}:{d:0>2}.{d:0>6}Z", .{
            year,
            time.month,
            time.day,
            time.hour,
            time.minute,
            time.second,
            @as(u32, @intCast(@divTrunc(nsecsInSecond, 1_000))),
        })
    else
        try std.fmt.bufPrint(&tmp, "{d:0>4}-{d:0>2}-{d:0>2}T{d:0>2}:{d:0>2}:{d:0>2}.{d:0>9}Z", .{
            year,
            time.month,
            time.day,
            time.hour,
            time.minute,
            time.second,
            @as(u32, @intCast(nsecsInSecond)),
        });
    try self.buf.appendSlice(alloc, str);
}

fn decodeInt(comptime T: type, v: []const u8) T {
    return std.mem.readInt(T, v[0..@sizeOf(T)], .big);
}

fn decodeFloat64(v: []const u8) f64 {
    const n = decodeInt(u64, v);
    return @bitCast(n);
}

fn decodeIPv4(v: []const u8) u32 {
    return decodeInt(u32, v);
}

fn decodeTimestampISO8601(v: []const u8) i64 {
    const n = decodeInt(u64, v);
    return @bitCast(n);
}

test {
    _ = @import("ValuesDecoder_test.zig");
}
