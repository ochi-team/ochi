const std = @import("std");
const zeit = @import("zeit");

const ColumnType = @import("block_header.zig").ColumnType;

/// ValuesDecoder decodes values encoded by ValuesEncoder back to string representations.
pub const ValuesDecoder = struct {
    buf: std.ArrayList(u8),
    values: std.ArrayList([]const u8),
    dictStrings: ?[]const []const u8 = null,
    allocator: std.mem.Allocator,

    pub fn init(allocator: std.mem.Allocator) !*ValuesDecoder {
        const vd = try allocator.create(ValuesDecoder);
        vd.* = .{
            .buf = std.ArrayList(u8).empty,
            .values = std.ArrayList([]const u8).empty,
            .allocator = allocator,
        };
        return vd;
    }

    pub fn deinit(self: *ValuesDecoder) void {
        if (self.dictStrings) |ds| {
            self.allocator.free(ds);
        }
        self.buf.deinit(self.allocator);
        self.values.deinit(self.allocator);
        self.allocator.destroy(self);
    }

    pub fn decode(
        self: *ValuesDecoder,
        values: [][]u8,
        vt: ColumnType,
        dictValues: []const []const u8,
    ) !void {
        switch (vt) {
            .string => {
                // values are already decoded
            },
            .dict => {
                if (self.dictStrings) |ds| {
                    if (ds.len > 0) self.allocator.free(ds);
                }
                self.dictStrings = try self.allocator.dupe([]const u8, dictValues);
                if (self.dictStrings) |ds| {
                    for (values, 0..) |v, i| {
                        if (v.len < 1) {
                            return error.InvalidDictValue;
                        }
                        const id: usize = @intCast(v[0]);
                        if (id >= ds.len) {
                            return error.DictIdOutOfRange;
                        }
                        values[i] = @constCast(ds[id]);
                    }
                }
            },
            .uint8 => {
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
                for (values, 0..) |v, i| {
                    if (v.len < 2) {
                        return error.InvalidValueLength;
                    }
                    const n = decodeInt(u16, v);
                    const start = self.buf.items.len;
                    try self.decodeUint64String(n);
                    values[i] = self.buf.items[start..];
                }
            },
            .uint32 => {
                for (values, 0..) |v, i| {
                    if (v.len < 4) {
                        return error.InvalidValueLength;
                    }
                    const n = decodeInt(u32, v);
                    const start = self.buf.items.len;
                    try self.decodeUint64String(n);
                    values[i] = self.buf.items[start..];
                }
            },
            .uint64 => {
                for (values, 0..) |v, i| {
                    if (v.len < 8) {
                        return error.InvalidValueLength;
                    }
                    const n = decodeInt(u64, v);
                    const start = self.buf.items.len;
                    try self.decodeUint64String(n);
                    values[i] = self.buf.items[start..];
                }
            },
            .int64 => {
                for (values, 0..) |v, i| {
                    if (v.len < 8) {
                        return error.InvalidValueLength;
                    }
                    const n = decodeInt(i64, v);
                    const start = self.buf.items.len;
                    try self.decodeInt64String(n);
                    values[i] = self.buf.items[start..];
                }
            },
            .float64 => {
                for (values, 0..) |v, i| {
                    if (v.len < 8) {
                        return error.InvalidValueLength;
                    }
                    const f = decodeFloat64(v);
                    const start = self.buf.items.len;
                    try self.decodeFloat64String(f);
                    values[i] = self.buf.items[start..];
                }
            },
            .ipv4 => {
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
                for (values, 0..) |v, i| {
                    if (v.len < 8) {
                        return error.InvalidValueLength;
                    }
                    const timestamp = decodeTimestampISO8601(v);
                    const start = self.buf.items.len;
                    try self.decodeTimestampISO8601String(timestamp);
                    values[i] = self.buf.items[start..];
                }
            },
            else => {
                return error.UnknownValueType;
            },
        }
    }

    fn decodeUint8String(self: *ValuesDecoder, n: u8) void {
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

    fn decodeUint64String(self: *ValuesDecoder, n: u64) !void {
        var tmp: [20]u8 = undefined;
        const str = std.fmt.bufPrint(&tmp, "{d}", .{n}) catch unreachable;
        try self.buf.appendSlice(self.allocator, str);
    }

    fn decodeInt64String(self: *ValuesDecoder, n: i64) !void {
        var tmp: [21]u8 = undefined;
        const str = std.fmt.bufPrint(&tmp, "{d}", .{n}) catch unreachable;
        try self.buf.appendSlice(self.allocator, str);
    }

    fn decodeFloat64String(self: *ValuesDecoder, f: f64) !void {
        var tmp: [64]u8 = undefined;
        const str = std.fmt.bufPrint(&tmp, "{d}", .{f}) catch unreachable;
        try self.buf.appendSlice(self.allocator, str);
    }

    fn decodeIPv4String(self: *ValuesDecoder, n: u32) void {
        self.decodeUint8String(@intCast((n >> 24) & 0xFF));
        self.buf.appendAssumeCapacity('.');
        self.decodeUint8String(@intCast((n >> 16) & 0xFF));
        self.buf.appendAssumeCapacity('.');
        self.decodeUint8String(@intCast((n >> 8) & 0xFF));
        self.buf.appendAssumeCapacity('.');
        self.decodeUint8String(@intCast(n & 0xFF));
    }

    fn decodeTimestampISO8601String(self: *ValuesDecoder, nsecs: i64) !void {
        const instant = try zeit.instant(.{ .source = .{ .unix_nano = nsecs } });
        const time = instant.time();

        // Calculate milliseconds within the second from total nanoseconds
        const nsecs_in_second = @mod(nsecs, 1_000_000_000);
        const millis = @divTrunc(@abs(nsecs_in_second), 1_000_000);

        // Cast year to unsigned to avoid '+' prefix in formatting
        const year: u16 = if (time.year >= 0) @intCast(time.year) else 0;

        var tmp: [32]u8 = undefined;
        // TODO: support nanoseconds
        const str = std.fmt.bufPrint(&tmp, "{d:0>4}-{d:0>2}-{d:0>2}T{d:0>2}:{d:0>2}:{d:0>2}.{d:0>3}Z", .{
            year,
            time.month,
            time.day,
            time.hour,
            time.minute,
            time.second,
            millis,
        }) catch unreachable;
        try self.buf.appendSlice(self.allocator, str);
    }
};

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

test "ValuesDecoder.decodeUint8String" {
    const allocator = std.testing.allocator;
    const decoder = try ValuesDecoder.init(allocator);
    defer decoder.deinit();

    // Ensure capacity for writing
    try decoder.buf.ensureUnusedCapacity(allocator, 16);

    decoder.buf.clearRetainingCapacity();
    decoder.decodeUint8String(0);
    try std.testing.expectEqualStrings("0", decoder.buf.items);

    decoder.buf.clearRetainingCapacity();
    decoder.decodeUint8String(9);
    try std.testing.expectEqualStrings("9", decoder.buf.items);

    decoder.buf.clearRetainingCapacity();
    decoder.decodeUint8String(42);
    try std.testing.expectEqualStrings("42", decoder.buf.items);

    decoder.buf.clearRetainingCapacity();
    decoder.decodeUint8String(99);
    try std.testing.expectEqualStrings("99", decoder.buf.items);

    decoder.buf.clearRetainingCapacity();
    decoder.decodeUint8String(100);
    try std.testing.expectEqualStrings("100", decoder.buf.items);

    decoder.buf.clearRetainingCapacity();
    decoder.decodeUint8String(199);
    try std.testing.expectEqualStrings("199", decoder.buf.items);

    decoder.buf.clearRetainingCapacity();
    decoder.decodeUint8String(200);
    try std.testing.expectEqualStrings("200", decoder.buf.items);

    decoder.buf.clearRetainingCapacity();
    decoder.decodeUint8String(255);
    try std.testing.expectEqualStrings("255", decoder.buf.items);
}

test "ValuesDecoder.decodeIPv4String" {
    const allocator = std.testing.allocator;
    const decoder = try ValuesDecoder.init(allocator);
    defer decoder.deinit();

    try decoder.buf.ensureUnusedCapacity(allocator, 16);

    // 1.2.3.4 = (1 << 24) | (2 << 16) | (3 << 8) | 4
    const ip: u32 = (1 << 24) | (2 << 16) | (3 << 8) | 4;
    decoder.buf.clearRetainingCapacity();
    decoder.decodeIPv4String(ip);
    try std.testing.expectEqualStrings("1.2.3.4", decoder.buf.items);

    const ip2: u32 = (192 << 24) | (168 << 16) | (1 << 8) | 1;
    decoder.buf.clearRetainingCapacity();
    decoder.decodeIPv4String(ip2);
    try std.testing.expectEqualStrings("192.168.1.1", decoder.buf.items);
}
