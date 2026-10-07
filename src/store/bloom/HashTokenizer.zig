const std = @import("std");

const tracy = @import("tracy");

const bloom = @import("bloom.zig");
const Bucket = bloom.Bucket;
const isASCII = bloom.isASCII;
const isChar = bloom.isChar;
const Logger = @import("logging");
const TokenIterator = @import("TokenIterator.zig");

pub const Tokenizer = @This();
buckets: []Bucket,
bitset: std.bit_set.DynamicBitSetUnmanaged,

pub fn init(allocator: std.mem.Allocator, buckets: []Bucket) !Tokenizer {
    @memset(buckets, .{ .value = 0, .overflows = .empty });
    return .{
        .buckets = buckets,
        .bitset = try std.bit_set.DynamicBitSetUnmanaged.initEmpty(allocator, buckets.len),
    };
}

pub fn deinit(self: *Tokenizer, allocator: std.mem.Allocator) void {
    for (0..self.buckets.len) |i| {
        self.buckets[i].overflows.deinit(allocator);
    }
    self.bitset.deinit(allocator);
}

pub fn reset(self: *Tokenizer) void {
    for (0..self.buckets.len) |i| {
        self.buckets[i].overflows.clearRetainingCapacity();
        self.buckets[i].value = 0;
    }
    self.bitset.unsetAll();
}

pub fn tokenizeValues(
    self: *Tokenizer,
    allocator: std.mem.Allocator,
    values: []const []const u8,
) !std.ArrayList(u64) {
    const z = tracy.Zone.begin(.{
        .src = @src(),
        .name = "tokenizeValues",
    });
    defer z.end();

    var dst: std.ArrayList(u64) = try .initCapacity(allocator, 128);
    errdefer dst.deinit(allocator);
    for (values, 0..) |val, i| {
        if (i > 0 and std.mem.eql(u8, val, values[i - 1])) {
            continue;
        }

        try self.appendToken(allocator, &dst, val);
    }

    Logger.log(.debug, "token values dst", .{ .len = dst.items.len });
    return dst;
}

fn appendToken(
    self: *Tokenizer,
    allocator: std.mem.Allocator,
    dst: *std.ArrayList(u64),
    value: []const u8,
) !void {
    if (isASCII(value)) {
        try self.appendAsciiToken(allocator, dst, value);
    }
    // TODO: support unicode tokens
    // https://github.com/jacobsandlund/uucode
    // try self.appendUnicodeToken(allocator, dst, value);
    return;
}

fn appendAsciiToken(
    self: *Tokenizer,
    allocator: std.mem.Allocator,
    dst: *std.ArrayList(u64),
    value: []const u8,
) !void {
    var it: TokenIterator = .{ .value = value };
    while (it.next()) |token| {
        const maybeHash = try self.addToken(allocator, token);
        if (maybeHash) |hash| {
            try dst.append(allocator, hash);
        }
    }
}

fn appendUnicodeToken(
    self: *Tokenizer,
    allocator: std.mem.Allocator,
    dst: *std.ArrayList(u64),
    value: []const u8,
) !void {
    var str = value;
    while (str.len > 0) {
        var offset = str.len;
        var strView = std.unicode.Utf8View.init(str) catch {
            str = str[1..];
            continue;
        };
        var strIter = strView.iterator();
        while (strIter.next()) |s| {
            if (isTokenSymbol(s)) {
                offset = strIter.i;
                break;
            }
        }

        str = str[offset..];
        offset = str.len;

        if (std.unicode.Utf8View.init(str)) |view| {
            strIter = view.iterator();
            for (strIter.next()) |s| {
                if (!isTokenSymbol(s)) {
                    offset = strIter.i;
                    break;
                }
            }
        }

        if (offset == 0) {
            break;
        }

        const token = str[0..offset];
        str = str[offset..];
        const maybeHash = self.addToken(token);
        if (maybeHash) |hash| {
            try dst.append(allocator, hash);
        }
    }
}

fn addToken(self: *Tokenizer, allocator: std.mem.Allocator, token: []const u8) !?u64 {
    const h = std.hash.XxHash64.hash(0, token);
    const idx = h % @as(u64, self.buckets.len);

    var bucket = &self.buckets[idx];
    if (!self.bitset.isSet(idx)) {
        bucket.value = h;
        self.bitset.set(idx);
        return h;
    }

    if (bucket.value == h) {
        return null;
    }

    for (bucket.overflows.items) |v| {
        if (v == h) {
            return null;
        }
    }
    try bucket.overflows.append(allocator, h);
    return h;
}

fn isTokenSymbol(c: u8) bool {
    if (c < 0x80) {
        return isChar(c);
    }

    return isUnicodeLetter(c) or isUnicodeNumber(c) or c == '_';
}

fn isUnicodeLetter(c: u8) bool {
    return c != 0;
}

fn isUnicodeNumber(c: u8) bool {
    return c == 0;
}
