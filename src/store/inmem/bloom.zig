const std = @import("std");
const Encoder = @import("encoding").Encoder;

pub const HashTokenizer = struct {
    const Bucket = struct {
        value: usize,
        overflows: std.ArrayList(usize),
    };

    buckets: [1024]Bucket,
    bitset: std.bit_set.DynamicBitSet,

    pub fn init(allocator: std.mem.Allocator) !*HashTokenizer {
        const s = try allocator.create(HashTokenizer);
        var buckets: [1024]Bucket = undefined;
        for (0..buckets.len) |i| {
            buckets[i] = Bucket{
                .value = 0,
                .overflows = std.ArrayList(usize).empty,
            };
        }
        s.* = HashTokenizer{
            .buckets = buckets,
            .bitset = try std.bit_set.DynamicBitSet.initEmpty(allocator, buckets.len),
        };
        return s;
    }

    pub fn deinit(self: *HashTokenizer, allocator: std.mem.Allocator) void {
        for (0..self.buckets.len) |i| {
            self.buckets[i].overflows.deinit(allocator);
        }
        self.bitset.deinit();
        allocator.destroy(self);
    }

    pub fn tokenizeValues(
        self: *HashTokenizer,
        allocator: std.mem.Allocator,
        values: [][]const u8,
    ) !std.ArrayList(u64) {
        var dst: std.ArrayList(u64) = try std.ArrayList(u64).initCapacity(allocator, 2);
        errdefer dst.deinit(allocator);
        for (values, 0..) |val, i| {
            if (i > 0 and std.mem.eql(u8, val, values[i - 1])) {
                continue;
            }

            try self.appendToken(allocator, &dst, val);
        }

        return dst;
    }

    fn appendToken(
        self: *HashTokenizer,
        allocator: std.mem.Allocator,
        dst: *std.ArrayList(u64),
        value: []const u8,
    ) !void {
        if (isASCII(value)) {
            try self.appendAsciiToken(allocator, dst, value);
        }
        // TODO: support unicode tokens
        // try self.appendUnicodeToken(allocator, dst, value);
        return;
    }

    fn appendAsciiToken(
        self: *HashTokenizer,
        allocator: std.mem.Allocator,
        dst: *std.ArrayList(u64),
        value: []const u8,
    ) !void {
        var i: usize = 0;
        while (i < value.len) {
            var start = value.len;
            // find start
            while (i < value.len) {
                if (!isTokenChar(value[i])) {
                    i += 1;
                    continue;
                }
                start = i;
                i += 1;
                break;
            }
            // find end
            var end = value.len;
            while (i < value.len) {
                if (isTokenChar(value[i])) {
                    i += 1;
                    continue;
                }
                end = i;
                i += 1;
                break;
            }

            if (end <= start) {
                break;
            }
            const token = value[start..end];
            const maybeHash = try self.addToken(allocator, token);
            if (maybeHash) |hash| {
                try dst.append(allocator, hash);
            }
        }
    }

    fn appendUnicodeToken(
        self: *HashTokenizer,
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

    fn addToken(self: *HashTokenizer, allocator: std.mem.Allocator, token: []const u8) !?u64 {
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
};

test "tokenizeValues" {
    const Case = struct {
        input: []const []const u8,
        expected: []const u64,
    };
    const cases = [_]Case{
        .{ .input = &[_][]const u8{}, .expected = &[_]u64{} },
        .{ .input = &[_][]const u8{""}, .expected = &[_]u64{} },
        .{ .input = &[_][]const u8{"foo"}, .expected = &[_]u64{0x33BF00A859C4BA3F} },
        .{ .input = &[_][]const u8{ "foo -- foo", "~~'(foo) ==^%" }, .expected = &[_]u64{0x33BF00A859C4BA3F} },
        .{
            .input = &[_][]const u8{"foo bar -- .##([baz]## %^&* Groovy"},
            .expected = &[_]u64{ 0x33BF00A859C4BA3F, 0x48A37C90AD27A659, 0x42598CF26A247404, 15498472218330607137 },
        },
        .{
            .input = &[_][]const u8{"foo bar -- .##([baz]## %^&* Groovy [[foo]] <<bar>> --- baz!!"},
            .expected = &[_]u64{ 0x33BF00A859C4BA3F, 0x48A37C90AD27A659, 0x42598CF26A247404, 15498472218330607137 },
        },
        // .{
        //     .input = &[_][]const u8{ "Юникод 999 var12.34", "34 var12 qwer" },
        //     .expected = &[_]u64{ 0xFE846FA145CEABD1, 0xD8316E61D84F6BA4, 0x6D67BA71C4E03D10, 0x5E8D522CA93563ED, 0xED80AED10E029FC8 },
        // },
    };

    for (cases) |c| {
        const allocator = std.testing.allocator;
        const tokenizer = try HashTokenizer.init(allocator);
        defer tokenizer.deinit(allocator);

        var tokens = try tokenizer.tokenizeValues(allocator, @constCast(c.input));
        defer tokens.deinit(allocator);

        try std.testing.expectEqualSlices(u64, c.expected, tokens.items);
    }
}

fn isASCII(s: []const u8) bool {
    for (s) |b| {
        if (b >= 0x80) {
            return false;
        }
    }
    return true;
}

inline fn isTokenChar(c: u8) bool {
    return tokenCharTable[c] != 0;
}

const tokenCharTable = blk: {
    var a: [256]u8 = undefined;
    for (0..256) |c| {
        if (c >= 'a' and c <= 'z' or c >= 'A' and c <= 'Z' or c >= '0' and c <= '9' or c == '_') {
            a[c] = 1;
        } else {
            a[c] = 0;
        }
    }
    break :blk a;
};

fn isTokenSymbol(c: u8) bool {
    if (c < 0x80) {
        return isTokenChar(c);
    }

    return isUnicodeLetter(c) or isUnicodeNumber(c) or c == '_';
}

fn isUnicodeLetter(c: u8) bool {
    return c != 0;
}

fn isUnicodeNumber(c: u8) bool {
    return c == 0;
}

pub fn encodeBloomHashes(allocator: std.mem.Allocator, hashes: []u64) ![]u8 {
    var bf = try BloomFilter.initHashes(allocator, hashes);
    defer bf.deinit(allocator);

    const dstSize = bf.bound();
    const dst = try allocator.alloc(u8, dstSize);
    bf.encode(dst);
    return dst;
}

pub const BloomFilter = struct {
    bits: []u64,

    const bitsPerEntry = 16;
    const hashRounds = 6;

    pub fn initHashes(allocator: std.mem.Allocator, hashes: []u64) !*BloomFilter {
        // +63 to have a gap rounding to upper value
        const len = (hashes.len * bitsPerEntry + 63) / 64;

        const bits = try allocator.alloc(u64, len);
        errdefer allocator.free(bits);
        @memset(bits, 0);

        const hashCount = hashes.len * hashRounds;
        const hashedHashes = try allocator.alloc(u64, hashCount);
        defer allocator.free(hashedHashes);
        putHashes(hashedHashes, hashes);

        setupBits(bits, hashedHashes);

        const s = try allocator.create(BloomFilter);
        s.* = BloomFilter{
            .bits = bits,
        };
        return s;
    }

    pub fn deinit(self: *BloomFilter, allocator: std.mem.Allocator) void {
        allocator.free(self.bits);
        allocator.destroy(self);
    }

    fn putHashes(dst: []u64, src: []u64) void {
        var buf: [8]u8 align(@alignOf(u64)) = undefined;
        const p: *u64 = @ptrCast(&buf);
        var i: usize = 0;

        for (src) |hash| {
            p.* = hash;

            inline for (0..hashRounds) |_| {
                const h = std.hash.XxHash64.hash(0, &buf);
                dst[i] = h;
                i += 1;
                p.* += 1;
            }
        }
    }

    fn setupBits(bits: []u64, hashes: []u64) void {
        const maxBits = bits.len * 64;
        for (hashes) |hash| {
            const idx = hash % maxBits;
            const i = idx / 64;
            const bitOrder: u6 = @intCast(idx % 64);
            const mask: u64 = @as(u64, 1) << bitOrder;
            const word = bits[i];
            if (word & mask == 0) {
                bits[i] = word | mask;
            }
        }
    }

    pub fn bound(self: *const BloomFilter) usize {
        return @sizeOf(u64) * self.bits.len;
    }

    pub fn encode(self: *const BloomFilter, dst: []u8) void {
        var enc = Encoder.init(dst);
        for (self.bits) |word| {
            enc.writeInt(u64, word);
        }
    }

    pub fn contains(self: *BloomFilter, hashes: []u64) bool {
        if (self.bits.len == 0) return true;

        const maxBits = self.bits.len * 64;
        for (hashes) |hash| {
            const idx = hash % maxBits;
            const i = idx / 64;
            const bitOrder: u6 = @intCast(idx % 64);
            const mask = @as(u64, 1) << bitOrder;
            const word = self.bits[i];
            if (word & mask == 0) {
                return false;
            }
        }
        return true;
    }
};

test "BloomFilter" {
    const allocator = std.testing.allocator;
    const Case = struct {
        tokens: []const []const u8,
        expectedEncoded: ?[]const u8,
    };

    const thousandTokens = try allocator.alloc([]u8, 1000);
    defer allocator.free(thousandTokens);
    for (0..1000) |i| {
        thousandTokens[i] = try std.fmt.allocPrint(allocator, "{d}", .{i + 1000});
    }
    defer {
        for (0..1000) |i| {
            allocator.free(thousandTokens[i]);
        }
    }
    const cases = [_]Case{
        .{
            .tokens = &[_][]const u8{"foo"},
            .expectedEncoded = "\x00\x00\x00\x82\x40\x18\x00\x04",
        },
        .{
            .tokens = &[_][]const u8{ "foo", "bar", "baz" },
            .expectedEncoded = "\x00\x00\x81\xA3\x48\x5C\x10\x26",
        },
        .{
            .tokens = &[_][]const u8{ "foo", "bar", "baz", "foo" },
            .expectedEncoded = "\x00\x00\x81\xA3\x48\x5C\x10\x26",
        },
        .{
            .tokens = thousandTokens,
            .expectedEncoded = null,
        },
    };

    for (cases) |case| {
        // init
        var tokenizer = try HashTokenizer.init(allocator);
        defer tokenizer.deinit(allocator);
        var hashes = try tokenizer.tokenizeValues(allocator, @constCast(case.tokens));
        defer hashes.deinit(allocator);

        const bf = try BloomFilter.initHashes(allocator, hashes.items);
        defer bf.deinit(allocator);

        // make expected hashes
        const hashedHashes = try allocator.alloc(u64, BloomFilter.hashRounds * hashes.items.len);
        defer allocator.free(hashedHashes);
        BloomFilter.putHashes(hashedHashes, hashes.items);

        // validate
        try std.testing.expect(bf.contains(hashedHashes));
        if (case.expectedEncoded) |expected| {
            const bufSize = bf.bound();
            const buf = try allocator.alloc(u8, bufSize);
            defer allocator.free(buf);
            bf.encode(buf);

            try std.testing.expectEqualStrings(expected, buf);
        }
    }
}
