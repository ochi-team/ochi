const std = @import("std");

const Encoder = @import("encoding").Encoder;
const Logger = @import("logging");

pub const bucketsSize = 1024;
pub const Bucket = struct {
    value: u64,
    overflows: std.ArrayList(u64),
};

pub const HashTokenizer = struct {
    buckets: []Bucket,
    bitset: std.bit_set.DynamicBitSetUnmanaged,

    pub fn init(allocator: std.mem.Allocator, buckets: []Bucket) !HashTokenizer {
        @memset(buckets, .{ .value = 0, .overflows = .empty });
        return .{
            .buckets = buckets,
            .bitset = try std.bit_set.DynamicBitSetUnmanaged.initEmpty(allocator, buckets.len),
        };
    }

    pub fn deinit(self: *HashTokenizer, allocator: std.mem.Allocator) void {
        for (0..self.buckets.len) |i| {
            self.buckets[i].overflows.deinit(allocator);
        }
        self.bitset.deinit(allocator);
    }

    pub fn reset(self: *HashTokenizer) void {
        for (0..self.buckets.len) |i| {
            self.buckets[i].overflows.clearRetainingCapacity();
            self.buckets[i].value = 0;
        }
        self.bitset.unsetAll();
    }

    pub fn tokenizeValues(
        self: *HashTokenizer,
        allocator: std.mem.Allocator,
        values: []const []const u8,
    ) !std.ArrayList(u64) {
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
        self: *HashTokenizer,
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
        var buckets: [bucketsSize]Bucket = undefined;
        var tokenizer = try HashTokenizer.init(allocator, &buckets);
        defer tokenizer.deinit(allocator);

        var tokens = try tokenizer.tokenizeValues(allocator, c.input);
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

pub const BloomFilter = struct {
    bits: []u64,

    const bitsPerEntry = 16;
    const hashRounds = 6;

    pub inline fn boundHashes(hashes: []const u64) usize {
        // +63 to have a gap rounding to upper value
        const len = (hashes.len * bitsPerEntry + 63) / 64;
        return @sizeOf(u64) * len;
    }

    pub fn deinit(self: *BloomFilter, allocator: std.mem.Allocator) void {
        allocator.free(self.bits);
    }

    pub fn writeBits(dst: []u8, src: []const u64) void {
        @memset(dst, 0);
        var buf: [8]u8 align(@alignOf(u64)) = undefined;
        const p: *u64 = @ptrCast(&buf);

        const maxBits = dst.len << 3; // * 8
        for (src) |srcHash| {
            p.* = srcHash;

            inline for (0..hashRounds) |_| {
                const hash = std.hash.XxHash64.hash(0, &buf);
                p.* += 1;

                const idx = hash % maxBits;
                const iHash = idx / 64;
                const bitOrder: u6 = @intCast(idx % 64);
                setEncodedBit(dst, iHash, bitOrder);
            }
        }
    }

    fn setEncodedBit(dst: []u8, wordIndex: usize, bitOrder: u6) void {
        const byteIndex = wordIndex * @sizeOf(u64) + (7 - bitOrder / 8);
        const byteBit: u3 = @intCast(bitOrder % 8);
        dst[byteIndex] |= @as(u8, 1) << byteBit;
    }
};

const Decoder = @import("encoding").Decoder;

// TODO: this test is mediocre, we must test a hit rate here >90% to confirm the hash is viable,
// testing readiability for the data is not very useful
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
        var buckets: [bucketsSize]Bucket = undefined;
        var tokenizer = try HashTokenizer.init(allocator, &buckets);
        defer tokenizer.deinit(allocator);
        var hashes = try tokenizer.tokenizeValues(allocator, case.tokens);
        defer hashes.deinit(allocator);

        const buf = try allocator.alloc(u8, BloomFilter.boundHashes(hashes.items));
        defer allocator.free(buf);
        BloomFilter.writeBits(buf, hashes.items);

        if (case.expectedEncoded) |expected| {
            try std.testing.expectEqualStrings(expected, buf);
        }
    }
}
