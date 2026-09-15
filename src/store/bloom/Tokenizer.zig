const std = @import("std");
const Allocator = std.mem.Allocator;

const TokenIterator = @import("TokenIterator.zig");
const bloom = @import("bloom.zig");

const Logger = @import("logging");

pub fn tokenize(alloc: Allocator, dst: *std.ArrayList([]const u8), val: []const u8) !void {
    if (!bloom.isASCII(val)) {
        // TODO: support unicode here
        return;
    }

    var set = std.StringHashMap(void).init(alloc);
    defer set.deinit();

    var it: TokenIterator = .{ .value = val };
    while (it.next()) |token| {
        const gop = try set.getOrPut(token);
        if (!gop.found_existing) {
            dst.appendBounded(alloc, token) catch |err| {
                switch (err) {
                    error.OutOfMemory => {
                        Logger.log(.warn, "tokenize dst out of bound, consider increase the buffer", .{});
                        return;
                    },
                }
            };
        }
    }
}
