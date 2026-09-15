const bloom = @import("bloom.zig");
const isChar = bloom.isChar;

// TODO: worth looking at std.mem.splitScalar and std.mem.tokenizeScalar
// to see if we need this iterator
pub const TokenIterator = @This();
value: []const u8,
i: usize = 0,

pub fn next(self: *TokenIterator) ?[]const u8 {
    const value = self.value;

    while (self.i < value.len and !isChar(value[self.i])) {
        self.i += 1;
    }

    if (self.i >= value.len) {
        return null;
    }

    const start = self.i;

    while (self.i < value.len and isChar(value[self.i])) {
        self.i += 1;
    }

    return value[start..self.i];
}
