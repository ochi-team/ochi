const std = @import("std");
const bloom = @import("../bloom/bloom.zig");

/// by all means every token must match
pub fn matchPredicateByAll(value: []const u8, tokens: []const []const u8) bool {
    for (tokens) |token| {
        if (!matchPredicate(value, token)) return false;
    }

    return true;
}

fn matchPredicate(v: []const u8, token: []const u8) bool {
    if (token.len == 0) return v.len == 0;

    if (token.len > v.len) return false;

    return tokenPosition(v, token) != null;
}

/// returns the offset of the first token sequence occurrence in value
fn tokenPosition(v: []const u8, token: []const u8) ?usize {
    if (token.len == 0) return 0;
    if (token.len > v.len) return null;

    // TODO: support unicode. Now every non-ASCII byte is treated as a separator,
    const startsWithToken = bloom.isChar(token[0]);
    const endsWithToken = bloom.isChar(token[token.len - 1]);

    var pos: usize = 0;
    while (std.mem.indexOfPos(u8, v, pos, token)) |n| {
        pos = n;
        const end = pos + token.len;

        if (startsWithToken and pos > 0 and bloom.isChar(v[pos - 1])) {
            pos += 1;
            continue;
        }
        if (endsWithToken and end < v.len and bloom.isChar(v[end])) {
            pos += 1;
            continue;
        }

        return pos;
    }

    return null;
}
