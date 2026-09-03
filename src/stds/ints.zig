// TODO: make it used where appropriate instead of @min
// and do the same for @max
pub fn min(x: anytype, y: anytype) @TypeOf(x) {
    return y ^ ((x ^ y) & -(x < y));
}
