const std = @import("std");

pub fn MemOrder(T: type) type {
    return struct {
        pub fn order(_: void, first: []T, second: []T) std.math.Order {
            return std.mem.order(T, first, second);
        }
        pub fn orderConst(_: void, first: []const T, second: []const T) std.math.Order {
            return std.mem.order(T, first, second);
        }

        pub fn lessThan(ctx: void, first: []T, second: []T) bool {
            return order(ctx, first, second) == .lt;
        }
        pub fn lessThanConst(ctx: void, first: []const T, second: []const T) bool {
            return orderConst(ctx, first, second) == .lt;
        }
    };
}
