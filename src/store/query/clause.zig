const std = @import("std");

const Query = @import("../../query/Query.zig");
const FilterExpression = Query.FilterExpression;

const Logger = @import("logging");

pub fn unwrapOrs(dst: *std.ArrayList(*const FilterExpression), expr: [2]*const FilterExpression) bool {
    return unwrapConjuction(dst, expr, .orOp);
}

/// flattens nested ands into dst, ors are kept as is
pub fn unwrapAnds(dst: *std.ArrayList(*const FilterExpression), expr: [2]*const FilterExpression) bool {
    return unwrapConjuction(dst, expr, .andOp);
}

fn unwrapConjuction(dst: *std.ArrayList(*const FilterExpression), expr: [2]*const FilterExpression, comptime op: std.meta.Tag(FilterExpression)) bool {
    dst.appendSliceAssumeCapacity(expr[0..]);

    var res: bool = true;

    var i: usize = 0;
    while (i < dst.items.len) {
        if (dst.items[i].* != op) {
            i += 1;
            continue;
        }

        const e = @field(dst.items[i].*, @tagName(op));
        // insert the right operand first to validate the array has enough space
        dst.insertBounded(i + 1, e[1]) catch {
            Logger.log(.err, "conjunction expression buffer is full, consider to extend it", .{ .op = @tagName(op) });
            i += 1;
            res = false;
            continue;
        };
        dst.items[i] = e[0];
    }

    return res;
}
