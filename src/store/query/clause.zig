const std = @import("std");

const Query = @import("../../query/Query.zig");
const FilterExpression = Query.FilterExpression;

const Logger = @import("logging");

pub fn unwrapOrs(dst: *std.ArrayList(*const FilterExpression), expr: [2]*const FilterExpression) void {
    dst.appendSliceAssumeCapacity(expr[0..]);

    var i: usize = 0;
    while (i < dst.items.len) {
        switch (dst.items[i].*) {
            .orOp => |e| {
                // don't accumulate or expressions, override them,

                // but first insert to i+1 in order to validated the array has enough space
                dst.insertBounded(i + 1, e[1]) catch {
                    Logger.log(.err, "conjunction expression buffer is full, consider to extend it", .{});
                    i += 1;
                    continue;
                };
                dst.items[i] = e[0];
            },
            // otherwise move to the next item
            else => i += 1,
        }
    }
}
