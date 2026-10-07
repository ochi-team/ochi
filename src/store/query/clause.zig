const std = @import("std");

const Query = @import("../../query/Query.zig");
const FilterExpression = Query.FilterExpression;

const Logger = @import("logging");

pub fn collectOrs(dst: *std.ArrayList(*const FilterExpression), expr: [2]*const FilterExpression) void {
    dst.appendSliceAssumeCapacity(expr[0..]);

    var i: usize = 0;
    while (i < dst.items.len) {
        const n = dst.items[i];

        i += 1;
        switch (n.*) {
            .orOp => |e| {
                dst.appendSliceBounded(e[0..]) catch {
                    Logger.log(.err, "conjunction expression buffer is full, consider to extend it", .{});
                    continue;
                };
            },
            else => continue,
        }
    }
}
