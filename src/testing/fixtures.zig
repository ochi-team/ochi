const std = @import("std");
const Allocator = std.mem.Allocator;

const Line = @import("../store/lines.zig").Line;
const Field = @import("../store/lines.zig").Field;
const maxColumns = @import("../store/data/Block.zig").maxColumns;
const deinitLinesFull = @import("../store/lines.zig").deinitLinesFull;

pub fn makeUniqueFieldLines(alloc: Allocator, cap: usize, tenant: u32) !std.ArrayList(Line) {
    var lines: std.ArrayList(Line) = try .initCapacity(alloc, cap);
    errdefer deinitLinesFull(alloc, &lines);

    for (0..lines.capacity) |i| {
        const fields = try alloc.alloc(Field, 1);
        errdefer alloc.free(fields);

        const key = try std.fmt.allocPrint(alloc, "tenant_{d}_key_{d}", .{ tenant, i });
        errdefer alloc.free(key);

        const value = try alloc.dupe(u8, "value");
        errdefer alloc.free(value);

        fields[0] = .{
            .key = key,
            .value = value,
        };
        lines.appendAssumeCapacity(.{
            .timestampNs = @intCast(i + 1),
            .fields = fields,
        });
    }

    return lines;
}
