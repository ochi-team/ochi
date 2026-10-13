const std = @import("std");
const Allocator = std.mem.Allocator;

const Line = @import("../store/lines.zig").Line;
const Field = @import("../store/lines.zig").Field;
const deinitLinesFull = @import("../store/lines.zig").deinitLinesFull;
const copyFields = @import("../store/lines.zig").copyFields;

pub fn makeUniqueFieldLines(alloc: Allocator, cap: usize, tenant: u32) !std.ArrayList(Line) {
    var lines: std.ArrayList(Line) = try .initCapacity(alloc, cap);
    errdefer deinitLinesFull(alloc, &lines);

    for (0..lines.capacity) |i| {
        var keyBuf: [64]u8 = undefined;
        const key = try std.fmt.bufPrint(&keyBuf, "tenant_{d}_key_{d}", .{ tenant, i });
        const fields = try copyFields(alloc, &.{.{ .key = key, .value = "value" }});
        lines.appendAssumeCapacity(.{
            .timestampNs = @intCast(i + 1),
            .fields = fields,
        });
    }

    return lines;
}

pub const SampleLines = struct {
    fields1: [2]Field,
    fields2: [2]Field,
    fields3: [2]Field,
    lines: [3]Line,
};

pub fn populateSampleLines(sample: *SampleLines) void {
    sample.fields1 = .{
        .{ .key = "level", .value = "info" },
        .{ .key = "app", .value = "seq" },
    };
    sample.fields2 = .{
        .{ .key = "level", .value = "warn" },
        .{ .key = "app", .value = "seq" },
    };
    sample.fields3 = .{
        .{ .key = "level", .value = "warn" },
        .{ .key = "app", .value = "seq" },
    };
    sample.lines = .{
        .{
            .timestampNs = 1,
            .fields = sample.fields1[0..],
        },
        .{
            .timestampNs = 2,
            .fields = sample.fields2[0..],
        },
        .{
            .timestampNs = 3,
            .fields = sample.fields3[0..],
        },
    };
}

pub fn populateSampleLinesUnordered(sample: *SampleLines) void {
    sample.fields1 = .{
        .{ .key = "level", .value = "info" },
        .{ .key = "app", .value = "seq" },
    };
    sample.fields2 = .{
        .{ .key = "level", .value = "warn" },
        .{ .key = "app", .value = "seq" },
    };
    sample.fields3 = .{
        .{ .key = "level", .value = "warn" },
        .{ .key = "app", .value = "seq" },
    };
    sample.lines = .{
        .{
            .timestampNs = 2,
            .fields = sample.fields2[0..],
        },
        .{
            .timestampNs = 1,
            .fields = sample.fields1[0..],
        },
        .{
            .timestampNs = 3,
            .fields = sample.fields3[0..],
        },
    };
}
