const std = @import("std");

const zeit = @import("zeit");

const Block = @import("Block.zig");
const Line = @import("../lines.zig").Line;
const msgKey = @import("../lines.zig").msgKey;
const timestampKey = @import("../lines.zig").timestampKey;

const testing = std.testing;

test "sizingBlockAndFieldsMatch" {
    const io = testing.io;
    const Field = @import("../lines.zig").Field;

    var sameField1 = [_]Field{
        .{ .key = "level", .value = "info" },
        .{ .key = "app", .value = "seq" },
    };
    var sameField2 = [_]Field{
        .{ .key = "level", .value = "warn" },
        .{ .key = "app", .value = "seq" },
    };
    const linesOneSameField = [_]Line{
        .{
            .timestampNs = undefined,
            .fields = &sameField1,
        },
        .{
            .timestampNs = undefined,
            .fields = &sameField2,
        },
    };

    var emptyField1 = [_]Field{
        .{ .key = "level", .value = "info" },
        .{ .key = "app", .value = "" },
    };
    var emptyField2 = [_]Field{
        .{ .key = "level", .value = "" },
        .{ .key = "app", .value = "seq" },
    };
    const lineOneEmptyField = [_]Line{
        .{
            .timestampNs = undefined,
            .fields = &emptyField1,
        },
        .{
            .timestampNs = undefined,
            .fields = &emptyField2,
        },
    };

    var emptyKey1 = [_]Field{
        .{ .key = "", .value = "info" },
        .{ .key = "app", .value = "seq" },
    };
    var emptyKey2 = [_]Field{
        .{ .key = "level", .value = "info" },
        .{ .key = "", .value = "seq" },
    };
    const lineOneEmptyKey = [_]Line{
        .{
            .timestampNs = undefined,
            .fields = &emptyKey1,
        },
        .{
            .timestampNs = undefined,
            .fields = &emptyKey2,
        },
    };
    var diffField1 = [_]Field{
        .{ .key = "app", .value = "seq" },
    };
    var diffField2 = [_]Field{
        .{ .key = "level", .value = "info" },
    };
    const diffFieldsLine = [_]Line{
        .{
            .timestampNs = undefined,
            .fields = &diffField1,
        },
        .{
            .timestampNs = undefined,
            .fields = &diffField2,
        },
    };

    const Case = struct {
        lines: []const Line,
    };
    const cases = [_]Case{
        .{
            .lines = &linesOneSameField,
        },
        .{
            .lines = &lineOneEmptyField,
        },
        .{
            .lines = &lineOneEmptyKey,
        },
        .{
            .lines = &diffFieldsLine,
        },
    };
    for (cases) |case| {
        const alloc = testing.allocator;

        var fieldsSize: u32 = 0;
        for (case.lines) |line| {
            fieldsSize += line.fieldsSize();
        }

        var block = try Block.initFromLines(alloc, case.lines);
        defer block.deinit(alloc);
        const blockSize = block.size();

        try testing.expectEqual(fieldsSize, blockSize);

        // Verify size matches actual JSON serialization
        var totalJsonSize: u32 = 0;
        const timeInst = try zeit.instant(io, .{ .source = .now });
        var timeBuf: [36]u8 = undefined;
        const now = try timeInst.time().bufPrint(&timeBuf, .rfc3339Nano);
        for (case.lines) |line| {
            var obj: std.json.ObjectMap = .empty;
            defer obj.deinit(alloc);

            try obj.put(alloc, timestampKey, .{ .string = now });
            for (line.fields) |f| {
                if (f.value.len == 0) continue;
                const key = if (f.key.len == 0) msgKey else f.key;
                try obj.put(alloc, key, .{ .string = f.value });
            }

            const value = std.json.Value{ .object = obj };

            var writer = try std.Io.Writer.Allocating.initCapacity(alloc, 128);
            defer writer.deinit();
            try std.json.fmt(value, .{}).format(&writer.writer);

            // Add 1 for newline
            totalJsonSize += @intCast(writer.written().len + 1);
        }

        try testing.expectEqual(blockSize, totalJsonSize);
    }
}
