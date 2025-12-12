const std = @import("std");
const tsRfc3339Nano = "2006-01-02T15:04:05.999999999Z07:00";
const tsLineJsonSurrounding = "{\"_time\":\"\"}\n";
const lineTsSize: u32 = tsRfc3339Nano.len + tsLineJsonSurrounding.len;
const lineSurroundSize: u32 = "\"\":\"\",".len;
const msgKey = "_msg";

const Block = @import("block.zig").Block;
const Line = @import("../lines.zig").Line;

// gives size in resulted json object
// TODO: test against real resulted log record
pub inline fn blockJsonSize(self: *const Block) u32 {
    if (self.timestamps.len == 0) {
        return 0;
    }

    var res: u32 = @intCast(lineTsSize * self.timestamps.len);

    for (self.getCelledColumns()) |col| {
        res += @intCast(keyValSize(col.key, col.values[0]) * self.timestamps.len);
    }

    for (self.getColumns()) |col| {
        for (col.values) |val| {
            // TODO: test the empty values are skipped in resulted block
            if (val.len == 0) {
                continue;
            }

            res += keyValSize(col.key, val);
        }
    }

    return res;
}

pub inline fn fieldsJsonSize(self: *const Line) u32 {
    var res: u32 = lineTsSize;
    for (self.fields) |f| {
        if (f.value.len == 0) continue;

        res += keyValSize(f.key, f.value);
    }

    return res;
}

inline fn keyValSize(key: []const u8, val: []const u8) u32 {
    const keySize = if (key.len == 0) msgKey.len else key.len;
    return @intCast(lineSurroundSize + keySize + val.len);
}

test "sizingBlockAndFieldsMatch" {
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
            .sid = undefined,
            .fields = &sameField1,
            .encodedTags = undefined,
        },
        .{
            .timestampNs = undefined,
            .sid = undefined,
            .fields = &sameField2,
            .encodedTags = undefined,
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
            .sid = undefined,
            .fields = &emptyField1,
            .encodedTags = undefined,
        },
        .{
            .timestampNs = undefined,
            .sid = undefined,
            .fields = &emptyField2,
            .encodedTags = undefined,
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
            .sid = undefined,
            .fields = &emptyKey1,
            .encodedTags = undefined,
        },
        .{
            .timestampNs = undefined,
            .sid = undefined,
            .fields = &emptyKey2,
            .encodedTags = undefined,
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
    };
    for (cases) |case| {
        const alloc = std.testing.allocator;

        var fieldsSize: u32 = 0;
        for (case.lines) |line| {
            fieldsSize += fieldsJsonSize(&line);
        }

        const blockLines = try alloc.alloc(*const Line, case.lines.len);
        defer alloc.free(blockLines);
        for (0..case.lines.len) |i| {
            blockLines[i] = &case.lines[i];
        }
        const block = try Block.init(alloc, blockLines);
        defer block.deinit(alloc);
        const blockSize = blockJsonSize(block);

        try std.testing.expectEqual(fieldsSize, blockSize);
    }
}
