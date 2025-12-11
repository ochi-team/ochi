const tsRfc3339Nano = "2006-01-02T15:04:05.999999999Z07:00";
const tsLineJsonSurrounding = "{\"_time\":\"\"}\n";
const lineTsSize = tsRfc3339Nano.len + tsLineJsonSurrounding.len;
const lineSurroundSize = "\"\":\"\",".len;
const msgKey = "_msg";

const Block = @import("block.zig").Block;
const Line = @import("../lines.zig").Line;

// gives size in resulted json object
// TODO: test against real resulted log record
pub inline fn blockSize(self: *Block) u32 {
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

pub inline fn fieldsSize(self: *const Line) u32 {
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
