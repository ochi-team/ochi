// timestamp in UTC
const tsRfc3339Nano = "2006-01-02T15:04:05.999999999Z";
const tsLineJsonSurrounding = "{\"" ++ timestampKey ++ "\":\"\"}\n";
const lineTsSize: u32 = tsRfc3339Nano.len + tsLineJsonSurrounding.len;
const lineSurroundSize: u32 = "\"\":\"\",".len;

const Block = @import("Block.zig");
const Line = @import("../lines.zig").Line;
const msgKey = @import("../lines.zig").msgKey;
const timestampKey = @import("../lines.zig").timestampKey;

// gives size in resulted json object
// TODO: test against real resulted log record
pub fn blockJsonSize(self: *const Block) u32 {
    if (self.timestamps.len == 0) {
        return 0;
    }

    var res: u32 = @intCast(lineTsSize * self.timestamps.len);

    for (self.getInvariantColumns()) |col| {
        if (col.values.len == 1) {
            res += @intCast(keyValSize(col.key, col.values[0]) * self.timestamps.len);
        } else {
            for (col.values) |val| {
                if (val.len == 0) {
                    continue;
                }
                res += keyValSize(col.key, val);
                break;
            }
        }
    }

    for (self.getColumns()) |col| {
        for (col.values) |val| {
            // TODO: make empty values are skipped in resulted block
            if (val.len == 0) {
                continue;
            }

            res += keyValSize(col.key, val);
        }
    }

    return res;
}

pub fn linesJsonSize(lines: []const Line) u32 {
    var res: u32 = 0;
    for (lines) |line| {
        res += fieldsJsonSize(line);
    }
    return res;
}

pub fn fieldsJsonSize(self: Line) u32 {
    var res: u32 = lineTsSize;
    for (self.fields) |f| {
        if (f.value.len == 0) continue;

        res += keyValSize(f.key, f.value);
    }

    return res;
}

fn keyValSize(key: []const u8, val: []const u8) u32 {
    const keySize = if (key.len == 0) msgKey.len else key.len;
    return @intCast(lineSurroundSize + keySize + val.len);
}
