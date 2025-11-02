const std = @import("std");

const Field = @import("process.zig").Field;
const Lines = @import("process.zig").Lines;
const Line = @import("process.zig").Line;
const SID = @import("process.zig").SID;

fn sortLines(_: void, one: *const Line, another: *const Line) bool {
    // TODO: sort by stream, tenant id, timestamp
    return one.timestampNs < another.timestampNs;
}

fn sortFields(_: void, one: Field, another: Field) bool {
    return std.mem.lessThan(u8, one.key, another.key);
}

pub const BlockWriter = struct {
    pub fn init(allocator: std.mem.Allocator) !*BlockWriter {
        const w = try allocator.create(BlockWriter);
        w.* = BlockWriter{};
        return w;
    }
};

pub const MemPart = struct {
    pub fn init(allocator: std.mem.Allocator) !*MemPart {
        const p = try allocator.create(MemPart);
        p.* = MemPart{};
        return p;
    }
};

pub const DataShard = struct {
    lines: Lines,

    fn mustFlush(_: *DataShard) bool {
        return true;
    }

    fn flush(self: *DataShard, allocator: std.mem.Allocator) !void {
        // TODO: take it from a pool to reuse mem
        const memPart = try MemPart.init(allocator);
        std.mem.sortUnstable(*const Line, self.lines.items, {}, sortLines);
        for (self.lines.items) |line| {
            std.mem.sortUnstable(Field, @constCast(line.fields), {}, sortFields);
        }

        var blockSize: u32 = 0;
        var prevSID: ?SID = null;
        for (self.lines.items) |line| {
            if (prevSID == null) {
                prevSID = line.sid;
                blockSize += line.fieldsLen();
            }
        }
        _ = memPart;
    }
};

pub const Data = struct {
    shards: []DataShard,
    mutex: std.Thread.Mutex,

    pub fn init(allocator: std.mem.Allocator) !*Data {
        const i = try allocator.create(Data);
        // TODO: log warning if can't get cpus, no clue why getCpuCount may fail, perhaps due to a weird CPU architecture
        const cpus = std.Thread.getCpuCount() catch 4;
        const shards = try allocator.alloc(DataShard, cpus);
        i.* = Data{
            // TODO: parts
            // TODO: small parts
            .shards = shards,
            .mutex = .{},
        };
        return i;
    }

    pub fn deinit(self: *Data, allocator: std.mem.Allocator) void {
        allocator.free(self.shards);
        allocator.destroy(self);
    }

    pub fn addLines(self: *Data, allocator: std.mem.Allocator, lines: Lines) !void {
        // TODO: remove this garbage,
        // add an atomic counter and scroll shards like ring buffer, every shard has its own mutex to data
        self.mutex.lock();
        defer self.mutex.unlock();
        var shard = &self.shards[0];
        shard.lines = lines;

        if (shard.mustFlush()) {
            try shard.flush(allocator);
        } else {
            // TODO: start a timer to flush a shard every sec
            // reset the timer on flush
        }
    }
};
