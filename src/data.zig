const std = @import("std");

const Line = @import("store/lines.zig").Line;

const MemTable = @import("store/inmem/TableMem.zig");

pub const DataShard = struct {
    lines: std.ArrayList(*const Line),

    fn mustFlush(_: *DataShard) bool {
        return true;
    }

    fn flush(self: *DataShard, allocator: std.mem.Allocator) !void {
        if (self.lines.items.len == 0) {
            return;
        }
        const memTable = try MemTable.init(allocator);
        try memTable.addLines(allocator, self.lines.items);
        // const p = memPart.open(allocator);
        // _ = p;
    }
};

pub const Data = struct {
    shards: []DataShard,
    mutex: std.Thread.Mutex,

    pub fn init(allocator: std.mem.Allocator) !*Data {
        const i = try allocator.create(Data);
        // TODO: log warning if can't get cpus, no clue why getCpuCount may fail,
        // perhaps due to a weird CPU architecture
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

    pub fn addLines(self: *Data, allocator: std.mem.Allocator, lines: std.ArrayList(*const Line)) !void {
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
