const std = @import("std");

const Field = @import("process.zig").Field;
const Lines = @import("process.zig").Lines;
const Line = @import("process.zig").Line;
const SID = @import("process.zig").SID;

fn columnLessThan(_: void, one: Column, another: Column) bool {
    return std.mem.lessThan(u8, one.key, another.key);
}

fn lineLessThan(_: void, one: *const Line, another: *const Line) bool {
    // sid is less
    return one.sid.lessThan(&another.sid) or
        // or sid is eq, but timestamp is less
        (one.sid.eql(&another.sid) and one.timestampNs < another.timestampNs);
}

fn fieldLessThan(_: void, one: Field, another: Field) bool {
    return std.mem.lessThan(u8, one.key, another.key);
}

pub const BlockHeader = struct {
    pub fn encode(self: *const BlockHeader, buf: []u8) void {
        _ = self;
        _ = buf;
        // TODO: implement
        unreachable;
    }
};

pub const IndexBlockHeader = struct {
    pub fn init(allocator: std.mem.Allocator) !*IndexBlockHeader {
        const h = try allocator.create(IndexBlockHeader);
        h.* = IndexBlockHeader{};
        return h;
    }
    pub fn write(self: *const IndexBlockHeader, buf: []u8) void {
        _ = self;
        _ = buf;
        // TODO: implement
        unreachable;
    }
};

pub const StreamWriter = struct {
    pub fn init(allocator: std.mem.Allocator) !*StreamWriter {
        const w = try allocator.create(StreamWriter);
        w.* = StreamWriter{};
        return w;
    }

    pub fn deinit(self: *StreamWriter, allocator: std.mem.Allocator) void {
        allocator.destroy(self);
    }

    pub fn write(self: *StreamWriter, allocator: std.mem.Allocator, block: *Block, sid: SID) BlockHeader {
        _ = self;
        _ = allocator;
        _ = block;
        _ = sid;
        // TODO implement
        unreachable;
    }
};

pub const BlockWriter = struct {
    streamWriter: *StreamWriter,
    indexBlockHeader: *IndexBlockHeader,

    // state
    sidFirst: ?SID,

    indexBlock: []u8,

    pub fn init(allocator: std.mem.Allocator) !*BlockWriter {
        const w = try allocator.create(BlockWriter);
        const streamWriter = try StreamWriter.init(allocator);
        const indexBlockHeader = try IndexBlockHeader.init(allocator);
        const indexBlock = try allocator.alloc(u8, 20 * 1024);
        w.* = BlockWriter{
            .streamWriter = streamWriter,
            .indexBlockHeader = indexBlockHeader,
            .sidFirst = null,
            .indexBlock = indexBlock,
        };
        return w;
    }

    pub fn deinint(self: *BlockWriter, allocator: std.mem.Allocator) void {
        allocator.free(self.indexBlock);
        self.streamWriter.deinit(allocator);
        allocator.destroy(self);
    }

    pub fn writeLines(self: *BlockWriter, allocator: std.mem.Allocator, sid: SID, lines: []*const Line) !void {
        const block = try Block.init(allocator, lines);
        defer block.deinit(allocator);
        try self.writeBlock(allocator, block, sid);
    }

    fn writeBlock(self: *BlockWriter, allocator: std.mem.Allocator, block: *Block, sid: SID) !void {
        if (block.len() == 0) {
            return;
        }

        const hasState = self.sidFirst == null;
        if (hasState) {
            self.sidFirst = sid;
        }

        const blockHeader = self.streamWriter.write(allocator, block, sid);

        // TODO: update block header and timestamp header stats

        blockHeader.encode(self.indexBlock);

        // TODO: implement growing buffer of blockIndex and flush only if it reached ~128kb
        if (true) {
            self.flushIndexBlock();
            self.indexBlock.len = 0;
        }

        // TODO: implement
        unreachable;
    }

    fn flushIndexBlock(self: *BlockWriter) void {
        self.indexBlockHeader.write(self.indexBlock);
        // TODO: write meta index block
        self.sidFirst = null;
    }

    pub fn finish(_: *BlockWriter) void {
        // TODO: implement
        unreachable;
    }
};

const maxColumns = 1000;

pub const Block = struct {
    columns: []Column,
    determinedColumns: []Field,

    pub fn init(allocator: std.mem.Allocator, lines: []*const Line) !*Block {
        const b = try allocator.create(Block);
        b.* = Block{
            .columns = undefined,
            .determinedColumns = undefined,
        };

        try b.put(allocator, lines);
        b.sort();
        return b;
    }

    pub fn deinit(self: *Block, allocator: std.mem.Allocator) void {
        for (self.columns) |col| {
            allocator.free(col.values);
        }
        allocator.free(self.columns);
        allocator.free(self.determinedColumns);
        allocator.destroy(self);
    }

    pub fn len(self: *Block) usize {
        return self.columns.len + self.determinedColumns.len;
    }

    fn put(self: *Block, allocator: std.mem.Allocator, lines: []*const Line) !void {
        // TODO: implement fast path when lines have same fields

        var columnI = std.StringHashMap(usize).init(allocator);
        for (lines) |line| {
            // TODO: implement maxColumns limit

            for (line.fields) |field| {
                if (!columnI.contains(field.key)) {
                    try columnI.put(field.key, columnI.count());
                }
            }
        }

        const columns = try allocator.alloc(Column, columnI.count());
        const c = Column{ .key = "", .values = undefined };
        @memset(columns, c);

        var columnIter = columnI.iterator();
        while (columnIter.next()) |entry| {
            const key = entry.key_ptr.*;
            const idx = entry.value_ptr.*;

            var col = &columns[idx];
            col.key = key;
            col.values = try allocator.alloc([]const u8, lines.len);
            @memset(col.values, "");
        }

        for (lines, 0..) |line, i| {
            for (line.fields) |field| {
                const idx = columnI.get(field.key).?;
                const col = columns[idx];
                col.values[i] = field.value;
            }
        }

        // TODO: detect repeated columns having a single value across all the lines in the block,
        // if it exists - define as "determined" and remove from columns

        self.determinedColumns = try allocator.alloc(Field, 0);
        self.columns = columns;
    }

    fn sort(self: *Block) void {
        if (self.len() > maxColumns) @panic("columns and determinedColumns size exceeded maxColumns");

        std.mem.sortUnstable(Column, self.columns, {}, columnLessThan);
        std.mem.sortUnstable(Field, self.determinedColumns, {}, fieldLessThan);
    }
};

pub const Column = struct {
    key: []const u8,
    values: [][]const u8,
};

pub const MemPart = struct {
    pub fn init(allocator: std.mem.Allocator) !*MemPart {
        const p = try allocator.create(MemPart);
        p.* = MemPart{};

        return p;
    }

    pub fn addLines(_: *MemPart, allocator: std.mem.Allocator, lines: Lines) !void {
        std.mem.sortUnstable(*const Line, lines.items, {}, lineLessThan);

        const blockWriter = try BlockWriter.init(allocator);
        defer blockWriter.deinint(allocator);

        var streamI: u32 = 0;
        var blockSize: u32 = 0;
        var prevSID: SID = lines.items[0].sid;

        for (lines.items, 0..) |line, i| {
            std.mem.sortUnstable(Field, line.fields, {}, fieldLessThan);

            if (blockSize >= maxBlockSize or !line.sid.eql(&prevSID)) {
                try blockWriter.writeLines(allocator, prevSID, lines.items[streamI..i]);
                prevSID = line.sid;
                blockSize = 0;
                streamI = @intCast(i);
            }
            blockSize += line.fieldsLen();
        }
        if (streamI != lines.items.len) {
            try blockWriter.writeLines(allocator, prevSID, lines.items[streamI..]);
        }
        blockWriter.finish();
    }

    pub fn open(_: *MemPart, _: std.mem.Allocator) *Part {
        // TODO: implement
        unreachable;
    }
};

pub const Part = struct {
    pub fn init(allocator: std.mem.Allocator) !*Part {
        const p = try allocator.create(Part);
        p.* = Part{};
        return p;
    }
};

// 2mb block size, on merging it takes double amount up to 4mb
const maxBlockSize = 2 * 1024 * 1024;

pub const DataShard = struct {
    lines: Lines,

    fn mustFlush(_: *DataShard) bool {
        return true;
    }

    fn flush(self: *DataShard, allocator: std.mem.Allocator) !void {
        if (self.lines.items.len == 0) {
            return;
        }
        const memPart = try MemPart.init(allocator);
        try memPart.addLines(allocator, self.lines);
        const p = memPart.open(allocator);
        _ = p;
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
