const std = @import("std");

const Field = @import("../lines.zig").Field;
const Line = @import("../lines.zig").Line;
const lineLessThan = @import("../lines.zig").lineLessThan;
const fieldLessThan = @import("../lines.zig").fieldLessThan;
const SID = @import("../lines.zig").SID;

const StreamWriter = @import("stream_writer.zig").StreamWriter;
const BlockWriter = @import("block_writer.zig").BlockWriter;

// 2mb block size, on merging it takes double amount up to 4mb
const maxBlockSize = 2 * 1024 * 1024;

pub const Error = error{
    EmptyLines,
};

pub const MemTable = struct {

    // TODO: write a header

    streamWriter: *StreamWriter,

    // columnNames: *Buffer,
    // columnIdxs: *Buffer,
    // metaindex: *Buffer,
    // index: *Buffer,
    // columnsHeaderIndex: *Buffer,
    // columnsHeader: *Buffer,
    // timestamps: *Buffer,

    // TODO: write bloom filter

    pub fn init(allocator: std.mem.Allocator) !*MemTable {
        const p = try allocator.create(MemTable);
        errdefer allocator.destroy(p);
        const streamWriter = try StreamWriter.init(allocator);
        p.* = MemTable{
            .streamWriter = streamWriter,
        };

        return p;
    }
    pub fn deinit(self: *MemTable, allocator: std.mem.Allocator) void {
        self.streamWriter.deinit(allocator);
        allocator.destroy(self);
    }

    pub fn addLines(self: *MemTable, allocator: std.mem.Allocator, lines: []*const Line) !void {
        if (lines.len == 0) {
            return Error.EmptyLines;
        }

        var blockWriter = try BlockWriter.init(allocator);
        defer blockWriter.deinit(allocator);

        var streamI: u32 = 0;
        var blockSize: u32 = 0;
        var prevSID: SID = lines[0].sid;

        std.mem.sortUnstable(*const Line, lines, {}, lineLessThan);
        for (lines, 0..) |line, i| {
            std.mem.sortUnstable(Field, line.fields, {}, fieldLessThan);

            if (blockSize >= maxBlockSize or !line.sid.eql(&prevSID)) {
                try blockWriter.writeLines(allocator, prevSID, lines[streamI..i], self.streamWriter);
                prevSID = line.sid;
                blockSize = 0;
                streamI = @intCast(i);
            }
            blockSize += line.fieldsLen();
        }
        if (streamI != lines.len) {
            try blockWriter.writeLines(allocator, prevSID, lines[streamI..], self.streamWriter);
        }
        try blockWriter.finish(allocator, self.streamWriter);
    }
};

test {
    _ = @import("memtable_test.zig");
}
