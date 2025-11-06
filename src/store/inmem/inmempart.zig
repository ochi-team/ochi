const std = @import("std");

const Field = @import("../lines.zig").Field;
const Lines = @import("../lines.zig").Lines;
const Line = @import("../lines.zig").Line;
const lineLessThan = @import("../lines.zig").lineLessThan;
const fieldLessThan = @import("../lines.zig").fieldLessThan;
const SID = @import("../lines.zig").SID;

const Buffer = @import("buffer.zig").Buffer;
const BlockWriter = @import("block_writer.zig").BlockWriter;

// 2mb block size, on merging it takes double amount up to 4mb
const maxBlockSize = 2 * 1024 * 1024;

pub const MemPart = struct {
    // TODO: write a header

    // columnNames: *Buffer,
    // columnIdxs: *Buffer,
    // metaindex: *Buffer,
    // index: *Buffer,
    // columnsHeaderIndex: *Buffer,
    // columnsHeader: *Buffer,
    // timestamps: *Buffer,

    // TODO: write bloom filter

    pub fn init(allocator: std.mem.Allocator) !*MemPart {
        const p = try allocator.create(MemPart);
        p.* = MemPart{};

        return p;
    }
    pub fn deinit(self: *MemPart, allocator: std.mem.Allocator) void {
        allocator.destroy(self);
    }

    pub fn addLines(_: *MemPart, allocator: std.mem.Allocator, lines: Lines) !void {
        const blockWriter = try BlockWriter.init(allocator);
        defer blockWriter.deinint(allocator);

        var streamI: u32 = 0;
        var blockSize: u32 = 0;
        var prevSID: SID = lines.items[0].sid;

        std.mem.sortUnstable(*const Line, lines.items, {}, lineLessThan);
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
};
