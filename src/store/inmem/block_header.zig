const std = @import("std");

const SID = @import("../lines.zig").SID;
const Block = @import("block.zig").Block;

pub const BlockHeader = struct {
    sid: SID,
    size: u64,
    len: u32,
    timestampsHeader: TimestampsHeader,

    pub fn init(block: *Block, sid: SID) BlockHeader {
        return .{
            .sid = sid,
            .size = block.size(),
            .len = @intCast(block.len()),
            .timestampsHeader = .{
                .offset = 0,
                .size = 0,
                .min = 0,
                .max = 0,
            },
        };
    }

    // 24 sid, 8 size, 4 len, 32 timestampsHeader
    const blockHeaderEncodedLen = 24 + 8 + 4 + 32;
    pub fn encode(self: *BlockHeader, buf: *std.ArrayList(u8)) !void {
        if (buf.capacity - buf.items.len < blockHeaderEncodedLen) {
            return std.mem.Allocator.Error.OutOfMemory;
        }

        try self.sid.encode(buf);

        var intBuf: [8]u8 = undefined;

        std.mem.writeInt(u64, &intBuf, self.size, .big);
        try buf.appendSliceBounded(&intBuf);

        std.mem.writeInt(u32, intBuf[0..4], self.len, .big);
        try buf.appendSliceBounded(intBuf[0..4]);

        try self.timestampsHeader.encode(buf);
    }
};

pub const TimestampsHeader = struct {
    offset: u64,
    size: u64,
    min: u64,
    max: u64,

    pub fn encode(self: *TimestampsHeader, buf: *std.ArrayList(u8)) !void {
        var intBuf: [8]u8 = undefined;
        std.mem.writeInt(u64, &intBuf, self.offset, .big);
        try buf.appendSliceBounded(&intBuf);
        std.mem.writeInt(u64, &intBuf, self.size, .big);
        try buf.appendSliceBounded(&intBuf);
        std.mem.writeInt(u64, &intBuf, self.min, .big);
        try buf.appendSliceBounded(&intBuf);
        std.mem.writeInt(u64, &intBuf, self.max, .big);
        try buf.appendSliceBounded(&intBuf);
    }
};
