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

    const blockHeaderSize = @sizeOf(BlockHeader);

    pub fn encode(self: *BlockHeader, buf: *std.ArrayList(u8)) !void {
        try self.sid.encode(buf);

        const u64Buf: [8]u8 = @bitCast(self.size);
        try buf.appendSliceBounded(&u64Buf);

        const u32Buf: [4]u8 = @bitCast(self.len);
        try buf.appendSliceBounded(&u32Buf);

        try self.timestampsHeader.encode(buf);
    }
};

pub const TimestampsHeader = struct {
    offset: u64,
    size: u64,
    min: u64,
    max: u64,

    pub fn encode(self: *TimestampsHeader, buf: *std.ArrayList(u8)) !void {
        var u64Buf: [8]u8 = @bitCast(self.offset);
        try buf.appendSliceBounded(&u64Buf);

        u64Buf = @bitCast(self.size);
        try buf.appendSliceBounded(&u64Buf);

        u64Buf = @bitCast(self.min);
        try buf.appendSliceBounded(&u64Buf);

        u64Buf = @bitCast(self.max);
        try buf.appendSliceBounded(&u64Buf);
    }
};
