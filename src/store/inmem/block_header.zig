const std = @import("std");

const SID = @import("../lines.zig").SID;
const Block = @import("block.zig").Block;
const Encoder = @import("encoder.zig").Encoder;

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

        var ser = Encoder.init(buf);
        try ser.writeInt(u64, self.size);
        try ser.writeInt(u32, self.len);

        try self.timestampsHeader.encode(buf);
    }
};

pub const TimestampsHeader = struct {
    offset: u64,
    size: u64,
    min: u64,
    max: u64,

    pub fn encode(self: *TimestampsHeader, buf: *std.ArrayList(u8)) !void {
        var ser = Encoder.init(buf);
        try ser.writeInt(u64, self.offset);
        try ser.writeInt(u64, self.size);
        try ser.writeInt(u64, self.min);
        try ser.writeInt(u64, self.max);
    }
};
