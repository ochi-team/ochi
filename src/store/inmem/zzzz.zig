const SID = @import("../lines.zig").SID;
const Block = @import("block.zig").Block;

pub const BlockHeader = struct {
    sid: SID,
    size: u64,
    len: u32,
    // timestampsHeader: *TimestampsHeader,

    pub fn init(block: *Block, sid: SID) BlockHeader {
        return .{
            .sid = sid,
            .size = block.size(),
            .len = @intCast(block.len()),
            // .timestampsHeader = undefined,
        };
    }

    pub fn encode(self: *const BlockHeader, buf: []u8) void {
        _ = self;
        _ = buf;
        // TODO: implement
        unreachable;
    }
};

pub const StreamWriter = struct {
    timestampsBuffer: *Buffer,
    indexBuffer: *Buffer,

    pub fn init(allocator: std.mem.Allocator) !*StreamWriter {
        const tsBuffer = try Buffer.init(allocator);
        const indexBuffer = try Buffer.init(allocator);

        const w = try allocator.create(StreamWriter);
        w.* = StreamWriter{
            .timestampsBuffer = tsBuffer,
            .indexBuffer = indexBuffer,
        };
        return w;
    }

    pub fn deinit(self: *StreamWriter, allocator: std.mem.Allocator) void {
        allocator.destroy(self);
    }

    pub fn write(self: *StreamWriter, allocator: std.mem.Allocator, block: *Block, blockHeader: *BlockHeader) BlockHeader {
        try self.writeTimestamps(allocator, blockHeader.timestampsHeader, block.timestamps);

        // TODO implement
        unreachable;
    }

    fn writeTimestamps(self: *StreamWriter, allocator: std.mem.Allocator, tsHeader: *TimestampsHeader, timestamps: []u64) !void {
        const encodedTimestamps = try tsHeader.encode(allocator, timestamps);
        // TODO: write tsHeader data from encodedTimestamps

        tsHeader.size = encodedTimestamps.len;
        tsHeader.min = timestamps[0];
        tsHeader.max = timestamps[timestamps.len - 1];

        self.timestampsBuffer.write(encodedTimestamps);
    }
};

