const std = @import("std");
const Allocator = std.mem.Allocator;
const Io = std.Io;

const BlockHeader = @import("BlockHeader.zig");
const TimestampsHeader = BlockHeader.TimestampsHeader;
const TimestampsEncoder = @import("TimestampsEncoder.zig");
const EncodingType = TimestampsEncoder.EncodingType;
const TableReader = @import("TableReader.zig");

const maxTimestampsBlockSize = @import("BlockData.zig").maxTimestampsBlockSize;

pub const TimestampsData = @This();
data: []const u8 = "",

encodingType: EncodingType = .Undefined,

minTimestamp: u64 = 0,
maxTimestamp: u64 = 0,

pub fn readFrom(
    io: Io,
    buf: []u8,
    th: *const TimestampsHeader,
    sr: *const TableReader,
) !TimestampsData {
    std.debug.assert(buf.len <= maxTimestampsBlockSize);

    const n = try sr.readTimestamps(io, buf, th.offset);
    std.debug.assert(n == buf.len);

    return .{
        .data = buf,
        .encodingType = th.encodingType,
        .minTimestamp = th.min,
        .maxTimestamp = th.max,
    };
}

pub fn deinit(self: *TimestampsData, alloc: Allocator) void {
    if (self.data.len > 0) alloc.free(self.data);
    self.* = .{};
}

pub fn copy(self: *const TimestampsData, buf: []u8) TimestampsData {
    @memcpy(buf, self.data);
    return .{
        .data = buf,
        .encodingType = self.encodingType,
        .minTimestamp = self.minTimestamp,
        .maxTimestamp = self.maxTimestamp,
    };
}
