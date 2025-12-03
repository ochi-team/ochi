const std = @import("std");
const compress = @import("../../compress/compress.zig");
const Decoder = @import("encode.zig").Decoder;

const UnpackError = error{
    InvalidFormat,
    InvalidBlockType,
    InsufficientData,
    DecompressionFailed,
};

const compressionKindPlain: u8 = 0;
const compressionKindZstd: u8 = 1;

const uintBlockType8: u8 = 0;
const uintBlockType16: u8 = 1;
const uintBlockType32: u8 = 2;
const uintBlockType64: u8 = 3;
const uintBlockTypeCell8: u8 = 4;
const uintBlockTypeCell16: u8 = 5;
const uintBlockTypeCell32: u8 = 6;
const uintBlockTypeCell64: u8 = 7;

/// Unpacks values that were encoded by ValuesEncoder.packValues
/// If count > 0, the packed data contains only one value which should be duplicated count times
pub fn unpackValues(allocator: std.mem.Allocator, encoded: []const u8, count: usize) !std.ArrayList([]const u8) {
    var offset: usize = 0;

    // Unpack lengths section
    const lengths = try unpackLengths(allocator, encoded, &offset);
    defer allocator.free(lengths);

    // Unpack values section
    var localOffset: usize = 0;
    const valuesData = try unpackBytes(allocator, encoded[offset..], &localOffset);
    defer allocator.free(valuesData);

    // Handle cell types (single length value repeated for all strings)
    const actualLengths = if (lengths.len == 1 and valuesData.len > 0) blk: {
        const cellLen = lengths[0];
        if (cellLen == 0) {
            // Empty strings case - shouldn't happen in practice
            break :blk lengths;
        }
        const expandedLengths = try allocator.alloc(u64, valuesData.len / cellLen);
        for (expandedLengths) |*len| {
            len.* = cellLen;
        }
        break :blk expandedLengths;
    } else lengths;

    const shouldFreeLengths = (actualLengths.ptr != lengths.ptr);
    defer if (shouldFreeLengths) allocator.free(actualLengths);

    // Split values according to lengths
    var result = try std.ArrayList([]const u8).initCapacity(allocator, actualLengths.len);
    errdefer {
        for (result.items) |item| {
            allocator.free(item);
        }
        result.deinit(allocator);
    }

    var valueOffset: usize = 0;
    for (actualLengths) |len| {
        if (valueOffset + len > valuesData.len) {
            return UnpackError.InsufficientData;
        }
        const value = try allocator.dupe(u8, valuesData[valueOffset .. valueOffset + len]);
        result.appendAssumeCapacity(value);
        valueOffset += len;
    }

    // If we unpacked only 1 value but expected more (count > 1),
    // all values are the same - duplicate the single value
    if (result.items.len == 1 and count > 1) {
        const firstValue = result.items[0];
        try result.ensureTotalCapacity(allocator, count);
        for (1..count) |_| {
            const duplicatedValue = try allocator.dupe(u8, firstValue);
            result.appendAssumeCapacity(duplicatedValue);
        }
    }

    return result;
}

fn unpackLengths(allocator: std.mem.Allocator, data: []const u8, offset: *usize) ![]u64 {
    // Unpack the compressed/plain lengths data
    var localOffset: usize = 0;
    const lengthsData = try unpackBytes(allocator, data, &localOffset);
    defer allocator.free(lengthsData);
    offset.* += localOffset;

    // Decode the block type and lengths
    if (lengthsData.len < 1) {
        return UnpackError.InsufficientData;
    }

    var decoder = Decoder.init(lengthsData);
    const blockType = try decoder.readInt(u8);

    switch (blockType) {
        uintBlockTypeCell8 => {
            const cellLen = try decoder.readInt(u8);
            // We don't know the count yet, so we'll return an empty array
            // and handle this in the caller
            return try allocator.dupe(u64, &[_]u64{cellLen});
        },
        uintBlockTypeCell16 => {
            const cellLen = try decoder.readInt(u16);
            return try allocator.dupe(u64, &[_]u64{cellLen});
        },
        uintBlockTypeCell32 => {
            const cellLen = try decoder.readInt(u32);
            return try allocator.dupe(u64, &[_]u64{cellLen});
        },
        uintBlockTypeCell64 => {
            const cellLen = try decoder.readInt(u64);
            return try allocator.dupe(u64, &[_]u64{cellLen});
        },
        uintBlockType8 => {
            const count = (lengthsData.len - 1) / @sizeOf(u8);
            const lengths = try allocator.alloc(u64, count);
            for (0..count) |i| {
                lengths[i] = try decoder.readInt(u8);
            }
            return lengths;
        },
        uintBlockType16 => {
            const count = (lengthsData.len - 1) / @sizeOf(u16);
            const lengths = try allocator.alloc(u64, count);
            for (0..count) |i| {
                lengths[i] = try decoder.readInt(u16);
            }
            return lengths;
        },
        uintBlockType32 => {
            const count = (lengthsData.len - 1) / @sizeOf(u32);
            const lengths = try allocator.alloc(u64, count);
            for (0..count) |i| {
                lengths[i] = try decoder.readInt(u32);
            }
            return lengths;
        },
        uintBlockType64 => {
            const count = (lengthsData.len - 1) / @sizeOf(u64);
            const lengths = try allocator.alloc(u64, count);
            for (0..count) |i| {
                lengths[i] = try decoder.readInt(u64);
            }
            return lengths;
        },
        else => return UnpackError.InvalidBlockType,
    }
}

fn unpackBytes(allocator: std.mem.Allocator, data: []const u8, offset: *usize) ![]u8 {
    if (data.len < 1) {
        return UnpackError.InsufficientData;
    }

    var decoder = Decoder.init(data);
    const compressionKind = try decoder.readInt(u8);

    switch (compressionKind) {
        compressionKindPlain => {
            // Plain format: [kind:u8][len:u8][data]
            const len = try decoder.readInt(u8);
            const bytes = try decoder.readBytes(len);
            offset.* += decoder.offset;
            return try allocator.dupe(u8, bytes);
        },
        compressionKindZstd => {
            // ZSTD format: [kind:u8][len:leb128][compressed_data]
            const compressedLen = try readLeb128(&decoder);
            const compressedData = try decoder.readBytes(compressedLen);
            offset.* += decoder.offset;

            // Get decompressed size
            const decompressedSize = try compress.getFrameContentSize(compressedData);

            // Decompress
            const decompressed = try allocator.alloc(u8, decompressedSize);
            errdefer allocator.free(decompressed);

            const actualSize = compress.decompress(decompressed, compressedData) catch {
                return UnpackError.DecompressionFailed;
            };

            if (actualSize != decompressedSize) {
                allocator.free(decompressed);
                return UnpackError.DecompressionFailed;
            }

            return decompressed;
        },
        else => return UnpackError.InvalidFormat,
    }
}

fn readLeb128(decoder: *Decoder) !usize {
    var result: u64 = 0;
    var shift: u6 = 0;
    var i: usize = 0;

    while (i < 10) : (i += 1) {
        const byte = try decoder.readInt(u8);
        result |= @as(u64, byte & 0x7f) << shift;

        if ((byte & 0x80) == 0) {
            return @intCast(result);
        }

        shift += 7;
    }

    return UnpackError.InvalidFormat;
}
