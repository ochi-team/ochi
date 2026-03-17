const std = @import("std");

const C = @import("c").C;

pub const CompressError = error{
    Unknown,
};

// ZSTD_CONTENTSIZE_UNKNOWN = 0xffffffffffffffff
// ZSTD_CONTENTSIZE_ERROR = 0xfffffffffffffffe
const unknownSize: c_ulonglong = 0xffffffffffffffff;
const errorSize: c_ulonglong = 0xfffffffffffffffe;

pub fn compressAuto(dst: []u8, src: []const u8) CompressError!usize {
    const level: u8 = if (src.len <= 512) 1 else if (src.len <= 4096) 2 else 3;
    return compress(dst, src, level);
}

pub fn compress(dst: []u8, src: []const u8, level: u8) CompressError!usize {
    const res = C.ZSTD_compress(dst.ptr, dst.len, src.ptr, src.len, level);
    if (C.ZSTD_isError(res) == 1) {
        // TODO: log an error to understand the exact error code
        // const errCode = c.zstd.ZSTD_getErrorCode(res);
        // const msg = c.zstd.ZSTD_getErrorName(res);
        return CompressError.Unknown;
    }
    return res;
}

pub const BoundError = error{
    Unknown,
};

pub fn compressBound(size: usize) BoundError!usize {
    const res = C.ZSTD_compressBound(size);
    if (C.ZSTD_isError(res) == 1) {
        // TODO: log an error to understand the exact error code
        // const errCode = c.zstd.ZSTD_getErrorCode(res);
        // const msg = c.zstd.ZSTD_getErrorName(res);
        return BoundError.Unknown;
    }
    return res;
}

pub const DecompressError = error{
    Unknown,
    InsufficientCapacity,
    OutOfMemory,
};

// TODO: handle ZSTD_CONTENTSIZE_UNKNOWN and ZSTD_CONTENTSIZE_ERROR properly
pub fn getFrameContentSize(src: []const u8) DecompressError!usize {
    // ZSTD frames have a minimum size of 4 bytes (magic number)
    // but ZSTD_getFrameContentSize can determine the size with fewer bytes
    // in practice. Let ZSTD tell us if the data is invalid.
    const res = C.ZSTD_getFrameContentSize(src.ptr, src.len);

    if (res == unknownSize) {
        return DecompressError.Unknown;
    }
    if (res == errorSize) {
        return DecompressError.Unknown;
    }
    return res;
}

// TODO: handle ZSTD_CONTENTSIZE_UNKNOWN and ZSTD_CONTENTSIZE_ERROR properly
pub fn getFrameContentSize2(srcPtr: [*]const u8, srcLen: usize) DecompressError!usize {
    // ZSTD frames have a minimum size of 4 bytes (magic number)
    // but ZSTD_getFrameContentSize can determine the size with fewer bytes
    // in practice. Let ZSTD tell us if the data is invalid.
    const res = C.ZSTD_getFrameContentSize(srcPtr, srcLen);

    if (res == unknownSize) {
        return DecompressError.Unknown;
    }
    if (res == errorSize) {
        return DecompressError.Unknown;
    }
    return res;
}

//TODO: DEPRECATED Use decompressToBuf or decompressToArrayList instead 
pub fn decompress(dst: []u8, src: []const u8) DecompressError!usize {
    const res = C.ZSTD_decompress(dst.ptr, dst.len, src.ptr, src.len);
    if (C.ZSTD_isError(res) == 1) {
        const errCode = C.ZSTD_getErrorCode(res);
        const msg = C.ZSTD_getErrorName(res);
        std.debug.print("decompress error: {d}, msg={s}\n", .{ errCode, msg });
        // TODO: log an error to understand the exact error code
        return handleErrCode(errCode);
    }
    return res;
}

pub fn decompressToArrayList(
    allocator: std.mem.Allocator,
    dst: *std.ArrayList(u8),
    src: []const u8,
) DecompressError!void {
    const dstStream = C.ZSTD_createDStream() orelse return DecompressError.OutOfMemory;
    defer _ = C.ZSTD_freeDStream(dstStream);

    var res = C.ZSTD_initDStream(dstStream);
    if (C.ZSTD_isError(res) == 1) {
        const errCode = C.ZSTD_getErrorCode(res);
        return handleErrCode(errCode);
    }

    dst.clearRetainingCapacity();

    const contentSize = C.ZSTD_getFrameContentSize(src.ptr, src.len);
    const outChunk: usize = C.ZSTD_DStreamOutSize();
    if (contentSize != unknownSize and contentSize != errorSize and contentSize <= std.math.maxInt(usize)) {
        try dst.ensureTotalCapacity(allocator, @intCast(contentSize));
    } else {
        try dst.ensureTotalCapacity(allocator, outChunk);
    }

    var input = C.ZSTD_inBuffer{ .src = src.ptr, .size = src.len, .pos = 0 };

    while (true) {
        try dst.ensureUnusedCapacity(allocator, outChunk);
        var outSlice = dst.unusedCapacitySlice();
        if (outSlice.len > outChunk) {
            outSlice = outSlice[0..outChunk];
        }
        var output = C.ZSTD_outBuffer{ .dst = outSlice.ptr, .size = outSlice.len, .pos = 0 };

        res = C.ZSTD_decompressStream(dstStream, &output, &input);

        if (C.ZSTD_isError(res) == 1) {
            const errCode = C.ZSTD_getErrorCode(res);
            return handleErrCode(errCode);
        }

        dst.items.len += output.pos;

        if (res == 0) {
            break;
        }
        if (input.pos == input.size and output.pos == 0) {
            return DecompressError.Unknown;
        }
    }

    return;
}

pub fn decompressToBuf(allocator: std.mem.Allocator, src: []const u8) DecompressError![]u8 {
    var dst: std.ArrayList(u8) = .empty;
    defer dst.deinit(allocator);

    try decompressToArrayList(allocator, &dst, src);
    return try dst.toOwnedSlice(allocator);
}

fn handleErrCode(code: C.ZSTD_ErrorCode) DecompressError {
    return switch (code) {
        70 => return DecompressError.InsufficientCapacity,
        else => DecompressError.Unknown,
    };
}

test "decompressToBuf" {
    const alloc = std.testing.allocator;
    const sizeUncompressed = 10 * 1024 * 1024; //10 Mb
    const cases = [_][]const u8{"a" ** sizeUncompressed} ** 5; //5 cases

    for (cases) |case| {
        const bound = try compressBound(case.len);
        const compressed = try alloc.alloc(u8, bound);
        defer alloc.free(compressed);

        const compressedLen = try compressAuto(compressed, case);

        const decompressed = try decompressToBuf(alloc, compressed[0..compressedLen]);
        defer alloc.free(decompressed);

        try std.testing.expectEqualSlices(u8, case, decompressed);
    }
}

test "decompressToArrayList" {
    const alloc = std.testing.allocator;
    var dst: std.ArrayList(u8) = .empty;
    defer dst.deinit(alloc);

    const sizeUncompressed = 10 * 1024 * 1024; //10 Mb
    const cases = [_][]const u8{"a" ** sizeUncompressed} ** 5; //5 cases

    for (cases) |case| {
        const bound = try compressBound(case.len);
        const compressed = try alloc.alloc(u8, bound);
        defer alloc.free(compressed);

        const compressedLen = try compressAuto(compressed, case);
        try decompressToArrayList(alloc, &dst, compressed[0..compressedLen]);

        try std.testing.expectEqualSlices(u8, case, dst.items);
    }
}
