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

//TODO: DEPRECATED Use decompressToBuf or decompressToArrayList instead
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
    const dstSize = try getFrameContentSize(src);
    try dst.ensureUnusedCapacity(allocator, dstSize);
    const res = C.ZSTD_decompress(dst.items.ptr, dst.items.len, src.ptr, src.len);

    if (C.ZSTD_isError(res) == 1) {
        const errCode = C.ZSTD_getErrorCode(res);
        const msg = C.ZSTD_getErrorName(res);
        std.debug.print("decompress error: {d}, msg={s}\n", .{ errCode, msg });
        // TODO: log an error to understand the exact error code
        return handleErrCode(errCode);
    }

    // If ZSTD reports a smaller size, reflect the actual length.
    dst.items.len = res;
}

pub fn decompressGetBuf(allocator: std.mem.Allocator, src: []const u8) DecompressError![]u8 {
    const dstSize = try getFrameContentSize(src);
    const dst = allocator.alloc(u8, dstSize) catch return DecompressError.OutOfMemory;
    errdefer allocator.free(dst);

    const res = C.ZSTD_decompress(dst.ptr, dst.len, src.ptr, src.len);

    if (C.ZSTD_isError(res) == 1) {
        const errCode = C.ZSTD_getErrorCode(res);
        const msg = C.ZSTD_getErrorName(res);
        std.debug.print("decompress error: {d}, msg={s}\n", .{ errCode, msg });
        // TODO: log an error to understand the exact error code
        return handleErrCode(errCode);
    }

    return dst;
}

fn handleErrCode(code: C.ZSTD_ErrorCode) DecompressError {
    return switch (code) {
        70 => return DecompressError.InsufficientCapacity,
        else => DecompressError.Unknown,
    };
}

test "decompressGetBuf" {
    const alloc = std.testing.allocator;
    const sizeUncompressed = 10 * 1024 * 1024; //10 Mb
    const cases = [_][]const u8{"a" ** sizeUncompressed} ** 5; //5 cases

    for (cases) |case| {
        const bound = try compressBound(case.len);
        const compressed = try alloc.alloc(u8, bound);
        defer alloc.free(compressed);

        const compressedLen = try compressAuto(compressed, case);

        const decompressed = try decompressGetBuf(alloc, compressed[0..compressedLen]);
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
