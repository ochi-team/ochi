const std = @import("std");
const C = @import("c").C;

pub const CompressError = error{
    Unknown,
};

// ZSTD_CONTENTSIZE_UNKNOWN = 0xffffffffffffffff
// ZSTD_CONTENTSIZE_ERROR = 0xfffffffffffffffe
const unknownSize: c_ulonglong = 0xffffffffffffffff;
const errorSize: c_ulonglong = 0xfffffffffffffffe;
const limitStreamChunks = std.math.maxInt(u32) - 1;

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
    BadChunkAdded,
    OutOfLimitChunks,
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
    const buf = try decompressGetBuf(allocator, src);
    defer allocator.free(buf);

    dst.appendSlice(allocator, buf) catch return DecompressError.OutOfMemory;
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

pub fn decompressUnknownSizeToArrayList(
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

    const contentSize = C.ZSTD_getFrameContentSize(src.ptr, src.len);
    const outChunk: usize = C.ZSTD_DStreamOutSize();
    if (contentSize != unknownSize and contentSize != errorSize and contentSize <= std.math.maxInt(usize)) {
        return decompressToArrayList(allocator, dst, src);
    }

    var chunk = try allocator.alloc(u8, outChunk);
    defer allocator.free(chunk);
    
    var input = C.ZSTD_inBuffer{ .src = src.ptr, .size = src.len, .pos = 0 };
    var countChunks: u32 = 0;

    while (true): (countChunks += 1) {
        if (countChunks >= limitStreamChunks) {
            return DecompressError.OutOfLimitChunks;
        }

        var output = C.ZSTD_outBuffer{ .dst = chunk.ptr, .size = chunk.len, .pos = 0 };

        res = C.ZSTD_decompressStream(dstStream, &output, &input);

        if (C.ZSTD_isError(res) == 1) {
            const errCode = C.ZSTD_getErrorCode(res);
            return handleErrCode(errCode);
        }

        if (input.pos == input.size and output.pos == 0 and res != 0) {
            return DecompressError.BadChunkAdded;
        }

        if (output.pos > 0) {
            dst.appendSlice(allocator, chunk[0..output.pos]) catch return DecompressError.OutOfMemory;
        }

        if (res == 0) {
            break;
        }
        
    }

    return;
}

fn handleErrCode(code: C.ZSTD_ErrorCode) DecompressError {
    return switch (code) {
        70 => return DecompressError.InsufficientCapacity,
        else => DecompressError.Unknown,
    };
}

test "compress decompressGetBuf" {
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

test "compress decompressToArrayList empty dst" {
    const alloc = std.testing.allocator;
    var dst: std.ArrayList(u8) = .empty;
    defer dst.deinit(alloc);

    const sizeUncompressed = 1 * 1024; //1 Kb
    const case = "a" ** sizeUncompressed;
    const bound = try compressBound(case.len);
    const compressed = try alloc.alloc(u8, bound);
    defer alloc.free(compressed);

    const compressedLen = try compressAuto(compressed, case);
    try decompressToArrayList(alloc, &dst, compressed[0..compressedLen]);

    try std.testing.expectEqualSlices(u8, case, dst.items);
}

test "compress decompressToArrayList dst with elements" {
    const alloc = std.testing.allocator;
    var dst: std.ArrayList(u8) = .empty;
    const existsSlice = "aaa";
    try dst.appendSlice(alloc, existsSlice);
    defer dst.deinit(alloc);
    const startLen = dst.items.len;

    const sizeUncompressed = 1 * 1024 * 1024; //1 Mb
    const cases = [_][]const u8{"b" ** sizeUncompressed} ** 5; //5 cases
    for (cases) |case| {
        const nextPosition = dst.items.len;
        const bound = try compressBound(case.len);
        const compressed = try alloc.alloc(u8, bound);
        defer alloc.free(compressed);

        const compressedLen = try compressAuto(compressed, case);
        try decompressToArrayList(alloc, &dst, compressed[0..compressedLen]);

        try std.testing.expectEqualSlices(u8, case, dst.items[nextPosition..]);
    }

    try std.testing.expectEqualSlices(u8, existsSlice, dst.items[0..startLen]);
}

test "compress decompressUnknownSizeToArrayList dst with elements" {
    const alloc = std.testing.allocator;
    var dst: std.ArrayList(u8) = .empty;
    const existsSlice = "aaa";
    try dst.appendSlice(alloc, existsSlice);
    defer dst.deinit(alloc);
    const startLen = dst.items.len;

    const sizeUncompressed = 1 * 1024; //1 Kb
    const case = "b" ** sizeUncompressed;
    const bound = C.ZSTD_compressBound(case.len);
    const compressed = try alloc.alloc(u8, bound);
    defer alloc.free(compressed);
    const cctx = C.ZSTD_createCCtx();
    if (cctx == null) return DecompressError.Unknown;

    defer _ = C.ZSTD_freeCCtx(cctx);

    _ = C.ZSTD_CCtx_setParameter(
        cctx,
        C.ZSTD_c_contentSizeFlag,
        0,
    );

    const compressedLen = C.ZSTD_compress2(
        cctx,
        compressed.ptr,
        bound,
        case.ptr,
        case.len,
    );

    if (C.ZSTD_isError(compressedLen) != 0) {
        return DecompressError.Unknown;
    }

    try decompressUnknownSizeToArrayList(alloc, &dst, compressed[0..compressedLen]);
    try std.testing.expectEqualSlices(u8, case, dst.items[startLen..]);
    try std.testing.expectEqualSlices(u8, existsSlice, dst.items[0..startLen]);
}
