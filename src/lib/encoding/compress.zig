const C = @import("c").C;

pub const CompressError = error{
    Unknown,
};

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
};

// TODO: handle ZSTD_CONTENTSIZE_UNKNOWN and ZSTD_CONTENTSIZE_ERROR properly
pub fn getFrameContentSize(src: []const u8) DecompressError!usize {
    // ZSTD frames have a minimum size of 4 bytes (magic number)
    // but ZSTD_getFrameContentSize can determine the size with fewer bytes
    // in practice. Let ZSTD tell us if the data is invalid.
    const res = C.ZSTD_getFrameContentSize(src.ptr, src.len);
    // ZSTD_CONTENTSIZE_UNKNOWN = 0xffffffffffffffff
    // ZSTD_CONTENTSIZE_ERROR = 0xfffffffffffffffe
    const unknownSize: c_ulonglong = 0xffffffffffffffff;
    const errorSize: c_ulonglong = 0xfffffffffffffffe;

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
        // TODO: log an error to understand the exact error code
        // const errCode = c.zstd.ZSTD_getErrorCode(res);
        // const msg = c.zstd.ZSTD_getErrorName(res);
        return DecompressError.Unknown;
    }
    return res;
}
