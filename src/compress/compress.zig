const c = @cImport({
    @cInclude("zstd.h");
});

pub const CompressError = error{
    Unknown,
};

pub fn compressAuto(dst: []u8, src: []u8) CompressError!usize {
    const level: u8 = if (src.len <= 512) 1 else if (src.len <= 4096) 2 else 3;
    return compress(dst, src, level);
}

pub fn compress(dst: []u8, src: []u8, level: u8) CompressError!usize {
    const res = c.ZSTD_compress(dst.ptr, dst.len, src.ptr, src.len, level);
    if (c.ZSTD_isError(res) == 1) {
        // TODO: log an error to understand the exact error code
        // const errCode = c.ZSTD_getErrorCode(res);
        // const msg = c.ZSTD_getErrorName(res);
        return CompressError.Unknown;
    }
    return res;
}

pub const BoundError = error{
    Unknown,
};

pub fn bound(size: usize) BoundError!usize {
    const res = c.ZSTD_compressBound(size);
    if (c.ZSTD_isError(res) == 1) {
        // TODO: log an error to understand the exact error code
        // const errCode = c.ZSTD_getErrorCode(res);
        // const msg = c.ZSTD_getErrorName(res);
        return BoundError.Unknown;
    }
    return res;
}

pub inline fn inlineBound(comptime size: usize) usize {
    const res = c.ZSTD_COMPRESSBOUND(size);
    if (c.ZSTD_isError(res)) {
        // TODO: log an error to understand the exact error code
        // const errCode = c.ZSTD_getErrorCode(res);
        // const msg = c.ZSTD_getErrorName(res);
        return BoundError.Unknown;
    }
    return res;
}
