// compress

const compress_mod = @import("compress.zig");

pub const compressAuto = compress_mod.compressAuto;
pub const compress = compress_mod.compress;
pub const compressBound = compress_mod.compressBound;
pub const createCCtx = compress_mod.createCCtx;
pub const freeCCtx = compress_mod.freeCCtx;
pub const createDCtx = compress_mod.createDCtx;
pub const freeDCtx = compress_mod.freeDCtx;
pub const CCtx = compress_mod.CCtx;
pub const DCtx = compress_mod.DCtx;

pub const Error = compress_mod.Error;

pub const getFrameContentSize = compress_mod.getFrameContentSize;
pub const decompress = compress_mod.decompress;

// decode

pub const Decoder = @import("Decoder.zig");
pub const Encoder = @import("Encoder.zig");

test {
    @import("std").testing.refAllDecls(@This());
}
