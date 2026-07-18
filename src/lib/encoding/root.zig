// compress

const compress = @import("compress.zig");

pub const compressAuto = compress.compressAuto;
pub const compressBound = compress.compressBound;
pub const createCCtx = compress.createCCtx;
pub const createStaticCCtx = compress.createStaticCCtx;
pub const freeCCtx = compress.freeCCtx;
pub const createDCtx = compress.createDCtx;
pub const createStaticDCtx = compress.createStaticDCtx;
pub const freeDCtx = compress.freeDCtx;
pub const CCtx = compress.CCtx;
pub const StaticCCtx = compress.StaticCCtx;
pub const StaticDCtx = compress.StaticDCtx;
pub const DCtx = compress.DCtx;

pub const Error = compress.Error;

pub const getFrameContentSize = compress.getFrameContentSize;
pub const decompress = compress.decompress;

// decode

pub const Decoder = @import("Decoder.zig");
pub const Encoder = @import("Encoder.zig");

test {
    @import("std").testing.refAllDecls(@This());
}
