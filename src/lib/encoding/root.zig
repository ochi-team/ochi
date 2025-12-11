// compress

const compress = @import("compress.zig");

pub const CompressError = compress.CompressError;
pub const compressAuto = compress.compressAuto;

pub const BoundError = compress.BoundError;

pub const compressBound = compress.compressBound;

pub const DecompressError = compress.DecompressError;

pub const getFrameContentSize = compress.getFrameContentSize;

pub const decompress = compress.decompress;

// decode

// pub const Decoder = @import("Decoder.zig");
// pub const DecodeError = Decoder.DecodeError;
// pub const Encoder = @import("Encoder.zig");
