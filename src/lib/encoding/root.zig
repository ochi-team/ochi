// compress

const compress = @import("compress.zig");

pub const compressAuto = compress.compressAuto;
pub const compressBound = compress.compressBound;

pub const Error = compress.Error;

pub const getFrameContentSize = compress.getFrameContentSize;
pub const decompress = compress.decompress;

// decode

pub const Decoder = @import("Decoder.zig");
pub const Encoder = @import("Encoder.zig");

test {
    @import("std").testing.refAllDecls(@This());
}
