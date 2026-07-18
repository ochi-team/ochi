pub const C = @cImport({
    // zstd compression
    @cDefine("ZSTD_STATIC_LINKING_ONLY", "1");
    @cInclude("zstd.h");
    // call statvfs and use struct_statvfs
    @cInclude("sys/statvfs.h");
});
