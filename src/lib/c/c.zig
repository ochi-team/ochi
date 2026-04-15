pub const C = @cImport({
    // zstd compression
    @cInclude("zstd.h");
    // call statvfs and use struct_statvfs
    @cInclude("sys/statvfs.h");
});
