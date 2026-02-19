pub const C = @cImport({
    @cInclude("zstd.h");
    @cInclude("sys/statvfs.h");
});
