const std = @import("std");
const C = @import("c").C;
const Logger = @import("logging");

pub const Error = error{
    DecompressUnknown,
    DecompressInsufficientCapacity,
    DecompressBadChunkAdded,
    DecompressOutOfLimitChunks,
};

// TODO: investigate if we can apply dictionary compression or openzl,
// but efficient dictionary compression may require data reordering and headers redesign

// ZSTD_CONTENTSIZE_UNKNOWN = 0xffffffffffffffff
// ZSTD_CONTENTSIZE_ERROR = 0xfffffffffffffffe
const unknownSize: c_ulonglong = 0xffffffffffffffff;
const errorSize: c_ulonglong = 0xfffffffffffffffe;

pub const CCtx = *C.ZSTD_CCtx;
pub const DCtx = *C.ZSTD_DCtx;
pub const StaticCCtx = struct {
    ctx: CCtx,
    workspace: []align(8) u8,
};
pub const StaticDCtx = struct {
    ctx: DCtx,
    workspace: []align(8) u8,
};

const autoLevle = 1;

pub fn createCCtx() std.mem.Allocator.Error!CCtx {
    return C.ZSTD_createCCtx() orelse std.mem.Allocator.Error.OutOfMemory;
}

pub fn createStaticCCtx(alloc: std.mem.Allocator) !StaticCCtx {
    const workspaceSize = C.ZSTD_estimateCCtxSize(autoLevle);
    if (C.ZSTD_isError(workspaceSize) == 1) return handleErrCode(C.ZSTD_getErrorCode(workspaceSize));

    const workspace = try alloc.alignedAlloc(u8, .@"8", workspaceSize);
    errdefer alloc.free(workspace);

    const ctx = C.ZSTD_initStaticCCtx(workspace.ptr, workspace.len) orelse
        return std.mem.Allocator.Error.OutOfMemory;

    return .{
        .ctx = ctx,
        .workspace = workspace,
    };
}

pub fn freeCCtx(ctx: CCtx) void {
    _ = C.ZSTD_freeCCtx(ctx);
}

pub fn createDCtx() std.mem.Allocator.Error!DCtx {
    return C.ZSTD_createDCtx() orelse std.mem.Allocator.Error.OutOfMemory;
}

pub fn createStaticDCtx(alloc: std.mem.Allocator) !StaticDCtx {
    const workspaceSize = C.ZSTD_estimateDCtxSize();
    if (C.ZSTD_isError(workspaceSize) == 1) return handleErrCode(C.ZSTD_getErrorCode(workspaceSize));

    const workspace = try alloc.alignedAlloc(u8, .@"8", workspaceSize);
    errdefer alloc.free(workspace);

    const ctx = C.ZSTD_initStaticDCtx(workspace.ptr, workspace.len) orelse
        return std.mem.Allocator.Error.OutOfMemory;

    return .{
        .ctx = ctx,
        .workspace = workspace,
    };
}

pub fn freeDCtx(ctx: DCtx) void {
    _ = C.ZSTD_freeDCtx(ctx);
}

pub fn compressAuto(ctx: CCtx, dst: []u8, src: []const u8) Error!usize {
    return compress(ctx, dst, src, autoLevle);
}

pub fn compress(ctx: CCtx, dst: []u8, src: []const u8, level: u8) Error!usize {
    const res = C.ZSTD_compressCCtx(ctx, dst.ptr, dst.len, src.ptr, src.len, level);
    if (C.ZSTD_isError(res) == 1) {
        const errCode = C.ZSTD_getErrorCode(res);
        const msg = C.ZSTD_getErrorName(res);
        Logger.log(.err, "compress error", .{ .code = errCode, .zstd_msg = std.mem.span(msg) });
        return handleErrCode(errCode);
    }
    return res;
}

pub fn compressBound(size: usize) Error!usize {
    const res = C.ZSTD_compressBound(size);
    if (C.ZSTD_isError(res) == 1) {
        const errCode = C.ZSTD_getErrorCode(res);
        const msg = C.ZSTD_getErrorName(res);
        Logger.log(.err, "decompress error", .{ .code = errCode, .zstd_msg = std.mem.span(msg) });
        return handleErrCode(errCode);
    }
    return res;
}

pub fn getFrameContentSize(src: []const u8) Error!usize {
    // ZSTD frames have a minimum size of 4 bytes (magic number)
    // but ZSTD_getFrameContentSize can determine the size with fewer bytes
    // in practice. Let ZSTD tell us if the data is invalid.
    const res = C.ZSTD_getFrameContentSize(src.ptr, src.len);

    if (res == unknownSize) {
        return Error.DecompressUnknown;
    }
    if (res == errorSize) {
        return Error.DecompressUnknown;
    }
    return res;
}

pub fn decompress(ctx: DCtx, dst: []u8, src: []const u8) Error!usize {
    const res = C.ZSTD_decompressDCtx(ctx, dst.ptr, dst.len, src.ptr, src.len);
    if (C.ZSTD_isError(res) == 1) {
        const errCode = C.ZSTD_getErrorCode(res);
        const msg = C.ZSTD_getErrorName(res);
        Logger.log(.err, "decompress error", .{ .code = errCode, .zstd_msg = std.mem.span(msg) });
        return handleErrCode(errCode);
    }
    return res;
}

fn handleErrCode(code: C.ZSTD_ErrorCode) Error {
    return switch (code) {
        70 => return Error.DecompressInsufficientCapacity,
        else => Error.DecompressUnknown,
    };
}
