const std = @import("std");

const httpz = @import("httpz");

const AppContext = @import("../dispatch.zig").AppContext;
const writeHeapProfile = @import("../pprof/backend.zig").writeHeapProfile;

pub fn allocsHandler(ctx: *AppContext, _: *httpz.Request, res: *httpz.Response) !void {
    res.status = 200;
    res.header("content-type", "application/octet-stream");
    // TODO: we can't use the regular allocator and io,
    // either pass injected reinstrumented or fix the instrumentation of debug io and debug allocator,
    // til it fixed we can't confirm it doesn't lack memory
    try res.buffer.ensureUnusedCapacity(8 * 1024);
    try writeHeapProfile(std.Options.debug_io, std.heap.page_allocator, ctx.pprofAlloc, res.writer());
}
