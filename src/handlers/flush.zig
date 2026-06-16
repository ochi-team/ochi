const std = @import("std");

const httpz = @import("httpz");

const AppContext = @import("../dispatch.zig").AppContext;
const ApiError = @import("../server/error.zig").ApiError;
const Logger = @import("logging");

/// flushHandler allows to call a service to remotely flush all the pending buffers,
/// it's used primarily for testing purposes
pub fn flushHandler(ctx: *AppContext, r: *httpz.Request, res: *httpz.Response) !void {
    const contentType = r.headers.get("content-type");

    if (contentType != null and !std.mem.eql(u8, "application/json", contentType.?)) {
        return ApiError.ContentTypeNotSupported;
    }

    ctx.store.flush(ctx.io, ctx.allocator) catch |err| {
        Logger.log(.err, "failed to flush store", .{ .err = err });
        return ApiError.InternalError;
    };

    res.status = 200;
}
