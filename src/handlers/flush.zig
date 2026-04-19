const std = @import("std");

const httpz = @import("httpz");

const AppContext = @import("../dispatch.zig").AppContext;
const ApiError = @import("../server/error.zig").ApiError;

/// flushHandler allows to call a service to remotely flush all the pending buffers,
/// it's used primarily for testing purposes
pub fn flushHandler(ctx: *AppContext, r: *httpz.Request, res: *httpz.Response) !void {
    const contentType = r.headers.get("content-type");

    if (contentType != null and !std.mem.eql(u8, "application/json", contentType.?)) {
        return ApiError.ContentTypeNotSupported;
    }

    ctx.store.flush(res.arena) catch |err| {
        std.debug.print("[ERROR] Failed to flush store: {s}\n", .{@errorName(err)});
        return ApiError.InternalError;
    };

    res.status = 200;
}
