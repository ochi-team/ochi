const std = @import("std");

const httpz = @import("httpz");

const AppConfig = @import("Conf.zig").AppConfig;
const Processor = @import("process.zig").Processor;
const tenant = @import("store/tenant.zig");

const Store = @import("store.zig").Store;

const InsertError = @import("server/error.zig").InsertError;

pub const AppContext = struct {
    conf: AppConfig,
    tenantID: tenant.TenantID,
    store: *Store,
};

pub const Dispatcher = struct {
    conf: AppConfig,
    store: *Store,

    pub fn dispatch(
        self: *Dispatcher,
        action: httpz.Action(*AppContext),
        req: *httpz.Request,
        res: *httpz.Response,
    ) void {
        const tenantID: tenant.TenantID = req.headers.get("X-Scope-OrgID") orelse "default";

        var ctx = AppContext{
            .conf = self.conf,
            .tenantID = tenantID,
            .store = self.store,
        };

        if (!tenant.isValidID(ctx.tenantID)) {
            res.status = 400;
            res.body = "tenant id is invalid";
            return;
        }

        action(&ctx, req, res) catch |err| switch (err) {
            InsertError.EmptyBody => {
                res.status = 400;
                res.body = "request body is empty";
            },
            InsertError.DecompressFailed => {
                res.status = 400;
                res.body = "failed to decompress request body";
            },
            InsertError.ContentEncodingNotSupported => {
                res.status = 415;
                res.body = "content-encoding is not supported";
            },
            InsertError.ContentTypeNotSupported => {
                res.status = 415;
                res.body = "content-type is not supported";
            },
            InsertError.MaxBodySize => {
                res.status = 413;
                res.body = "max body size is exceeded";
            },
            std.mem.Allocator.Error.OutOfMemory => {
                res.status = 500;
                res.body = "server is out of memory";
            },
            else => {
                res.status = 500;
                res.body = "internal server error";
            },
        };
    }
};
