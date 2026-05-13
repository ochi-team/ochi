const std = @import("std");
const Allocator = std.mem.Allocator;
const Io = std.Io;

const httpz = @import("httpz");
const DispatchMeter = @import("observe/DispatchMeter.zig");
const AppConfig = @import("Conf.zig").AppConfig;
const tenant = @import("store/tenant.zig");

const Store = @import("Store.zig").Store;

const ApiError = @import("server/error.zig").ApiError;

pub const AppContext = struct {
    io: Io,
    allocator: Allocator,
    conf: AppConfig,
    tenantID: tenant.TenantID,
    store: *Store,
};

pub const Dispatcher = struct {
    io: Io,
    allocator: Allocator,
    conf: AppConfig,
    store: *Store,
    meter: DispatchMeter,

    pub fn init(io: Io, allocator: Allocator, conf: AppConfig, store: *Store) !Dispatcher {
        var meter = try DispatchMeter.init(allocator, io);
        errdefer meter.deinit();

        return .{
            .io = io,
            .allocator = allocator,
            .conf = conf,
            .store = store,
            .meter = meter,
        };
    }

    pub fn deinit(self: *Dispatcher) void {
        self.meter.deinit();
    }

    pub fn dispatch(
        self: *Dispatcher,
        action: httpz.Action(*AppContext),
        req: *httpz.Request,
        res: *httpz.Response,
    ) void {
        defer self.observeRequest(req, res);

        const tenantID: tenant.TenantID = req.headers.get("X-Scope-OrgID") orelse "default";

        var ctx = AppContext{
            .io = self.io,
            .allocator = self.allocator,
            .conf = self.conf,
            .tenantID = tenantID,
            .store = self.store,
        };

        if (!tenant.isValidID(ctx.tenantID)) {
            res.status = 400;
            res.body = "tenant id is invalid";
            return;
        }

        // TODO: add error logging to every handler,
        // define a standard approach:
        // 1. where to log
        // 2. how do we propagate inner error, only diagnostic or real error
        // 3. do we return ApiError or any from a handler
        action(&ctx, req, res) catch |err| switch (err) {
            ApiError.EmptyBody => {
                res.status = 400;
                res.body = "request body is empty";
            },
            ApiError.DecompressFailed => {
                res.status = 400;
                res.body = "failed to decompress request body";
            },
            ApiError.ContentEncodingNotSupported => {
                res.status = 415;
                res.body = "content-encoding is not supported";
            },
            ApiError.ContentTypeNotSupported => {
                res.status = 415;
                res.body = "content-type is not supported";
            },
            ApiError.MaxBodySize => {
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

    fn observeRequest(self: *Dispatcher, req: *httpz.Request, res: *httpz.Response) void {
        const status: u16 = if (res.status == 0) 200 else res.status;
        const size: u64 = if (req.body()) |body| body.len else 0;

        self.meter.requests.incr(.{ .status = status, .path = req.url.path }) catch |err| {
            std.debug.print("[ERROR] failed to observe request: {}\n", .{err});
        };
        self.meter.throughput.incrBy(.{ .status = status, .path = req.url.path }, size) catch |err| {
            std.debug.print("[ERROR] failed to observe request: {}\n", .{err});
        };
    }
};
