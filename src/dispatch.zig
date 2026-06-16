const std = @import("std");
const Allocator = std.mem.Allocator;
const Io = std.Io;

const httpz = @import("httpz");
const DispatchMeter = @import("observe/DispatchMeter.zig");
const StoreMeter = @import("observe/StoreMeter.zig");
const Logger = @import("logging");
const AppConfig = @import("Conf.zig").AppConfig;

const Store = @import("Store.zig").Store;

const ApiError = @import("server/error.zig").ApiError;

const QueryError = @import("query/Loql.zig").QueryError;

pub const AppContext = struct {
    io: Io,
    allocator: Allocator,
    conf: *const AppConfig,
    tenantID: u64,
    store: *Store,

    dispatchMeter: *DispatchMeter,
    storeMeter: *StoreMeter,
};

pub const Dispatcher = struct {
    io: Io,
    allocator: Allocator,
    conf: *const AppConfig,
    store: *Store,
    meter: DispatchMeter,

    pub fn init(io: Io, allocator: Allocator, conf: *const AppConfig, store: *Store) !Dispatcher {
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
        const startedAt = Io.Timestamp.now(self.io, .awake);
        defer self.observeRequest(req, res, startedAt);

        const tenantIDStr: []const u8 = req.headers.get("x-scope-orgid") orelse "";
        const tenantID = blk: {
            if (tenantIDStr.len == 0) {
                break :blk 0;
            }
            break :blk std.fmt.parseInt(u64, tenantIDStr, 10) catch {
                res.status = 400;
                res.body = "tenant id is invalid";
                return;
            };
        };

        var ctx = AppContext{
            .io = self.io,
            .allocator = self.allocator,
            .conf = self.conf,
            .tenantID = tenantID,
            .store = self.store,
            .dispatchMeter = &self.meter,
            .storeMeter = &self.store.meter,
        };

        action(&ctx, req, res) catch |err| {
            Logger.log(.err, "failed to handle request", .{ .err = err });

            switch (err) {
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
                QueryError.EmptyQuery => {
                    res.status = 400;
                    res.body = "query is too long";
                },
                QueryError.QueryTooLong => {
                    res.status = 400;
                    res.body = "query is too long";
                },
                std.mem.Allocator.Error.OutOfMemory => {
                    res.status = 500;
                    res.body = "server is out of memory";
                },
                else => {
                    res.status = 500;
                    res.body = "internal server error";
                },
            }
        };
    }

    fn observeRequest(self: *Dispatcher, req: *httpz.Request, res: *httpz.Response, startedAt: Io.Timestamp) void {
        const status: u16 = if (res.status == 0) 200 else res.status;
        const size: u64 = if (req.body()) |body| body.len else 0;
        const elapsedMs = startedAt.untilNow(self.io, .awake).toMilliseconds();
        const latencyMs: u64 = if (elapsedMs < 0) 0 else @intCast(elapsedMs);
        const labels: DispatchMeter.Labels = .{ .status = status, .path = req.url.path };

        self.meter.requests.incr(labels) catch |err| {
            Logger.log(.err, "failed to observe request count", .{ .err = err });
        };
        self.meter.throughput.incrBy(labels, size) catch |err| {
            Logger.log(.err, "failed to observe request throughput", .{ .err = err });
        };
        self.meter.latencyMs.observe(labels, latencyMs) catch |err| {
            Logger.log(.err, "failed to observe request latency", .{ .err = err });
        };
    }
};
