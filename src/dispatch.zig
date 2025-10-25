const std = @import("std");

const httpz = @import("httpz");

const AppConfig = @import("conf.zig").AppConfig;
const Processor = @import("process.zig").Processor;

pub const AppContext = struct {
    conf: AppConfig,
    processor: *Processor,
};

pub const Dispatcher = struct {
    conf: AppConfig,
    processor: *Processor,

    pub fn dispatch(self: *Dispatcher, action: httpz.Action(*AppContext), req: *httpz.Request, res: *httpz.Response) !void {
        var ctx = AppContext{ .conf = self.conf, .processor = self.processor };

        action(&ctx, req, res) catch |err| switch (err) {
            std.mem.Allocator.Error.OutOfMemory => {
                res.status = 500;
                res.body = "server is otu of memory";
            },
            else => {
                res.status = 500;
                res.body = "internal server error";
            },
        };
    }
};
