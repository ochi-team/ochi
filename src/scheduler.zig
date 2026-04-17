const std = @import("std");

const JobError = error{
    OOM,
};

pub const Scheduler = struct {
    pool: *std.Thread.Pool,
    wg: std.Thread.WaitGroup = .{},
    stopped: std.atomic.Value(bool) = .init(false),
    intervalNs: u96,

    pub fn init(allocator: std.mem.Allocator, comptime threads_per_pool: u8, comptime intervalMs: u96) !*Scheduler {
        var scheduler = try allocator.create(Scheduler);
        errdefer allocator.destroy(scheduler);

        scheduler.pool = try allocator.create(std.Thread.Pool);
        errdefer allocator.destroy(scheduler.pool);
        try scheduler.pool.init(.{
            .allocator = allocator,
            .n_jobs = threads_per_pool,
        });
        errdefer scheduler.pool.deinit();

        scheduler.intervalNs = intervalMs * std.time.ns_per_ms;

        return scheduler;
    }

    pub fn deinit(scheduler: *Scheduler, allocator: std.mem.Allocator) void {
        scheduler.pool.deinit();
        allocator.destroy(scheduler.pool);
        allocator.destroy(scheduler);
    }

    pub fn start(scheduler: *Scheduler, job: anytype, jobArgs: anytype) void {
        scheduler.pool.spawnWg(&scheduler.wg, job, jobArgs);
    }

    pub fn sleepOrStop(scheduler: *Scheduler) void {
        // TODO: make this interval configurable,
        // it must be shorter for tests and longer for production
        const step = 250 * std.time.ns_per_ms;
        var remaining = scheduler.intervalMs;
        while (remaining > 0) {
            if (scheduler.stopped.load(.acquire)) return;
            const s = @min(remaining, step);
            std.Thread.sleep(s);
            remaining -= s;
        }
    }
};
