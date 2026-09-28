const std = @import("std");
const Io = std.Io;

const AppContext = @import("../dispatch.zig").AppContext;
const Store = @import("../Store.zig").Store;
const AccumulatorPool = @import("../AccumulatorPool.zig");
const TimerLoop = @import("../stds/xev/TimerLoop.zig");
const Field = @import("../store/lines.zig").Field;
const process = @import("ingest.zig").process;
const Logger = @import("logging");

const testing = std.testing;

const encodeTags = @import("../store/lines.zig").encodeTags;
const makeStreamID = @import("../store/lines.zig").makeStreamID;
const Query = @import("../query/Query.zig");
const Layout = @import("../Layout.zig");
const Runtime = @import("../Runtime.zig");
const Conf = @import("../Conf.zig");
const Consts = @import("../Consts.zig");

test "large body is processed and appears in the query response" {
    const alloc = testing.allocator;
    const io = testing.io;

    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    const rootPath = try tmp.dir.realPathFileAlloc(io, ".", alloc);
    defer alloc.free(rootPath);

    var partitionsPathBuf: [std.fs.max_path_bytes]u8 = undefined;
    const layout = try Layout.make(io, rootPath, &partitionsPathBuf);

    const conf = Conf.getConf();
    const runtime = try Runtime.init(io, alloc, rootPath, conf.app.maxCachePortion);
    defer runtime.deinit(alloc);

    var store = try Store.init(io, alloc, &conf, runtime, layout);
    defer store.deinit(io, alloc);

    var diagnostic: Logger.Diagnostic = .{};
    const timerLoop = try TimerLoop.init(alloc);
    defer timerLoop.deinit();
    const accumulatorPool = try AccumulatorPool.init(io, alloc, &store, timerLoop, 1);
    defer accumulatorPool.deinit(alloc);

    var sem: std.Io.Semaphore = .{ .permits = 0 };
    var ctx = AppContext{
        .io = io,
        .allocator = alloc,
        .conf = undefined,
        .store = &store,
        .dispatchMeter = undefined,
        .storeMeter = undefined,
        .pprofAlloc = undefined,
        .accumulatorPool = accumulatorPool,
        .request = &.{
            .tenantID = 0,
            .diagnostic = &diagnostic,
        },
        .querySem = &sem,
    };

    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const a = arena.allocator();

    // enough lines * message size to exceed Consts.maxBlockSize (the accumulator's inner buffer),
    // forcing at least one internal flush to the store mid-request
    const lineCount = 200;
    const msgSize = 15_000; // stays under defaultMaxFieldValueSize (16KiB)
    try testing.expect(lineCount * msgSize > Consts.maxBlockSize);

    const message = try a.alloc(u8, msgSize);
    @memset(message, 'x');

    const nowNs: u64 = @intCast(Io.Timestamp.now(io, .real).nanoseconds);

    var body: std.ArrayList(u8) = try .initCapacity(a, 4 * 1024 * 1024);
    body.appendSliceAssumeCapacity("{\"streams\":[{\"stream\":{\"app\":\"large\"},\"values\":[");
    for (0..lineCount) |i| {
        if (i != 0) body.appendAssumeCapacity(',');
        const entry = try std.fmt.allocPrint(a, "[\"{d}\",\"{s}\"]", .{ nowNs + i, message });
        defer a.free(entry);
        body.appendSliceAssumeCapacity(entry);
    }
    body.appendSliceAssumeCapacity("]}]}");

    try process(io, a, &ctx, body.items, .{ .tenantID = ctx.request.tenantID });

    var tags = [_]Field{.{ .key = "app", .value = "large" }};
    const encodedTags = try encodeTags(a, tags[0..]);
    const sid = makeStreamID(ctx.request.tenantID, encodedTags).id;

    const query = Query{
        .streamIDs = &.{sid},
        .start = 0,
        .end = nowNs + std.time.ns_per_hour,
    };

    // assert accumulator has a left over the flushed body,
    // but only part of it since the body doesn't fit its buffer
    const pendingLines = accumulatorPool.slots[0].accumulator.lines.items.len;
    try testing.expect(pendingLines > 0);
    try testing.expect(pendingLines < lineCount);

    // assert the rest of the lines available after flush
    try accumulatorPool.flushAll(io);
    try store.flush(io, alloc);

    var lines = try ctx.store.queryLines(io, a, alloc, ctx.request.tenantID, query);
    defer lines.deinit(a);

    try testing.expectEqual(lineCount, lines.items.len);
    for (lines.items) |line| {
        var found = false;
        for (line.fields) |field| {
            // _msg found
            if (field.key.len == 0) {
                try testing.expectEqualStrings(message, field.value);
                found = true;
            }
        }
        try testing.expect(found);
    }
}
