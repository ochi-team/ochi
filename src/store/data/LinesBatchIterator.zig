const std = @import("std");
const Allocator = std.mem.Allocator;
const Heap = @import("../../stds/heap.zig").Heap;

const Line = @import("../lines.zig").Line;
const SID = @import("../lines.zig").SID;

pub const Checkpoint = struct {
    sid: SID,
    end: u16,
};

pub const Run = struct {
    lines: []Line,
    start: usize = 0,
    cursor: usize = 0,
    end: usize,

    pub fn init(lines: []Line) Run {
        return .{
            .lines = lines,
            .start = 0,
            .cursor = 0,
            .end = lines.len,
        };
    }

    fn current(self: *const Run) ?*Line {
        if (self.cursor >= self.end) return null;
        return &self.lines[self.cursor];
    }
};

pub const Cursor = struct {
    runs: []Run,
};

const HeapEntry = struct {
    runIndex: usize,
    line: *Line,
};

fn heapEntryLessThan(a: HeapEntry, b: HeapEntry) bool {
    return a.line.timestampNs < b.line.timestampNs;
}

const EntriesHeap = Heap(HeapEntry, heapEntryLessThan);

pub const LinesBatchIterator = struct {
    runs: []Run,
    heap: std.ArrayList(HeapEntry),
    allocator: Allocator,

    pub fn init(alloc: Allocator, runs: []Run) !LinesBatchIterator {
        var it = LinesBatchIterator{
            .runs = runs,
            .heap = try std.ArrayList(HeapEntry).initCapacity(alloc, runs.len),
            .allocator = alloc,
        };
        it.reset();
        return it;
    }

    pub fn deinit(self: *LinesBatchIterator, alloc: Allocator) void {
        self.heap.deinit(alloc);
    }

    pub fn reset(self: *LinesBatchIterator) void {
        self.heap.clearRetainingCapacity();
        for (self.runs, 0..) |*run, i| {
            run.cursor = run.start;
            if (run.current()) |line| {
                self.heap.appendAssumeCapacity(.{ .runIndex = i, .line = line });
            }
        }
        var heap = EntriesHeap.init(self.allocator, &self.heap);
        heap.heapify();
    }

    pub fn len(self: *const LinesBatchIterator) usize {
        var res: usize = 0;
        for (self.runs) |run| {
            res += run.end - run.cursor;
        }
        return res;
    }

    pub fn next(self: *LinesBatchIterator) ?*Line {
        if (self.heap.items.len == 0) return null;

        const entry = self.heap.items[0];
        var run = &self.runs[entry.runIndex];
        run.cursor += 1;
        if (run.current()) |line| {
            self.heap.items[0] = .{ .runIndex = entry.runIndex, .line = line };
            var heap = EntriesHeap.init(self.allocator, &self.heap);
            heap.fix(0);
        } else {
            var heap = EntriesHeap.init(self.allocator, &self.heap);
            _ = heap.pop();
        }

        return entry.line;
    }

    pub fn cursor(self: *const LinesBatchIterator, alloc: Allocator) !Cursor {
        const runs = try alloc.alloc(Run, self.runs.len);
        for (self.runs, 0..) |run, i| {
            runs[i] = run;
        }
        return .{ .runs = runs };
    }

    pub fn freeCursor(_: *const LinesBatchIterator, alloc: Allocator, c: Cursor) void {
        alloc.free(c.runs);
    }

    pub fn window(self: *const LinesBatchIterator, alloc: Allocator, begin: Cursor, end: Cursor) !LinesBatchIterator {
        std.debug.assert(begin.runs.len == end.runs.len);
        std.debug.assert(begin.runs.len == self.runs.len);

        var runs = try alloc.alloc(Run, begin.runs.len);
        errdefer alloc.free(runs);

        var totalLen: usize = 0;
        for (begin.runs, end.runs, 0..) |b, e, i| {
            std.debug.assert(b.lines.ptr == e.lines.ptr);
            std.debug.assert(b.cursor <= e.cursor);
            runs[i] = .{
                .lines = b.lines,
                .start = b.cursor,
                .cursor = b.cursor,
                .end = e.cursor,
            };
            totalLen += e.cursor - b.cursor;
        }

        std.debug.assert(totalLen > 0);
        return LinesBatchIterator.init(alloc, runs);
    }

    pub fn deinitWindow(self: *LinesBatchIterator, alloc: Allocator) void {
        const runs = self.runs;
        self.deinit(alloc);
        alloc.free(runs);
    }
};
