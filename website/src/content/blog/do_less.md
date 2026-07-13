---
title: First round of optimizations
date: "2026-07-13"
description: "do less - go faster"
author: "Denis"
---

### First step of optimizations

TLDR throughput per CPU improved from 300KiB/s to 1.2MiB/s

After many months (ten to be precise) of working on Ochi and make it "just work" I reached a stage "the throughput is not ok, I have to improve it".

A bit of time spent with the profilers so I knew more or less what's happening.
There won't be hacky secrets. I want to show the basics veryone must pay attention to in every program.
The basic improvements bumped the performance ~4x and reduced the total RAM usage a bit.

### Do less

Ochi uses a lot of encoding and compression.

Zstd average compression ratio  ~x3, so it's good to save the disk space.

To deliver the new data a destination buffer must be allocated.

But what if we move the compressed data immediately to another destination?
```zig
const compressed = try alloc.alloc(u8, compressedBound);
defer alloc.free(compressed);
const cctx = try encoding.createCCtx();
defer encoding.freeCCtx(cctx);
const compressedLen = try encoding.compressAuto(cctx, compressed, uncompressed.items);
```

If we remove the compressed buffer in the same scope there is no point to allocate it,
so it's better to make sure the destination has enough space to handle it.
```zig
try destination.ensureUnusedCapacity(compressedBound)
const cctx = try encoding.createCCtx();
defer encoding.freeCCtx(cctx);
destination.items.len = try encoding.compressAuto(cctx, destination.unusedCapacitySlice(), uncompressed.items);
```

But what if the destination is a file? And it's often a case for a database.
And you tell me I must have a buffer pool.
But buffer pool a complex thing and still takes a synchronization to find a buffer and put it back.

Instead I could create a buffer and share it between different destinations to write, that's how FileDestination came:
```zig
pub const FileDestination = struct {
    file: Io.File,
    len: usize,
    /// buffer holds allocated chunk to reuse between write operations
    /// it's borrowed and shared between others destinations, so we don't clean it
    buf: *std.ArrayList(u8),
};
```

Every table requires quite a few files.
For every write cycle operation we could create a single one and share it between them.
The write job, let's say during a merge run is single threaded, there is no synchronization overhead to do.

Eventually it does a copy with **no allocation** to the buffer.

### Recycle

In the snippet above we saw one more object created every compression, it's zstd ctx.
Zstd it's just a sample, such things must be everywhere.

Every compression call we need the same object, so why not to reuse it?

Instead we carry a ctx pool. It's very simply Ring buffer that circles a pointer to the next element
via atomic.monotonic order so the users won't intersect its mutex.

So every compression calle requires a pool as a dependency and **nobody ever creates more objects**:
```zig
pub fn compressAuto(self: *Self, io: Io, dst: []u8, src: []const u8) !usize {
    const locked = self.next();
    locked.mx.lockUncancelable(io);
    defer locked.mx.unlock(io);

    return encoding.compressAuto(locked.val, dst, src);
}
```

Where next spins the drum to roll the next context in the buffer.

### Use known information

After obvious ones we have to start using the domain information.

Nothing is infinite: RAM is limited, CPU is limited, our users have limited data, etc.

We must know the limitation of the system sooner or later, it may take time.

But I know something about Ochi - limited set of fields a user is able to submit in the logs.

Either are unique set of keys.

So once we process columns (fields derived structures) and know this information, are we able to put the data on stack?

So knowing this info how can we shape this:
```zig
const ColumnDesc = struct {
    columndID: u16,
    offset: usize,
};

columns: std.ArrayList(ColumnDesc),
invariantColumns: std.ArrayList(ColumnDesc),

pub fn init(allocator: std.mem.Allocator) !*Self {
    const s = try allocator.create(Self);
    s.* = Self{
        .columns = std.ArrayList(ColumnDesc).empty,
        .celledColumns = std.ArrayList(ColumnDesc).empty,
    };
    return s;
}
```

First, layout of ColumnDesc is not efficient, it takes 32kb while the elements individually are 8 and 2 bytes.

Second, limit is just 2048 columns, it's 32kb for buffer for all the ColumnDesc's.

To adjust the layout I also had to come up with the offset limit. I know in advance the max buffer size for columns is 8kb,
which fits under `u32`. That gives me already twice less - 16kb.

Saving another 4kb on stack I could make them 2 independant arrays as:
```zig
columnsIDs: std.ArrayList(u16),
columnOffsets: std.ArrayList(u32),
invariantColumnsIDs: std.ArrayList(u16),
invariantColumnOffsets: std.ArrayList(u32),
```

Since the caller knows size of each specifically both `columns` and `invariantColumns` share the same buffer
```zig
var columnIDs: [Block.maxColumns]u16 = undefined;
var columnOffsets: [Block.maxColumns]u32 = undefined;
var cshIdx = ColumnsHeaderIndex.initBufferKnown(&columnIDs, &columnOffsets, csh.headers.len);
```

Therefore from 64kb (worst case) on heap it becomes **12kb on stack**.

### Ship faster

Last, but very important, the tests performance.

The faster feedback loop lets us ship it faster making more changes and bring better performance quicker.

Speed and quality of tests also must grow, so now Ochi has first fuzzing for data encoder.

This piece produced a lot of bugs, so I got tired to stuck on them and wanted to fix them all together.

Another part is to invalidate properly the memory. Setting it explicitly as uninitialized gives better guard against use after free and in the tests specifically it's easier to catch.

It must save a bit of time for future me not to pay attention to this component anymore and move beyond the events horizon.

###### Happy coding

Follow our journey.

[Github](https://github.com/ochi-team/ochi)
[Discord](https://discord.gg/AsCKpCNp5c)


