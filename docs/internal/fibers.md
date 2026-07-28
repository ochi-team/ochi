# Fibers

It documents the current state of an attempt to run coroutines for merge and flush tasks.

The implementation is flaky, not uniform and hard to maintain, so it expects complete reimplementation,
it requires bringing a custom fibers implementation in order to carry it.

This document describe the state what and why has been done in order to remove it during the reimplementation.

### loop

It's based on xev even loop, so the file TimerLoop must go to limbo completely

### store

store runs a couple repetitive tasks to evict cache and calculate the currently take disk space,
it does fully benefit from even loop being able to rearm tasks til the server is stopped.

the change is minimal, it schedules small tasks via addTimer:
- diskUsageSamplerTick
- cacheEvicterTick

the api is minimal and fits its purpose, no changes expected

### recorders

MergeTask handles the ctx to run a coroutines, I anticipate a similar structure is required to start a fiber.

TableTimerSlot is an attempt to run a timer per object. This solution is poorly scalable. 
It's better to assign a timer to a table instead of `flushAt`
and in the callback we get the parent field (table) and clean the left over resource of the timer.

Timers on data shards are in even worse state since their amount is not comptime known.

As a result to run the timers data objects (DataShard and DataRecorder) have the properties:
```zig
parent: *DataRecorder = undefined,
xevTimer: xev.Timer,
timerC: xev.Completion = .{},
timerCancelC: xev.Completion = .{},
```

On top of that recorder holds more properties to manage the created timers per object:
```zig
timerLoop: *TimerLoop,
taskCtx: TaskCtx,
mergePool: *xev.ThreadPool,
pendingMerges: std.atomic.Value(usize) = .init(0),

pendingDeadlineMx: std.atomic.Mutex = .unlocked,
pendingShardArms: std.ArrayList(*DataShard) = .empty,
pendingTableArms: std.ArrayList(*Table) = .empty,
tableTimerSlots: [maxMemTables]TableTimerSlot,
```

Where `mergePool` and `timerLoop` a shared pointers to the loops, 
`taskCtx` is an object holding the necessary tasks dependencies since the recorder doesn't hold its allocator and io.

`pendingMerges` simulates a low quality barrier in order to drain all the merges in the shutdown path via `waitForMergesToDrain`.

`tableTimerSlots` is an array of timers per table, same as in the shard.

Since all the tasks are expected to be scheduled from the same thread it uses `pendingDeadlineMx` as an atomic spin lock.

`requestShardTimer` and `requestTableTimer` append data to the lists of arms and notify the loop thread.

`pendingShardArms` and `pendingTableArms` hold scheduled objects on timer to flush, so a wake handle must drain them til the end, what `deadlineWakeHandler` does on wake:
collects the objects and reschedules the tasks on the worker thread if necessary on the designated timer.

`armShardTimer` and `armTableTimer` may reschedule the tasks to later in case the it's waken too early.
It eventually leads to `tableTimerCallback` or `shardTimerCallback`.

`submitMergeTask` has different purpose, it doesn't run the task by the timer, but schedules it to a shared thread pool.
