# Agent Guidelines for Ochi

## Logging database server
Default port is 9014

## Build/Test Commands
- **Build**: `zig build`
- **Run**: `zig build run`
- **Test all**: `zig build test`
- **Test single**: `zig build test -Dtest-filter="SIGTERM"` (use substring from test name)

## Code Style
- **Naming**: PascalCase for types/structs, camelCase for variables/fields and functions, but if a function returns a type - PascalCase
- **Comments**: TODO comments for future work, doc comments (///) for public API

## Imports

Never use in-place imports:

```zig
// BAD
var sink: @import("../store/query/Sink.zig") = .{};
defer sink.deinit(a);

// GOOD
const Sink = @import("../store/query/Sink.zig");
...
var sink: Sink = .{};
defer sink.deinit(a);
```

### Test

Good test has multiple cases
```zig
test "bitsetIsEmpty" {
    const f = struct {
        fn f(alloc: Allocator, len: usize, set: []const usize, expectedEmpty: bool) !void {
            var bitset: std.bit_set.DynamicBitSetUnmanaged = try .initEmpty(alloc, len);
            defer bitset.deinit(alloc);

            for (set) |i| {
                bitset.set(i);
            }

            try testing.expectEqual(expectedEmpty, BlockQuery.bitsetIsEmpty(&bitset));
        }
    }.f;

    const alloc = testing.allocator;
    try f(alloc, 0, &.{}, true);
    try f(alloc, 1, &.{}, true);
    try f(alloc, 1, &.{0}, false);
    try f(alloc, 64, &.{42}, false);
    try f(alloc, 64, &.{}, true);
    try f(alloc, 129, &.{128}, false);
    try f(alloc, 128, &.{127}, false);
    try f(alloc, 127, &.{126}, false);
}
```

And has proper assertions comparing all the fields at once:

```zig
// GOOD
const h = Self.decode(encodeBuf[0..offset]);
try std.testing.expectEqualDeep(case.header, h);
// BAD
const h = Self.decode(encodeBuf[0..offset]);
try std.testing.expectEqual(case.header.sid, h.sid);
try std.testing.expectEqual(case.header.minTs, h.minTs);
try std.testing.expectEqual(case.header.maxTs, h.maxTs);
```

Assertions infer types implicitly:
```zig
// GOOD
try testing.expectEqual(1, calls);
// BAD
try testing.expectEqual(@as(usize, 1), calls);
```

## Memory management

### ArrayList

Make sure first it has enough space:

```zig
// GOOD
try self.garbage.ensureUnusedCapacity(alloc, 1);
...
self.garbage.appendAssumeCapacity(buf);
// BAD
try self.garbage.append(alloc, buf);
```

### Docs

Use zigdoc to validate the API of the used Zig version, e.g.:
- zigdoc std.ArrayList
- zigdoc std.mem.Allocator
- zigdoc std.http.Server

